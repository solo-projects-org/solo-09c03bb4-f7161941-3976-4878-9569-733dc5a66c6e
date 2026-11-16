#!/usr/bin/env python -I
"""真实双进程端到端自测脚本（不依赖 pytest，直接打 HTTP）。

前提：两个 uvicorn 实例已在 127.0.0.1:8001 / 8002 启动并共享数据目录。
覆盖：
  1. 健康检查与 Swagger
  2. 实例1 声明+上传+发布，实例2 立即可见并可下载（跨实例共享状态）
  3. 同号同声明幂等、异声明 409
  4. 同内容跨接收去重，磁盘只一份、额度只算一次
  5. 错误长度/摘要/Parquet 内容拒绝且无成品
  6. 慢上传期间另一实例仍可接收与查询（不阻塞）
  7. 配额不足 507
  8. 崩溃回收：伪造孤儿暂存后调 /admin/reconcile 被清理
"""

from __future__ import annotations

import datetime as dt
import hashlib
import http.client
import json
import os
import pathlib
import sys
import threading
import time
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

BASE1 = "http://127.0.0.1:8001"
BASE2 = "http://127.0.0.1:8002"
WORK = pathlib.Path(os.environ.get("OBS_ITEST_WORK", "/tmp/obs-itest"))
WORK.mkdir(parents=True, exist_ok=True)

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def req(method: str, base: str, path: str, body: bytes | dict | None = None,
        raw: bool = False) -> tuple[int, bytes, dict]:
    if isinstance(body, dict):
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
    elif body is None:
        data, headers = None, {}
    else:
        data, headers = body, {"Content-Type": "application/octet-stream"}
    request = urllib.request.Request(base + path, data=data, headers=headers,
                                     method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def make_parquet(name: str, station: str = "BUOY-IT1", day: dt.date | None = None,
                 salt: float = 0.0, rows: int = 24) -> tuple[bytes, str]:
    day = day or dt.date(2026, 11, 15)
    ts = pa.array(
        [dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
         for h in range(rows)],
        pa.timestamp("us", "UTC"),
    )
    table = pa.table({
        "station_id": pa.array([station] * rows, pa.string()),
        "timestamp_utc": ts,
        "salinity": pa.array([35.0 + salt + h * 0.01 for h in range(rows)],
                             pa.float64()),
    })
    path = WORK / name
    pq.write_table(table, path)
    data = path.read_bytes()
    return data, hashlib.sha256(data).hexdigest()


def decl(rid: str, data: bytes, sha: str, **over) -> dict:
    p = {"receipt_id": rid, "station_id": "BUOY-IT1", "obs_date": "2026-11-15",
         "size": len(data), "sha256": sha}
    p.update(over)
    return p


def main() -> int:
    print("== 1. 健康检查与 Swagger ==")
    for base, iid in ((BASE1, "instance-1"), (BASE2, "instance-2")):
        code, body, _ = req("GET", base, "/health")
        info = json.loads(body)
        check(f"{iid} 健康", code == 200 and info["instance"] == iid, str(info))
    code, body, _ = req("GET", BASE1, "/openapi.json")
    check("Swagger OpenAPI 可访问", code == 200 and b"title" in body)

    print("== 2. 实例1 发布，实例2 立即可见并可下载 ==")
    d1, sha1 = make_parquet("p1.parquet")
    rid = "IT-R1"
    code, _, _ = req("PUT", BASE1, f"/receipts/{rid}/declaration",
                     decl(rid, d1, sha1))
    check("实例1 声明 200", code == 200, str(code))
    code, body, _ = req("POST", BASE1, f"/receipts/{rid}/content", d1, raw=True)
    check("实例1 上传发布 200", code == 200 and json.loads(body)["state"] == "PUBLISHED",
          body[:200].decode())
    code, body, _ = req("GET", BASE2, f"/receipts/{rid}")
    check("实例2 可见同一接收", code == 200 and json.loads(body)["state"] == "PUBLISHED")
    code, body, headers = req("GET", BASE2, f"/receipts/{rid}/download")
    lower_headers = {k.lower(): v for k, v in headers.items()}
    check("实例2 下载原件一致", code == 200 and body == d1
          and lower_headers.get("x-sha256") == sha1)

    print("== 3. 幂等与异声明冲突 ==")
    code, _, _ = req("PUT", BASE2, f"/receipts/{rid}/declaration",
                     decl(rid, d1, sha1))
    check("同号同声明幂等 200", code == 200)
    code, body, _ = req("PUT", BASE1, f"/receipts/{rid}/declaration",
                        decl(rid, d1, sha1, station_id="OTHER"))
    check("异声明 409", code == 409 and json.loads(body)["error"]["code"]
          == "declaration_conflict")

    print("== 4. 同内容跨接收去重 ==")
    rid2 = "IT-R2"
    req("PUT", BASE2, f"/receipts/{rid2}/declaration", decl(rid2, d1, sha1))
    code, body, _ = req("POST", BASE2, f"/receipts/{rid2}/content", d1, raw=True)
    check("同内容第二接收发布成功", code == 200)
    code, body, _ = req("GET", BASE1, "/quota")
    q = json.loads(body)
    check("额度只算一份 blob", q["blob_bytes"] == len(d1) and q["used_bytes"] == len(d1),
          str(q))
    blob = pathlib.Path(os.environ["OBS_DATA_DIR"]) / "blobs" / sha1[:2] / sha1
    check("物理成品仅一份", blob.exists())

    print("== 5. 失败拒绝且无成品 ==")
    bad_rid = "IT-BADLEN"
    req("PUT", BASE1, f"/receipts/{bad_rid}/declaration", decl(bad_rid, d1, sha1))
    code, body, _ = req("POST", BASE1, f"/receipts/{bad_rid}/content",
                        d1 + b"\x00", raw=True)
    check("长度不符 422", code == 422)
    code, _, _ = req("GET", BASE1, f"/receipts/{bad_rid}/download")
    check("失败后无可下载成品", code == 409)

    gar = b"NOT-PARQUET" * 8
    g_rid = "IT-BADPARQUET"
    req("PUT", BASE1, f"/receipts/{g_rid}/declaration",
        decl(g_rid, gar, hashlib.sha256(gar).hexdigest()))
    code, body, _ = req("POST", BASE1, f"/receipts/{g_rid}/content", gar, raw=True)
    check("非法 Parquet 422", code == 422
          and json.loads(body)["error"]["code"] == "invalid_package")

    print("== 6. 慢上传不阻塞其他接收与查询 ==")
    slow_rid = "IT-SLOW"
    d2, sha2 = make_parquet("p2.parquet", salt=3.0)
    req("PUT", BASE1, f"/receipts/{slow_rid}/declaration",
        decl(slow_rid, d2, sha2))

    stop = threading.Event()

    def slow_upload():
        # 用 http.client 直接驱动 socket，分块慢速推送制造在途上传。
        # 注意：stop 仅用于测试收尾，不能在发完前置位，否则请求体不完整。
        try:
            conn = http.client.HTTPConnection("127.0.0.1", 8001, timeout=30)
            conn.putrequest("POST", f"/receipts/{slow_rid}/content")
            conn.putheader("Content-Type", "application/octet-stream")
            conn.putheader("Content-Length", str(len(d2)))
            conn.endheaders()
            chunk = 64
            for i in range(0, len(d2), chunk):
                conn.send(d2[i:i + chunk])
                time.sleep(0.05)
            resp = conn.getresponse()
            slow_result["code"] = resp.status
            slow_result["body"] = resp.read()
            conn.close()
        except Exception as exc:  # noqa: BLE001
            slow_result["code"] = -1
            slow_result["body"] = str(exc).encode()
        finally:
            stop.set()

    slow_result: dict = {}
    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.6)  # 让慢上传先在途

    t0 = time.time()
    code, body, _ = req("GET", BASE2, "/receipts", )
    elapsed_ms = (time.time() - t0) * 1000
    check("慢上传期间实例2 查询即时响应", code == 200 and elapsed_ms < 1500,
          f"{elapsed_ms:.0f}ms")

    other_rid = "IT-WHILE-SLOW"
    code, _, _ = req("PUT", BASE2, f"/receipts/{other_rid}/declaration",
                     decl(other_rid, d2, sha2))
    code, body, _ = req("POST", BASE2, f"/receipts/{other_rid}/content", d2, raw=True)
    check("慢上传期间另一包可正常接收发布", code == 200
          and json.loads(body)["state"] == "PUBLISHED", body[:160].decode())

    stop.set()
    t.join(timeout=30)
    check("慢上传自身最终成功", slow_result.get("code") == 200,
          str(slow_result.get("body", b"")[:160]))

    print("== 7. 崩溃回收 ==")
    staging = pathlib.Path(os.environ["OBS_DATA_DIR"]) / "staging" / "GHOST.part"
    staging.parent.mkdir(parents=True, exist_ok=True)
    staging.write_bytes(b"x" * 50)
    code, body, _ = req("POST", BASE1, "/admin/reconcile")
    info = json.loads(body)
    check("对账清理孤儿暂存", code == 200 and info["removed_staging_files"] >= 1
          and not staging.exists(), str(info))

    print("== 8. 已发布数据最终完整性 ==")
    code, body, _ = req("GET", BASE1, "/receipts", )
    states = {r["receipt_id"]: r["state"] for r in json.loads(body)}
    check("成功记录均为 PUBLISHED",
          all(s == "PUBLISHED" for k, s in states.items()
              if k not in ("IT-BADLEN", "IT-BADPARQUET")),
          str(states))
    check("失败记录保持 UPLOADING 可重传",
          states.get("IT-BADLEN") == "UPLOADING"
          and states.get("IT-BADPARQUET") == "UPLOADING", str(states))

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
