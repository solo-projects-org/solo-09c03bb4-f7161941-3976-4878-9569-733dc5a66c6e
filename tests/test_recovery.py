"""崩溃回收（reconcile）与双实例并发测试。"""

from __future__ import annotations

import os
import pathlib
import threading

import pytest

from conftest import declare_payload, make_parquet
from app import main as main_mod
from app.db import Database
from app.storage import blob_path, reconcile, staging_path


def test_reconcile_removes_orphan_staging(client, settings, database, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    # 一个正常已发布接收
    client.put("/receipts/RCV-R1/declaration", json=declare_payload("RCV-R1", data))
    client.post("/receipts/RCV-R1/content", content=data)

    # 伪造另一实例崩溃留下的孤儿暂存（DB 中无此接收）
    orphan = pathlib.Path(settings.staging_dir) / "GHOST-1.part"
    orphan.write_bytes(b"x" * 123)
    # 伪造半成品 tmp（放在已存在的前缀目录下）
    tmp_blob = pathlib.Path(settings.blob_dir) / "ab" / "deadbeef.tmp.999"
    tmp_blob.parent.mkdir(parents=True, exist_ok=True)
    tmp_blob.write_bytes(b"tmp")

    result = reconcile(settings, database)
    assert result["removed_staging_files"] == 1
    assert not orphan.exists()
    assert not tmp_blob.exists()


def test_reconcile_resets_uploading_with_missing_staging(client, settings, database,
                                                         tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-R2"
    client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, data))
    # 模拟写盘后、登记中断：DB 显示有进度，但暂存文件没了
    part = pathlib.Path(staging_path(settings, rid))
    part.write_bytes(data)
    with database.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET bytes_received = ? WHERE receipt_id = ?",
            (len(data), rid),
        )
    part.unlink()

    result = reconcile(settings, database)
    assert result["reset_receipts"] >= 1
    row = client.get(f"/receipts/{rid}").json()
    assert row["state"] == "UPLOADING"
    assert row["bytes_received"] == 0
    assert "重新上传" in row["last_error"]

    # 未完成接收可重传成功
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"


def test_reconcile_handles_published_missing_blob(client, settings, database,
                                                  tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-R3"
    client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, data))
    client.post(f"/receipts/{rid}/content", content=data)
    sha = helpers.sha256_of(data)

    # 模拟磁盘成品丢失（不允许留下缺件的成功记录）
    blob = pathlib.Path(blob_path(settings, sha))
    blob.unlink()

    result = reconcile(settings, database)
    assert result["reset_receipts"] >= 1
    row = client.get(f"/receipts/{rid}").json()
    assert row["state"] == "UPLOADING"
    assert row["blob_sha256"] is None
    assert client.get(f"/receipts/{rid}/download").status_code == 409

    # 重传后恢复
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"
    assert blob.exists()


def test_reconcile_does_not_touch_live_inflight_of_other_instance(
    client, settings, database, tmp_path, helpers
):
    """A 实例在途的暂存，B 实例做对账不能误删。"""
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-R4"
    client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, data))

    # 模拟 A 实例正在上传：UPLOADING 且暂存存在
    part = pathlib.Path(staging_path(settings, rid))
    part.write_bytes(data[:50])
    with database.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET bytes_received = 50 WHERE receipt_id = ?", (rid,)
        )

    before = part.read_bytes()
    result = reconcile(settings, database)
    assert result["removed_staging_files"] == 0
    assert part.read_bytes() == before
    row = client.get(f"/receipts/{rid}").json()
    assert row["bytes_received"] == 50  # 在途进度未被破坏


def test_reconcile_removes_orphan_blob_and_repairs_refcount(
    client, settings, database, tmp_path, helpers
):
    d1 = make_parquet(tmp_path / "a.parquet")
    d2 = make_parquet(tmp_path / "b.parquet", salt=2.0)
    # 两个接收引用两个不同 blob
    for rid, d in (("RCV-R5", d1), ("RCV-R6", d2)):
        client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, d))
        client.post(f"/receipts/{rid}/content", content=d)

    # 伪造一个无引用孤儿 blob（磁盘有、账本无）
    ghost_sha = "f" * 64
    ghost = pathlib.Path(blob_path(settings, ghost_sha))
    ghost.parent.mkdir(parents=True, exist_ok=True)
    ghost.write_bytes(b"orphan")

    result = reconcile(settings, database)
    # 第一轮先把孤儿登记进账本（ref_count=0），随后删除
    assert result["removed_orphan_blobs"] == 1
    assert not ghost.exists()

    # 手工破坏 ref_count，对账应按真实引用修正
    with database.transaction() as conn:
        conn.execute("UPDATE blobs SET ref_count = 99")
    reconcile(settings, database)
    rows = database.query_all("SELECT sha256, ref_count FROM blobs")
    assert {r["ref_count"] for r in rows} == {1}


def test_second_database_instance_shares_state(settings, tmp_path, helpers):
    """模拟双实例：两个 Database 连接指向同一 SQLite 文件。"""
    db_a = Database(settings.db_path)
    db_b = Database(settings.db_path)
    data = make_parquet(tmp_path / "a.parquet")

    # A 写
    with db_a.transaction() as conn:
        conn.execute(
            "INSERT INTO receipts(receipt_id, station_id, obs_date, declared_size, "
            "declared_sha256, state) VALUES (?, ?, ?, ?, ?, 'UPLOADING')",
            ("RCV-X1", "BUOY-001", "2026-11-15", len(data), helpers.sha256_of(data)),
        )
    # B 立即可见（WAL 已提交）
    row = db_b.query_one("SELECT * FROM receipts WHERE receipt_id = ?", ("RCV-X1",))
    assert row is not None and row["station_id"] == "BUOY-001"
    db_a.close()
    db_b.close()


def test_concurrent_uploads_different_receipts_all_publish(client, tmp_path, helpers):
    """多线程并发上传不同接收号，全部成功且互不阻塞。"""
    payloads = {}
    for i in range(6):
        d = make_parquet(tmp_path / f"p{i}.parquet", salt=i * 0.5)
        rid = f"RCV-CNC{i}"
        client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, d))
        payloads[rid] = d

    errors: list[Exception] = []

    def worker(rid: str, data: bytes):
        try:
            r = client.post(f"/receipts/{rid}/content", content=data)
            assert r.status_code == 200, r.text
            assert r.json()["state"] == "PUBLISHED"
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(rid, d))
               for rid, d in payloads.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    for rid in payloads:
        assert client.get(f"/receipts/{rid}").json()["state"] == "PUBLISHED"


def test_concurrent_same_receipt_only_one_wins_lock(client, settings, tmp_path, helpers):
    """同一接收号并发上传：一个成功，其余收到 409 忙或最终一致成功。"""
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-LOCK1"
    client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, data))

    outcomes: list[int] = []
    lock_outcomes = threading.Lock()

    def worker():
        r = client.post(f"/receipts/{rid}/content", content=data)
        with lock_outcomes:
            outcomes.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert 200 in outcomes
    # 其余要么 409（receipt_busy），要么因为发布幂等也成功
    assert all(code in (200, 409) for code in outcomes)
    assert client.get(f"/receipts/{rid}").json()["state"] == "PUBLISHED"
    # 成品只有一份
    sha = helpers.sha256_of(data)
    assert pathlib.Path(blob_path(settings, sha)).exists()


def test_concurrent_declarations_same_id_consistent(client, tmp_path, helpers):
    """同接收号并发声明同内容：只插入一条记录。"""
    data = make_parquet(tmp_path / "a.parquet")
    payload = declare_payload("RCV-DCL", data)

    def worker():
        client.put("/receipts/RCV-DCL/declaration", json=payload)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rows = client.get("/receipts").json()
    assert len([r for r in rows if r["receipt_id"] == "RCV-DCL"]) == 1
