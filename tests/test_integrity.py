"""原件完整性：等长改坏检测、关联隔离、补传交接、删除交错与重启安全。

覆盖值班场景：
- 已发布共享 Parquet 在磁盘上被等长改坏后，下载不得再返回 200 与原 SHA-256；
- 引用该原件的全部接收号必须显示“不可供数（QUARANTINED）+ 补传原因”；
- 其余健康原件照常供数；
- 任一关联接收按原声明补传，通过长度、摘要与站日三关后恢复共享原件与全部关联接收；
- 同内容始终只占一份额度；删除/对账交错不误删健康原件、不重复计额；
- 慢核验（哈希重算）不拖住其他站日接收。
"""

from __future__ import annotations

import os
import pathlib
import threading
import time

from conftest import declare_payload, make_parquet  # noqa: E402
from app import main as main_mod
from app.db import Database
from app.storage import blob_path, reconcile, staging_path


def _publish(client, rid: str, data: bytes, *, station_id: str = "BUOY-001",
             **kw) -> None:
    r = client.put(f"/receipts/{rid}/declaration",
                   json=declare_payload(rid, data, station_id=station_id, **kw))
    assert r.status_code == 200, r.text
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "PUBLISHED"


def _corrupt_equal_length(path: pathlib.Path, offset: int = 4) -> bytes:
    """等长改坏：翻转一个字节，长度保持不变。"""
    raw = bytearray(path.read_bytes())
    raw[offset] ^= 0xFF
    path.write_bytes(bytes(raw))
    # 显式推进 mtime，确保下载路径的核验缓存失效
    os.utime(path, None)
    return bytes(raw)


def test_available_flag_and_healthy_download(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-I0", data)
    body = client.get("/receipts/RCV-I0").json()
    assert body["state"] == "PUBLISHED" and body["available"] is True
    r = client.get("/receipts/RCV-I0/download")
    assert r.status_code == 200 and r.content == data


def test_equal_length_corruption_quarantines_all_associated_receipts(
    client, settings, tmp_path, helpers
):
    data = make_parquet(tmp_path / "a.parquet")
    for rid in ("RCV-I1", "RCV-I2", "RCV-I3"):
        _publish(client, rid, data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    _corrupt_equal_length(blob)

    # 对账必须发现等长改坏（长度没变，只能靠 SHA-256 发现）
    result = reconcile(settings, main_mod.db)
    assert result["quarantined_blobs"] == 1

    # 所有关联接收均不可供数并带补传原因
    for rid in ("RCV-I1", "RCV-I2", "RCV-I3"):
        body = client.get(f"/receipts/{rid}").json()
        assert body["state"] == "QUARANTINED"
        assert body["available"] is False
        assert body["blob_sha256"] == sha  # 引用保留，等待补传
        assert "等待补传" in (body["last_error"] or "")
        r = client.get(f"/receipts/{rid}/download")
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "artifact_quarantined"
        # 绝不返回坏字节
        assert r.content == b"" or r.content != blob.read_bytes()

    # 可按隔离状态过滤
    ids = {x["receipt_id"] for x in
           client.get("/receipts", params={"state": "QUARANTINED"}).json()}
    assert ids == {"RCV-I1", "RCV-I2", "RCV-I3"}

    # 隔离件仍占额度（不释放、不重复计）
    quota = client.get("/quota").json()
    assert quota["blob_bytes"] == len(data)
    assert quota["quarantined_bytes"] == len(data)
    assert quota["used_bytes"] == len(data)


def test_healthy_other_station_still_served_while_one_blob_quarantined(
    client, settings, tmp_path, helpers
):
    d1 = make_parquet(tmp_path / "a.parquet", station_id="BUOY-001")
    d2 = make_parquet(tmp_path / "b.parquet", station_id="BUOY-002", salt=9.0)
    _publish(client, "RCV-OK", d2, station_id="BUOY-002")
    _publish(client, "RCV-BAD", d1)
    bad_blob = pathlib.Path(blob_path(settings, helpers.sha256_of(d1)))
    _corrupt_equal_length(bad_blob)
    reconcile(settings, main_mod.db)

    # 坏件不可供数
    assert client.get("/receipts/RCV-BAD/download").status_code == 409
    # 另一站日健康原件照常供数
    r = client.get("/receipts/RCV-OK/download")
    assert r.status_code == 200 and r.content == d2
    body = client.get("/receipts/RCV-OK").json()
    assert body["state"] == "PUBLISHED" and body["available"] is True


def test_download_lazy_detection_without_reconcile(client, settings, tmp_path, helpers):
    """不做对账，下载本身也要能发现等长改坏，且不吐坏字节。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-LZ1", data)
    _publish(client, "RCV-LZ2", data)
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(data)))
    _corrupt_equal_length(blob, offset=11)

    r = client.get("/receipts/RCV-LZ1/download")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "artifact_quarantined"
    # 惰性核验已把隔离落库，关联接收同步不可供数
    assert client.get("/receipts/RCV-LZ2").json()["state"] == "QUARANTINED"
    # 再次下载命中隔离缓存，立即 409
    assert client.get("/receipts/RCV-LZ2/download").status_code == 409


def test_reupload_restore_restores_all_associated_and_dedup_quota(
    client, settings, tmp_path, helpers
):
    data = make_parquet(tmp_path / "a.parquet")
    for rid in ("RCV-RS1", "RCV-RS2"):
        _publish(client, rid, data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    _corrupt_equal_length(blob)
    reconcile(settings, main_mod.db)
    assert client.get("/receipts/RCV-RS1").json()["state"] == "QUARANTINED"

    # 错误补传（摘要不符）必须拒绝，隔离状态保持
    bad = bytearray(data)
    bad[20] ^= 0x01
    r = client.post("/receipts/RCV-RS1/content", content=bytes(bad))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "content_mismatch"
    assert client.get("/receipts/RCV-RS1").json()["state"] == "QUARANTINED"

    # 任一关联接收按原声明补传正确原包 → 全部恢复
    r = client.post("/receipts/RCV-RS2/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"
    for rid in ("RCV-RS1", "RCV-RS2"):
        body = client.get(f"/receipts/{rid}").json()
        assert body["state"] == "PUBLISHED" and body["available"] is True
        got = client.get(f"/receipts/{rid}/download")
        assert got.status_code == 200 and got.content == data
        assert got.headers["x-sha256"] == sha

    # 物理只一份、额度只算一次
    assert len(list(pathlib.Path(settings.blob_dir).rglob(sha))) == 1
    quota = client.get("/quota").json()
    assert quota["blob_bytes"] == len(data)
    assert quota["quarantined_bytes"] == 0


def test_reupload_with_wrong_station_day_cannot_restore(client, settings, tmp_path, helpers):
    """补传内容的站日校验仍须通过（三关缺一不可）。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-SD", data)
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(data)))
    _corrupt_equal_length(blob)
    reconcile(settings, main_mod.db)

    wrong_day = make_parquet(tmp_path / "wd.parquet", day_offset=1)
    # 用错误内容按同接收号补传：长度可能相同但摘要不同，先被摘要关拒绝
    r = client.post("/receipts/RCV-SD/content", content=wrong_day)
    assert r.status_code == 422
    assert client.get("/receipts/RCV-SD").json()["state"] == "QUARANTINED"


def test_abort_quarantined_rejected_but_delete_works(client, settings, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    for rid in ("RCV-Q1", "RCV-Q2"):
        _publish(client, rid, data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    _corrupt_equal_length(blob)
    reconcile(settings, main_mod.db)

    # 隔离接收不能中止（引用仍在），但可删除
    r = client.post("/receipts/RCV-Q1/abort")
    assert r.status_code == 409
    assert client.get("/receipts/RCV-Q1").json()["state"] == "QUARANTINED"

    # 删除一个关联：坏件仍被另一接收引用，不得删除
    assert client.delete("/receipts/RCV-Q1").status_code == 204
    assert blob.exists()
    assert client.get("/receipts/RCV-Q2").json()["state"] == "QUARANTINED"

    # 最后一个关联删除：坏件回收、额度释放
    assert client.delete("/receipts/RCV-Q2").status_code == 204
    assert not blob.exists()
    assert client.get("/quota").json()["used_bytes"] == 0


def test_reconcile_idempotent_and_restores_repaired_blob(
    client, settings, database, tmp_path, helpers
):
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-IDP", data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    _corrupt_equal_length(blob)

    first = reconcile(settings, database)
    assert first["quarantined_blobs"] == 1 and first["restored_receipts"] == 0
    # 再跑一次：状态幂等，不重复计数
    second = reconcile(settings, database)
    assert second["quarantined_blobs"] == 0

    # 模拟外部把正确原包原子写回（补传的等价磁盘效果），对账应解除隔离
    blob.write_bytes(data)
    os.utime(blob, None)
    third = reconcile(settings, database)
    assert third["restored_receipts"] >= 1
    body = client.get("/receipts/RCV-IDP").json()
    assert body["state"] == "PUBLISHED" and body["available"] is True
    assert client.get("/receipts/RCV-IDP/download").status_code == 200


def test_quarantine_survives_restart_with_new_database_connection(
    client, settings, tmp_path, helpers
):
    """模拟双实例/重启：换一个 Database 连接指向同一 DB，隔离状态依然可见。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-RST", data)
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(data)))
    _corrupt_equal_length(blob)
    reconcile(settings, main_mod.db)

    db2 = Database(settings.db_path)
    try:
        row = db2.query_one(
            "SELECT state FROM receipts WHERE receipt_id = 'RCV-RST'"
        )
        assert row["state"] == "QUARANTINED"
        brow = db2.query_one("SELECT quarantined FROM blobs")
        assert brow["quarantined"] == 1
    finally:
        db2.close()


def test_interleaved_reconcile_and_reupload_never_double_counts(
    client, settings, tmp_path, helpers
):
    """对账与补传交错：恢复后 ref_count 正确，物理文件唯一。"""
    data = make_parquet(tmp_path / "a.parquet")
    for rid in ("RCV-IX1", "RCV-IX2", "RCV-IX3"):
        _publish(client, rid, data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(blob_path(settings, sha))
    _corrupt_equal_length(blob)
    reconcile(settings, main_mod.db)

    # 补传恢复与一次对账交错执行
    errors: list[Exception] = []

    def do_reconcile():
        try:
            reconcile(settings, main_mod.db)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=do_reconcile)
    t.start()
    r = client.post("/receipts/RCV-IX2/content", content=data)
    t.join()
    assert not errors
    assert r.status_code == 200

    for rid in ("RCV-IX1", "RCV-IX2", "RCV-IX3"):
        assert client.get(f"/receipts/{rid}").json()["state"] == "PUBLISHED"
    brow = main_mod.db.query_one("SELECT ref_count, quarantined FROM blobs WHERE sha256 = ?",
                                 (sha,))
    assert brow["ref_count"] == 3 and brow["quarantined"] == 0
    assert len(list(pathlib.Path(settings.blob_dir).rglob(sha))) == 1
    assert client.get("/quota").json()["blob_bytes"] == len(data)


def test_slow_verification_does_not_block_other_station(client, settings, tmp_path,
                                                        helpers, monkeypatch):
    """慢哈希核验期间，其他站日的查询/下载不被拖住。"""
    import app.storage as storage_mod

    d1 = make_parquet(tmp_path / "a.parquet", station_id="BUOY-001")
    d2 = make_parquet(tmp_path / "b.parquet", station_id="BUOY-002", salt=4.0)
    _publish(client, "RCV-SLOW", d1)
    _publish(client, "RCV-FAST", d2, station_id="BUOY-002")
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(d1)))
    _corrupt_equal_length(blob)

    real_hash = storage_mod.hash_file
    delay = {"on": True}

    def slow_hash(path):
        if delay["on"] and path == str(blob):
            time.sleep(0.8)
        return real_hash(path)

    monkeypatch.setattr(storage_mod, "hash_file", slow_hash)

    started = threading.Event()

    def slow_reconcile():
        started.set()
        reconcile(settings, main_mod.db)

    t = threading.Thread(target=slow_reconcile)
    t.start()
    started.wait()
    # 慢核验（锁外哈希进行中）时，另一站日健康接收的查询与下载应即时响应
    t0 = time.time()
    r = client.get("/receipts/RCV-FAST/download")
    elapsed_ms = (time.time() - t0) * 1000
    assert r.status_code == 200 and r.content == d2
    assert elapsed_ms < 500, f"慢核验拖住了其他接收：{elapsed_ms:.0f}ms"
    t.join()
    delay["on"] = False
    assert client.get("/receipts/RCV-SLOW").json()["state"] == "QUARANTINED"


def test_length_mismatch_corruption_also_quarantines(client, settings, tmp_path, helpers):
    """非等长损坏（长度变化）同样隔离，补传后恢复。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-LEN", data)
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(data)))
    blob.write_bytes(blob.read_bytes() + b"\x00")  # 长度 +1
    os.utime(blob, None)

    r = client.get("/receipts/RCV-LEN/download")
    assert r.status_code == 409
    assert client.get("/receipts/RCV-LEN").json()["state"] == "QUARANTINED"

    r = client.post("/receipts/RCV-LEN/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"
    assert client.get("/receipts/RCV-LEN/download").content == data


def test_published_missing_blob_still_resets_to_uploading(client, settings, database,
                                                          tmp_path, helpers):
    """缺件（文件被删）维持原有回退语义，与“坏件隔离”区分。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-MISS", data)
    blob = pathlib.Path(blob_path(settings, helpers.sha256_of(data)))
    blob.unlink()

    result = reconcile(settings, database)
    assert result["reset_receipts"] >= 1
    body = client.get("/receipts/RCV-MISS").json()
    assert body["state"] == "UPLOADING" and body["blob_sha256"] is None
    assert client.get("/receipts/RCV-MISS/download").status_code == 409
    # 重传恢复
    r = client.post("/receipts/RCV-MISS/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"


def test_stale_publishing_flag_after_crash_is_recovered(client, settings, database,
                                                         tmp_path, helpers):
    """发布临界态崩溃：残留 publishing=1 且暂存缺失时，对账清除标记并可重新上传。"""
    data = make_parquet(tmp_path / "a.parquet")
    _publish(client, "RCV-CRASH", data)
    # 手工构造“落盘后、发布完成前崩溃”：标记置 1，但暂存文件不存在
    with database.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET publishing = 1, state = 'UPLOADING', "
            "blob_sha256 = NULL, bytes_received = ? WHERE receipt_id = 'RCV-CRASH'",
            (len(data),),
        )
    assert not pathlib.Path(staging_path(settings, "RCV-CRASH")).exists()

    reconcile(settings, database)
    row = client.get("/receipts/RCV-CRASH").json()
    assert row["publishing"] == 0  # 陈旧标记被清
    # 重新上传可正常发布
    r = client.post("/receipts/RCV-CRASH/content", content=data)
    assert r.status_code == 200 and r.json()["state"] == "PUBLISHED"
    assert client.get("/receipts/RCV-CRASH/download").content == data


def test_active_publishing_staging_is_not_reaped(client, settings, database,
                                                  tmp_path, helpers):
    """活跃发布临界态（publishing=1 且暂存在）的暂存不被对账误删。"""
    data = make_parquet(tmp_path / "a.parquet")
    client.put("/receipts/RCV-LIVE/declaration",
               json=declare_payload("RCV-LIVE", data))
    part = pathlib.Path(staging_path(settings, "RCV-LIVE"))
    part.write_bytes(data)
    with database.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET bytes_received = ?, publishing = 1 "
            "WHERE receipt_id = 'RCV-LIVE'",
            (len(data),),
        )

    reconcile(settings, database)
    # 暂存保留、标记保留（文件仍在，不算陈旧）
    assert part.exists()
    assert client.get("/receipts/RCV-LIVE").json()["publishing"] == 1
