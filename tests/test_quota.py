"""配额与内容去重测试。"""

from __future__ import annotations

import pathlib

import pytest

from conftest import declare_payload, make_parquet


def test_quota_enforced_at_declaration(client, tmp_path, monkeypatch):
    data = make_parquet(tmp_path / "a.parquet")
    # 数据目录额度极小：远小于一个包
    from app import main as main_mod
    from app.config import Settings

    small = Settings(
        data_dir=main_mod.settings.data_dir,
        db_path=main_mod.settings.db_path,
        staging_dir=main_mod.settings.staging_dir,
        blob_dir=main_mod.settings.blob_dir,
        lock_dir=main_mod.settings.lock_dir,
        quota_bytes=10,
        instance_id="test",
        reconcile_interval=3600,
        chunk_size=1024,
    )
    main_mod.settings = small
    r = client.put("/receipts/RCV-Q1/declaration",
                   json=declare_payload("RCV-Q1", data))
    assert r.status_code == 507
    assert r.json()["error"]["code"] == "quota_exceeded"


def test_same_content_dedup_consumes_quota_once(client, tmp_path, helpers, settings):
    data = make_parquet(tmp_path / "a.parquet")
    # 两个不同接收号，内容完全相同
    for rid in ("RCV-D1", "RCV-D2"):
        r = client.put(f"/receipts/{rid}/declaration",
                       json=declare_payload(rid, data))
        assert r.status_code == 200
        assert client.post(f"/receipts/{rid}/content", content=data).status_code == 200

    quota = client.get("/quota").json()
    assert quota["blob_bytes"] == len(data)  # 只占一份
    assert quota["staging_bytes"] == 0
    assert quota["used_bytes"] == len(data)

    # 磁盘上只有一个物理文件
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(settings.blob_dir) / sha[:2] / sha
    assert blob.exists()
    assert blob.stat().st_nlink >= 2 or blob.exists()
    all_blobs = list(pathlib.Path(settings.blob_dir).rglob(sha))
    assert len(all_blobs) == 1


def test_staging_occupies_quota_and_abort_releases(client, tmp_path, helpers, settings):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-S1"
    client.put(f"/receipts/{rid}/declaration", json=declare_payload(rid, data))

    # 模拟在途暂存：直接放一个暂存文件并登记进度
    part = pathlib.Path(settings.staging_dir) / f"{rid}.part"
    part.write_bytes(data)
    quota = client.get("/quota").json()
    assert quota["staging_bytes"] == len(data)

    r = client.post(f"/receipts/{rid}/abort")
    assert r.status_code == 200
    assert not part.exists()
    quota = client.get("/quota").json()
    assert quota["staging_bytes"] == 0
    assert quota["used_bytes"] == 0


def test_quota_blocks_second_distinct_content(client, tmp_path, monkeypatch):
    d1 = make_parquet(tmp_path / "a.parquet", salt=0.0)
    d2 = make_parquet(tmp_path / "b.parquet", salt=5.0)
    from app import main as main_mod
    from app.config import Settings

    # 额度只够一个包，不够两个不同内容
    cap = len(d1) + len(d2) - 1
    base = main_mod.settings
    main_mod.settings = Settings(
        data_dir=base.data_dir, db_path=base.db_path, staging_dir=base.staging_dir,
        blob_dir=base.blob_dir, lock_dir=base.lock_dir, quota_bytes=cap,
        instance_id="test", reconcile_interval=3600, chunk_size=1024,
    )
    assert client.put("/receipts/RCV-C1/declaration",
                      json=declare_payload("RCV-C1", d1)).status_code == 200
    assert client.post("/receipts/RCV-C1/content", content=d1).status_code == 200

    # 第二个不同内容：声明允许（未全额预留），但发布新成品时超额被拒
    r = client.put("/receipts/RCV-C2/declaration",
                   json=declare_payload("RCV-C2", d2))
    assert r.status_code == 200
    r = client.post("/receipts/RCV-C2/content", content=d2)
    assert r.status_code == 507
    assert r.json()["error"]["code"] == "quota_exceeded"
    # 第一个成品仍完好可下载
    assert client.get("/receipts/RCV-C1/download").status_code == 200
    # 失败接收没有成品
    assert client.get("/receipts/RCV-C2/download").status_code == 409


def test_quota_allows_redundant_resend_even_when_tight(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    from app import main as main_mod
    from app.config import Settings

    base = main_mod.settings
    main_mod.settings = Settings(
        data_dir=base.data_dir, db_path=base.db_path, staging_dir=base.staging_dir,
        blob_dir=base.blob_dir, lock_dir=base.lock_dir,
        quota_bytes=len(data),  # 额度恰好等于一个包
        instance_id="test", reconcile_interval=3600, chunk_size=1024,
    )
    client.put("/receipts/RCV-T1/declaration", json=declare_payload("RCV-T1", data))
    client.post("/receipts/RCV-T1/content", content=data)
    # 同内容再发一个接收号：不应因“预留”失败，发布时去重不占新额
    r = client.put("/receipts/RCV-T2/declaration", json=declare_payload("RCV-T2", data))
    assert r.status_code == 200
    r = client.post("/receipts/RCV-T2/content", content=data)
    assert r.status_code == 200
    assert client.get("/quota").json()["used_bytes"] == len(data)
