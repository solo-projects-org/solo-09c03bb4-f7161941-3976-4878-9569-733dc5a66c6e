"""端到端主流程：声明、上传、发布、下载、幂等、冲突、校验失败。"""

from __future__ import annotations

import pathlib

import pytest

from conftest import declare_payload, make_parquet  # noqa: E402


def _decl(client, receipt_id, data, **kw):
    return client.put(
        f"/receipts/{receipt_id}/declaration",
        json=declare_payload(receipt_id, data, **kw),
    )


def test_health_and_swagger(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    r = client.get("/openapi.json")
    assert r.status_code == 200
    assert r.json()["info"]["title"].startswith("海洋数仓")
    r = client.get("/docs")
    assert r.status_code == 200


def test_full_publish_and_download_roundtrip(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1001"
    r = _decl(client, rid, data)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["state"] == "UPLOADING"
    assert body["bytes_received"] == 0

    r = client.post(f"/receipts/{rid}/content", content=data,
                    headers={"Content-Type": "application/octet-stream"})
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["state"] == "PUBLISHED"
    assert out["bytes_received"] == len(data)
    assert out["row_count"] == 24

    r = client.get(f"/receipts/{rid}")
    assert r.json()["state"] == "PUBLISHED"

    r = client.get(f"/receipts/{rid}/download")
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["x-sha256"] == helpers.sha256_of(data)
    assert r.headers["x-bytes"] == str(len(data))


def test_declaration_idempotent_same_declaration(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1002"
    r1 = _decl(client, rid, data)
    r2 = _decl(client, rid, data)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["created_at"] == r2.json()["created_at"]


def test_declaration_conflict_on_different_claims(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1003"
    assert _decl(client, rid, data).status_code == 200
    r = _decl(client, rid, data, station_id="BUOY-999")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "declaration_conflict"
    r = _decl(client, rid, data, obs_date="2026-11-14")
    assert r.status_code == 409

    payload = declare_payload(rid, data)
    payload["sha256"] = "0" * 64
    r = client.put(f"/receipts/{rid}/declaration", json=payload)
    assert r.status_code == 409

    payload = declare_payload(rid, data)
    payload["size"] = payload["size"] + 1
    r = client.put(f"/receipts/{rid}/declaration", json=payload)
    assert r.status_code == 409


def test_reupload_same_content_after_publish_is_idempotent(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1004"
    _decl(client, rid, data)
    first = client.post(f"/receipts/{rid}/content", content=data)
    assert first.status_code == 200
    second = client.post(f"/receipts/{rid}/content", content=data)
    assert second.status_code == 200
    assert second.json()["state"] == "PUBLISHED"
    assert client.get(f"/receipts/{rid}/download").content == data


def test_wrong_length_rejected_and_no_artifact(client, tmp_path, helpers, settings):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1005"
    _decl(client, rid, data)
    r = client.post(f"/receipts/{rid}/content", content=data + b"\x00")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "content_mismatch"

    r = client.get(f"/receipts/{rid}/download")
    assert r.status_code == 409
    blob_root = pathlib.Path(settings.blob_dir)
    assert all(not list(p.iterdir()) for p in blob_root.iterdir())

    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200
    assert r.json()["state"] == "PUBLISHED"


def test_wrong_sha_rejected(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1006"
    r = client.put(f"/receipts/{rid}/declaration",
                   json={"receipt_id": rid, "station_id": "BUOY-001",
                         "obs_date": "2026-11-15", "size": len(data),
                         "sha256": "a" * 64})
    assert r.status_code == 200
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "content_mismatch"
    body = client.get(f"/receipts/{rid}").json()
    assert body["state"] == "UPLOADING"
    assert "摘要不符" in body["last_error"]


def test_invalid_parquet_rejected(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-1007"
    garbage = b"NOT A PARQUET FILE" * 10
    _decl(client, rid, garbage)
    r = client.post(f"/receipts/{rid}/content", content=garbage)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_package"
    assert client.get(f"/receipts/{rid}/download").status_code == 409


def test_station_mismatch_inside_parquet_rejected(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet", station_override="BUOY-777")
    rid = "RCV-1008"
    _decl(client, rid, data, station_id="BUOY-001")
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 422
    assert "站号" in r.json()["error"]["message"]


def test_date_mismatch_inside_parquet_rejected(client, tmp_path, helpers):
    data = make_parquet(tmp_path / "a.parquet", day_offset=1)
    rid = "RCV-1009"
    _decl(client, rid, data, obs_date="2026-11-15")
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 422
    assert "UTC 观测日期" in r.json()["error"]["message"]


def test_missing_column_rejected(client, tmp_path, helpers):
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"station_id": ["BUOY-001"], "salinity": [35.0]}), p)
    data = p.read_bytes()
    rid = "RCV-1010"
    _decl(client, rid, data)
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 422
    assert "缺少必需列" in r.json()["error"]["message"]


def test_unknown_receipt_and_bad_inputs(client, tmp_path, helpers):
    assert client.get("/receipts/NOPE").status_code == 404
    assert client.post("/receipts/NOPE/content", content=b"x").status_code == 404

    data = make_parquet(tmp_path / "a.parquet")
    r = client.put("/receipts/bad id/declaration",
                   json={"receipt_id": "bad id", "station_id": "S",
                         "obs_date": "2026-11-15", "size": len(data),
                         "sha256": helpers.sha256_of(data)})
    assert r.status_code == 422

    r = client.put("/receipts/R1/declaration",
                   json={"receipt_id": "R1", "station_id": "S",
                         "obs_date": "2026-11-15", "size": -1,
                         "sha256": "z" * 64})
    assert r.status_code == 422


def test_list_filter_and_abort(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet")
    d2 = make_parquet(tmp_path / "b.parquet", salt=1.0)
    _decl(client, "RCV-L1", d1)
    client.post("/receipts/RCV-L1/content", content=d1)
    _decl(client, "RCV-L2", d2, station_id="BUOY-002")

    r = client.get("/receipts")
    assert r.status_code == 200 and len(r.json()) == 2
    r = client.get("/receipts", params={"state": "PUBLISHED"})
    assert {x["receipt_id"] for x in r.json()} == {"RCV-L1"}
    r = client.get("/receipts", params={"station_id": "BUOY-002"})
    assert {x["receipt_id"] for x in r.json()} == {"RCV-L2"}

    r = client.post("/receipts/RCV-L2/abort")
    assert r.status_code == 200
    assert "中止" in r.json()["last_error"]
    assert client.post("/receipts/RCV-L1/abort").status_code == 409


def test_string_and_naive_timestamp_accepted(client, tmp_path, helpers):
    for idx, kw in enumerate(({"string_ts": True}, {"tz": None})):
        data = make_parquet(tmp_path / f"ts{idx}.parquet", **kw)
        rid = f"RCV-TS{idx}"
        _decl(client, rid, data)
        r = client.post(f"/receipts/{rid}/content", content=data)
        assert r.status_code == 200, r.text
        assert r.json()["state"] == "PUBLISHED"


def test_null_station_and_null_timestamp_rejected(client, tmp_path, helpers):
    import datetime as dt

    import pyarrow as pa
    import pyarrow.parquet as pq

    # NULL 站号
    t = pa.table({
        "station_id": pa.array(["BUOY-001", None], pa.string()),
        "timestamp_utc": pa.array(
            [dt.datetime(2026, 11, 15, h, tzinfo=dt.timezone.utc) for h in range(2)],
            pa.timestamp("us", "UTC")),
        "salinity": pa.array([35.0, 35.1], pa.float64()),
    })
    p = tmp_path / "null_station.parquet"
    pq.write_table(t, p)
    data = p.read_bytes()
    rid = "RCV-NULL1"
    _decl(client, rid, data)
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 422
    assert "站号" in r.json()["error"]["message"]

    # NULL 时刻
    t2 = pa.table({
        "station_id": pa.array(["BUOY-001", "BUOY-001"], pa.string()),
        "timestamp_utc": pa.array(
            [dt.datetime(2026, 11, 15, 0, tzinfo=dt.timezone.utc), None],
            pa.timestamp("us", "UTC")),
        "salinity": pa.array([35.0, 35.1], pa.float64()),
    })
    p2 = tmp_path / "null_ts.parquet"
    pq.write_table(t2, p2)
    data2 = p2.read_bytes()
    rid2 = "RCV-NULL2"
    _decl(client, rid2, data2)
    r2 = client.post(f"/receipts/{rid2}/content", content=data2)
    assert r2.status_code == 422
    assert "UTC 观测日期" in r2.json()["error"]["message"]


def test_delete_receipt_reclaims_unique_blob(client, tmp_path, helpers, settings):
    data = make_parquet(tmp_path / "a.parquet")
    rid = "RCV-DEL"
    _decl(client, rid, data)
    client.post(f"/receipts/{rid}/content", content=data)
    sha = helpers.sha256_of(data)
    blob = pathlib.Path(settings.blob_dir) / sha[:2] / sha
    assert blob.exists()

    r = client.delete(f"/receipts/{rid}")
    assert r.status_code == 204
    assert not blob.exists()
    assert client.get(f"/receipts/{rid}").status_code == 404
