"""Parquet 原包内容校验。

规则（来自需求）：
- 必须能被 PyArrow 当作 Parquet 读取；
- 必须包含三列：station_id（站号）、timestamp_utc（UTC 时刻）、salinity（盐度）；
- 每一行的站号必须等于声明站号，时刻的 UTC 日期必须等于声明日期；
- 保留全部原始行，本模块只校验、不过滤、不改写。
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import InvalidPackage

REQUIRED_COLUMNS = ("station_id", "timestamp_utc", "salinity")


@dataclass(frozen=True)
class ValidationResult:
    row_count: int
    column_names: list[str]


def _parse_iso_timestamps(column: pa.ChunkedArray) -> pa.TimestampArray:
    t = column.type
    if pa.types.is_timestamp(t):
        if t.tz is None:
            return pc.assume_timezone(column, "UTC")
        return pc.cast(column, pa.timestamp(t.unit, "UTC"))
    if pa.types.is_date(t):
        return pc.cast(column, pa.timestamp("us", "UTC"))
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        # ISO-8601 字符串：直接 cast 到带 UTC 时区的 timestamp，
        # pyarrow 原生接受结尾 Z 与 ±HH:MM 偏移
        return pc.cast(column, pa.timestamp("us", "UTC"))
    raise InvalidPackage(
        f"timestamp_utc 列类型 {t} 不受支持，需为 timestamp/date/ISO8601 字符串"
    )


def validate_parquet(path: str, *, station_id: str, obs_date: dt.date) -> ValidationResult:
    """读取并校验一个已完整落盘的原包文件。"""
    try:
        table = pq.read_table(path)
    except (pa.ArrowInvalid, pa.ArrowIOError, OSError) as exc:
        raise InvalidPackage(f"无法按 Parquet 读取原包：{exc}") from exc

    names = table.column_names
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise InvalidPackage(f"原包缺少必需列：{', '.join(missing)}；实际列为 {names}")

    station_col = table.column("station_id")
    if not (pa.types.is_string(station_col.type) or pa.types.is_large_string(station_col.type)):
        raise InvalidPackage("station_id 列必须是字符串类型")

    ts_col = _parse_iso_timestamps(table.column("timestamp_utc"))
    sal_col = table.column("salinity")
    if not (pa.types.is_floating(sal_col.type) or pa.types.is_integer(sal_col.type)):
        raise InvalidPackage("salinity 列必须是数值类型")

    row_count = table.num_rows
    if row_count == 0:
        raise InvalidPackage("原包不允许为空表（至少保留一行观测）")

    # 站号逐行比对（NULL 不等于任何站号，fill_null 后按违规检出）
    expected_station = pa.scalar(station_id, type=station_col.type)
    station_neq = pc.fill_null(pc.not_equal(station_col, expected_station), True)
    station_mismatch = pc.sum(pc.cast(station_neq, pa.int64())).as_py()
    if station_mismatch:
        bad = pc.unique(pc.filter(station_col, station_neq)).to_pylist()
        raise InvalidPackage(
            f"存在 {station_mismatch} 行站号与声明 {station_id!r} 不符，异常站号样例：{bad[:5]}"
        )

    # 时刻按 UTC 取日期后逐行比对
    day_array = pc.day(ts_col)
    month_array = pc.month(ts_col)
    year_array = pc.year(ts_col)
    bad_day = pc.or_(
        pc.not_equal(year_array, pa.scalar(obs_date.year, type=pa.int64())),
        pc.or_(
            pc.not_equal(month_array, pa.scalar(obs_date.month, type=pa.int64())),
            pc.not_equal(day_array, pa.scalar(obs_date.day, type=pa.int64())),
        ),
    )
    # 时间戳为 NULL 时 bad_day 结果为 null，按违规处理
    bad_count = pc.sum(
        pc.cast(pc.fill_null(bad_day, True), pa.int64())
    ).as_py()
    if bad_count:
        raise InvalidPackage(
            f"存在 {bad_count} 行 UTC 观测日期与声明 {obs_date.isoformat()} 不符"
        )

    return ValidationResult(row_count=row_count, column_names=list(names))
