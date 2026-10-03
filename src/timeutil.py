"""时间语义工具。

现场传感器可能断网，恢复连接后一次性补报。所有业务时间必须带时区偏移，
排序与重放只认 occurred_at（现场发生时间），received_at 仅用于判断迟到。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def parse_ts(value: str) -> datetime:
    """解析带偏移的日期时间；不接受无时区的本地时间，避免跨机构对不齐。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"时间必须带时区偏移：{value}")
    return dt


def format_ts(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def now_iso() -> str:
    return format_ts(datetime.now(timezone.utc).astimezone())


def minutes_between(start: str, end: str) -> float:
    return (parse_ts(end) - parse_ts(start)).total_seconds() / 60.0


def minutes_until(ts: str, reference: str) -> float:
    """reference 之后还剩多少分钟（可为负，表示已过去）。"""
    return (parse_ts(ts) - parse_ts(reference)).total_seconds() / 60.0


def shift(ts: str, minutes: float) -> str:
    return format_ts(parse_ts(ts) + timedelta(minutes=minutes))


def window_contains(window: dict, ts: str) -> bool:
    return parse_ts(window["start"]) <= parse_ts(ts) <= parse_ts(window["end"])
