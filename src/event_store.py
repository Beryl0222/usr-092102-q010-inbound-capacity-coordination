"""事件存储与按事件时间重放。

关键语义：
- 顺序以 occurred_at（现场发生时间）为准，而非入库时间。断网传感器恢复后
  批量补传（payload.collection.transmission=backfill），其事件会被正确地
  插回历史时间轴对应位置。
- version 在同一 aggregate_id 内按事件时间严格递增；同一 event_id 重复写入
  按幂等处理。
"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .validator import validate_event


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _ingested_at_of(event: dict, fallback: datetime) -> datetime:
    coll = event.get("payload", {}).get("collection")
    if isinstance(coll, dict) and coll.get("ingested_at"):
        return parse_dt(coll["ingested_at"])
    return fallback


@dataclass(frozen=True)
class StoredEvent:
    event: dict
    ingested_at: datetime

    @property
    def event_id(self) -> str:
        return self.event["event_id"]

    @property
    def aggregate_id(self) -> str:
        return self.event["aggregate_id"]

    @property
    def occurred_at(self) -> datetime:
        return parse_dt(self.event["occurred_at"])

    @property
    def is_backfill(self) -> bool:
        coll = self.event.get("payload", {}).get("collection")
        return isinstance(coll, dict) and coll.get("transmission") == "backfill"


class ValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("；".join(errors))
        self.errors = errors


class EventStore:
    def __init__(self) -> None:
        self._events: dict[str, StoredEvent] = {}

    def append(self, event: dict, ingested_at: datetime | None = None) -> StoredEvent:
        """校验并入库。ingested_at 缺省取当前 UTC；允许乱序、迟到与补传。"""
        errors = validate_event(event)
        if errors:
            raise ValidationError(errors)
        eid = event["event_id"]
        if eid in self._events:
            # 幂等：同一事件重传不重复入库
            return self._events[eid]
        when = ingested_at or datetime.now(timezone.utc)
        stored = StoredEvent(event=event, ingested_at=when)
        self._events[eid] = stored
        return stored

    def append_many(self, events: Iterable[dict], ingested_at: datetime | None = None) -> list[StoredEvent]:
        return [self.append(e, ingested_at=ingested_at) for e in events]

    def get(self, event_id: str) -> dict:
        return self._events[event_id].event

    def all_stored(self) -> list[StoredEvent]:
        """按入库顺序返回（到达顺序），用于演示迟到/补传。"""
        return list(self._events.values())

    def replay(self, as_of: datetime | None = None, known_at: datetime | None = None) -> list[StoredEvent]:
        """按事件时间（occurred_at）重放；同一时刻按入库时间、event_id 稳定排序。

        - as_of：只重放 occurred_at <= as_of 的事件（站在现场时间轴某点）。
        - known_at：只重放 ingested_at <= known_at 的事件（站在‘系统当时已收到’
          的视角）。断网补传事件 ingested_at 晚于其 occurred_at，因此在补传到达
          前不可见，到达后仍按 occurred_at 插回正确位置。
        """
        ordered = sorted(
            self._events.values(),
            key=lambda s: (s.occurred_at, s.ingested_at, s.event_id),
        )
        if as_of is not None:
            ordered = [s for s in ordered if s.occurred_at <= as_of]
        if known_at is not None:
            ordered = [s for s in ordered if s.ingested_at <= known_at]
        return ordered

    def fold(self, reducer: Callable[[dict, StoredEvent], dict], state: dict,
             as_of: datetime | None = None, known_at: datetime | None = None) -> dict:
        for stored in self.replay(as_of=as_of, known_at=known_at):
            state = reducer(state, stored)
        return state

    def history(self, aggregate_id: str) -> list[StoredEvent]:
        return [s for s in self.replay() if s.aggregate_id == aggregate_id]

    def integrity_check(self) -> list[str]:
        """检查同一聚合内版本是否随事件时间严格递增、时间戳相同但版本倒挂等问题。"""
        problems: list[str] = []
        by_aggregate: dict[str, list[StoredEvent]] = {}
        for stored in self.replay():
            by_aggregate.setdefault(stored.aggregate_id, []).append(stored)
        for aggregate_id, items in by_aggregate.items():
            prev = None
            for stored in items:
                ver = stored.event["version"]
                if prev is not None:
                    if ver < prev.event["version"]:
                        problems.append(
                            f"聚合 {aggregate_id} 版本倒挂：{stored.event_id}(v{ver},"
                            f"{stored.occurred_at.isoformat()}) 早于 "
                            f"{prev.event_id}(v{prev.event['version']})"
                        )
                    elif ver == prev.event["version"]:
                        problems.append(f"聚合 {aggregate_id} 版本冲突：{prev.event_id} 与 {stored.event_id} 同为 v{ver}")
                prev = stored
        return problems

    def backfilled_events(self) -> list[StoredEvent]:
        """按到达顺序列出补传事件，便于核对它们与正常事件的时间错位。"""
        return [s for s in self.all_stored() if s.is_backfill]
