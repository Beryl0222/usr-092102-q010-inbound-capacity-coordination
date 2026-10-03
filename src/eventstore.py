"""事件存储与断网重放。

存储只追加（append-only）：
- event_id 幂等：同一事件重复上传不会产生两条记录；
- 同一 aggregate_id 的 version 必须严格递增（补报历史版本时拒绝，
  历史读数更正应作为新事件，而不是覆盖）；
- replay() 一律按 occurred_at（现场发生时间）排序——断网传感器
  received_at 很晚上传，也会被归回它真实发生的位置；
- as_of() 支持查看某个时点系统“已知/已发生”的状态。
"""

from __future__ import annotations

from dataclasses import dataclass

from .timeutil import parse_ts
from .validator import validate_event_full


class EventStoreError(ValueError):
    """事件被存储拒绝（版本冲突、重复 event_id 以外的非法事件等）。"""


@dataclass(frozen=True)
class StoredEvent:
    record: dict

    @property
    def event_id(self) -> str:
        return self.record["event_id"]

    @property
    def event_type(self) -> str:
        return self.record["event_type"]

    @property
    def aggregate_id(self) -> str:
        return self.record["aggregate_id"]

    @property
    def version(self) -> int:
        return self.record["version"]

    @property
    def occurred_at(self) -> str:
        return self.record["occurred_at"]

    @property
    def received_at(self) -> str:
        return self.record.get("received_at", self.record["occurred_at"])

    @property
    def is_backfill(self) -> bool:
        return self.record.get("source", {}).get("ingest") == "backfill_after_outage"


class EventStore:
    def __init__(self) -> None:
        self._events: dict[str, StoredEvent] = {}
        self._append_order: list[str] = []
        self._aggregate_versions: dict[str, set[int]] = {}

    def append(self, record: dict, *, strict: bool = True) -> StoredEvent:
        """追加一条事件。返回 StoredEvent；重复 event_id 原样返回已有事件。"""
        errors = validate_event_full(record) if strict else []
        if errors:
            raise EventStoreError("；".join(errors))

        event_id = record["event_id"]
        if event_id in self._events:
            existing = self._events[event_id]
            if existing.record != record:
                raise EventStoreError(
                    f"event_id={event_id} 已存在且内容不同；更正请发新事件，不得覆盖"
                )
            return existing

        aggregate_id = record["aggregate_id"]
        version = record["version"]
        versions = self._aggregate_versions.setdefault(aggregate_id, set())
        if version in versions:
            raise EventStoreError(
                f"aggregate {aggregate_id} 的 version {version} 已存在，版本必须严格递增"
            )

        stored = StoredEvent(record=dict(record))
        self._events[event_id] = stored
        self._append_order.append(event_id)
        versions.add(version)
        return stored

    def append_many(self, records: list[dict], *, strict: bool = True) -> list[StoredEvent]:
        return [self.append(r, strict=strict) for r in records]

    def next_version(self, aggregate_id: str) -> int:
        """该聚合下一个可用版本号（从 1 开始）。"""
        versions = self._aggregate_versions.get(aggregate_id, set())
        return max(versions, default=0) + 1

    def get(self, event_id: str) -> StoredEvent:
        return self._events[event_id]

    def all_events(self) -> list[StoredEvent]:
        """按追加（接收）顺序——这是平台实际看到消息的顺序。"""
        return [self._events[eid] for eid in self._append_order]

    def replay(self, *, include_backfill: bool = True) -> list[StoredEvent]:
        """按现场发生时间重放全部事件。

        断网期间的读数在恢复后才到达，但 occurred_at 是当时的读数时间，
        因此重放结果与“传感器没断网”时一致，迟到事件自动归位。
        同刻事件按 (version, 接收顺序) 稳定排序。
        """
        events = list(self._events.values())
        if not include_backfill:
            events = [e for e in events if not e.is_backfill]
        received_rank = {eid: i for i, eid in enumerate(self._append_order)}
        return sorted(
            events,
            key=lambda e: (
                parse_ts(e.occurred_at),
                e.aggregate_id,
                e.version,
                received_rank[e.event_id],
            ),
        )

    def replay_aggregate(self, aggregate_id: str) -> list[StoredEvent]:
        return [e for e in self.replay() if e.aggregate_id == aggregate_id]

    def as_of_occurred(self, moment: str) -> list[StoredEvent]:
        """按现场时间线，截至 moment 已经发生的事件（无论平台何时收到）。

        用于“恢复历史真相”：即使断网读数 12:40 才补报，12:05 的读数
        在查看 12:10 状态时仍应纳入——它那时确实发生了。
        """
        return [e for e in self.replay() if parse_ts(e.occurred_at) <= parse_ts(moment)]

    def as_of_known(self, moment: str) -> list[StoredEvent]:
        """按平台认知时间线，截至 moment 已经收到的事件。

        用于复盘“值班人员在当时能看到什么”：断网期间未到达的读数不可见。
        """
        return sorted(
            (e for e in self._events.values() if parse_ts(e.received_at) <= parse_ts(moment)),
            key=lambda e: (parse_ts(e.occurred_at), e.aggregate_id, e.version),
        )

    def by_correlation(self, correlation_id: str) -> list[StoredEvent]:
        """取一次限流协同链路上的全部事件，按发生时间排列。"""
        linked = [e for e in self._events.values() if e.record.get("correlation_id") == correlation_id]
        received_rank = {eid: i for i, eid in enumerate(self._append_order)}
        return sorted(
            linked,
            key=lambda e: (parse_ts(e.occurred_at), received_rank[e.event_id]),
        )

    def late_arrivals(self, *, lag_minutes: float = 1.0) -> list[StoredEvent]:
        """接收时间显著晚于发生时间的事件（断网补报特征）。"""
        laggers = []
        for e in self._events.values():
            delta = (parse_ts(e.received_at) - parse_ts(e.occurred_at)).total_seconds() / 60
            if delta >= lag_minutes:
                laggers.append(e)
        return sorted(laggers, key=lambda e: parse_ts(e.occurred_at))

    def __len__(self) -> int:
        return len(self._events)
