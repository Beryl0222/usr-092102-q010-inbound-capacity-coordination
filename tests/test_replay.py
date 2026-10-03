import unittest
from datetime import timezone

from src.event_store import EventStore, parse_dt
from src.scenario import load_zhangjiajie

TZ = "+08:00"


def capacity(event_id, agg, tm, ver, *, headroom, transmission="live", ingested=None):
    payload = {
        "owner": "transport", "location_id": "shuttle-west-gate",
        "window": {"starts_at": f"2026-10-03T{tm}:00+08:00",
                   "ends_at": f"2026-10-03T{int(tm[:2])}:{int(tm[3:5]) + 5:02d}:00+08:00",
                   "granularity_minutes": 5},
        "metric": {"code": "h", "unit": "人", "basis": "measured"},
        "safety_headroom": headroom,
        "collection": {
            "observed_at": f"2026-10-03T{tm}:00+08:00",
            "ingested_at": f"2026-10-03T{ingested or tm}:00+08:00",
            "transmission": transmission,
        },
    }
    return {
        "event_id": event_id, "event_type": "CAPACITY_REPORTED",
        "aggregate_type": "capacity_source", "aggregate_id": agg,
        "occurred_at": f"2026-10-03T{tm}:00+08:00", "version": ver,
        "summary": event_id, "payload": payload,
    }


class ReplayTest(unittest.TestCase):
    def test_backfill_inserted_at_occurrence_time(self) -> None:
        store = EventStore()
        # 到达顺序：12:00 实时 → 12:40 到达的两条补传（发生于 12:10/12:20）→ 12:30 实时
        store.append(capacity("e-1200", "a", "12:00", 1, headroom=100))
        store.append(capacity("e-1210", "a", "12:10", 2, headroom=80,
                              transmission="backfill", ingested="12:40"))
        store.append(capacity("e-1220", "a", "12:20", 3, headroom=60,
                              transmission="backfill", ingested="12:40"))
        store.append(capacity("e-1230", "a", "12:30", 4, headroom=40))

        replayed = [s.event_id for s in store.replay()]
        self.assertEqual(replayed, ["e-1200", "e-1210", "e-1220", "e-1230"])

        # 重放后的聚合状态必须反映补传（而非停留在断网前的旧值）
        from src.monitoring import Projection
        proj = Projection.build(store)
        self.assertEqual(proj.capacities["a"].safety_headroom, 40)

    def test_known_at_hides_backfill_until_it_arrives(self) -> None:
        store = load_zhangjiajie()
        before = store.replay(known_at=parse_dt("2026-10-03T12:39:00+08:00"))
        after = store.replay(known_at=parse_dt("2026-10-03T12:40:00+08:00"))
        ids_before = {s.event_id for s in before}
        ids_after = {s.event_id for s in after}
        self.assertNotIn("zjj-cap-card-03", ids_before)
        self.assertIn("zjj-cap-card-03", ids_after)
        # 补传到达后按发生时间排在 12:20 的现场观测位置，而不是流的末尾
        ordered = [s.event_id for s in after]
        self.assertLess(ordered.index("zjj-cap-card-02"), ordered.index("zjj-pressure-01"))

    def test_as_of_folds_event_time(self) -> None:
        store = load_zhangjiajie()
        at_1230 = store.replay(as_of=parse_dt("2026-10-03T12:30:59+08:00"))
        self.assertNotIn("zjj-pressure-01", {s.event_id for s in at_1230})

    def test_idempotent_append_and_version_integrity(self) -> None:
        store = load_zhangjiajie()
        n = len(store.all_stored())
        store.append(store.get("zjj-cap-card-03"))
        self.assertEqual(len(store.all_stored()), n)
        self.assertEqual(store.integrity_check(), [])

    def test_version_regression_detected(self) -> None:
        store = EventStore()
        store.append(capacity("e-1", "b", "12:00", 1, headroom=10))
        store.append(capacity("e-2", "b", "12:10", 1, headroom=5))  # 版本重复
        problems = store.integrity_check()
        self.assertEqual(len(problems), 1)
        self.assertIn("版本冲突", problems[0])


if __name__ == "__main__":
    unittest.main()
