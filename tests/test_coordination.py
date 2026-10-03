"""端到端特性测试：断网重放、限流全链路、预约保护、恢复条件、双投影与隐私。"""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

from src import scenario
from src.coordination import CoordinationTrace
from src.eventstore import EventStore, EventStoreError
from src.model import CapacityBoard
from src.privacy import (
    EphemeralVisitorSignals,
    PrivacyViolation,
    assert_clean_event,
    build_partner_packet,
)
from src.projections import PublicProjection
from src.validator import validate_event, validate_event_full

HAS_JSONSCHEMA = importlib.util.find_spec("jsonschema") is not None

ROOT = Path(__file__).parents[1]
TS = scenario._ts
WIN = scenario._window


class ScenarioFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ctx = scenario.build()
        cls.store = cls.ctx["store"]

    def test_all_events_pass_full_validation(self) -> None:
        for e in self.store.replay():
            self.assertEqual(validate_event_full(e.record), [], e.event_id)

    def test_exported_scenario_file_conforms(self) -> None:
        data = json.loads((ROOT / "data" / "scenario.json").read_text(encoding="utf-8"))
        self.assertEqual(data["correlation_id"], scenario.CORRELATION)
        self.assertEqual(len(data["events"]), data["event_count"])
        for record in data["events"]:
            self.assertEqual(validate_event_full(record), [], record["event_id"])

    def test_schema_is_valid_json(self) -> None:
        json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))


class OutageReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = scenario.build()
        self.store = self.ctx["store"]

    def test_backfilled_readings_replay_at_occurred_time(self) -> None:
        queue_events = [
            e for e in self.store.replay()
            if e.event_type == "CAPACITY_REPORTED"
            and e.record["payload"]["scope"]["source_id"] == "zjj-shuttle-queue"
        ]
        occurred = [e.occurred_at for e in queue_events]
        # 重放按发生时间：09:20 < 09:50 < 10:05 < 11:00 ...，补报读数归位
        self.assertEqual(occurred, sorted(occurred))
        values = [e.record["payload"]["readings"][0]["value"] for e in queue_events]
        self.assertEqual(values[:3], [22, 31, 38])

        # 接收顺序则不同：补报事件是 10:15 才到达的
        received = self.store.all_events()
        backfill = [e for e in received if e.is_backfill]
        self.assertEqual(len(backfill), 2)
        self.assertTrue(all(e.received_at == TS("10:15") for e in backfill))
        self.assertTrue(all(e.occurred_at < e.received_at for e in backfill))

    def test_as_of_occurred_vs_known_differ_during_outage(self) -> None:
        # 10:00 现场真相里 09:50 的读数已经发生；平台当时还没收到
        occurred = self.store.as_of_occurred(TS("10:00"))
        known = self.store.as_of_known(TS("10:00"))
        occurred_queues = {
            e.record["payload"]["readings"][0]["value"]
            for e in occurred
            if e.event_type == "CAPACITY_REPORTED"
            and e.record["payload"]["scope"]["source_id"] == "zjj-shuttle-queue"
        }
        known_queues = {
            e.record["payload"]["readings"][0]["value"]
            for e in known
            if e.event_type == "CAPACITY_REPORTED"
            and e.record["payload"]["scope"]["source_id"] == "zjj-shuttle-queue"
        }
        self.assertIn(31, occurred_queues)
        self.assertNotIn(31, known_queues)
        # 10:15 补报到达后，认知时间线才补上
        known_after = self.store.as_of_known(TS("10:15"))
        known_after_values = {
            e.record["payload"]["readings"][0]["value"]
            for e in known_after
            if e.event_type == "CAPACITY_REPORTED"
            and e.record["payload"]["scope"]["source_id"] == "zjj-shuttle-queue"
        }
        self.assertIn(31, known_after_values)
        self.assertIn(38, known_after_values)

    def test_idempotent_append_and_version_conflict(self) -> None:
        n = len(self.store)
        record = self.store.all_events()[0].record
        self.store.append(record)  # 重复 event_id 幂等
        self.assertEqual(len(self.store), n)
        dup = dict(record)
        dup["summary"] = "试图篡改"
        with self.assertRaises(EventStoreError):
            self.store.append(dup)


class FullFlowTraceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = scenario.build()
        self.store = self.ctx["store"]

    def test_trace_status_transitions(self) -> None:
        cid = scenario.CORRELATION
        # 10:26 只有信号
        trace_signal = CoordinationTrace(self.store).trace(cid, moment=TS("10:26"))
        self.assertEqual(trace_signal["status"], "triggered")
        # 10:28 提案等待责任方
        trace_proposed = CoordinationTrace(self.store).trace(cid, moment=TS("10:28"))
        self.assertEqual(trace_proposed["status"], "proposed_awaiting_responsible_party")
        # 10:40 限流生效中
        trace_effect = CoordinationTrace(self.store).trace(cid, moment=TS("10:40"))
        self.assertEqual(trace_effect["status"], "in_effect")
        self.assertEqual(len(trace_effect["active_actions"]), 2)
        # 终态恢复
        self.assertEqual(self.ctx["trace"]["status"], "restored")

    def test_timeline_covers_signal_to_restoration(self) -> None:
        types = [n["event_type"] for n in self.ctx["trace"]["timeline"]]
        self.assertIn("PRESSURE_DETECTED", types)
        self.assertIn("DIVERSION_PROPOSED", types)
        self.assertEqual(types.count("ACTION_CONFIRMED"), 2)
        self.assertEqual(types.count("NORMAL_SERVICE_RESTORED"), 2)
        # 因果链可回溯：恢复→决定→提案→信号
        timeline = self.ctx["trace"]["timeline"]
        restorations = [n for n in timeline if n["event_type"] == "NORMAL_SERVICE_RESTORED"]
        self.assertTrue(all(n["causation_id"] for n in restorations))

    def test_engine_only_proposes_responsible_party_confirms(self) -> None:
        # 10:26（提案前）没有任何动作生效
        trace = CoordinationTrace(self.store).trace(scenario.CORRELATION, moment=TS("10:26"))
        self.assertEqual(trace["active_actions"], [])
        proposal = self.ctx["proposal"]
        self.assertEqual(proposal.event_type, "DIVERSION_PROPOSED")
        # 决定由两个不同责任方、在各自 authority 内分别确认
        orgs = {d.record["payload"]["confirmed_by"]["org_id"] for d in self.ctx["decisions"]}
        self.assertEqual(orgs, {"zjj-scenic-bureau", "zjj-traffic-police"})

    def test_reservations_are_never_silently_cancelled(self) -> None:
        payload = self.ctx["proposal"].record["payload"]
        treatments = {item["treatment"] for item in payload["affected_reservations"]}
        self.assertTrue(treatments <= {"honor", "offer_choice"})
        self.assertEqual(payload["affected_reservations"][0]["count"], 1320)
        # 校验器拒绝任何静默取消语义
        bad = _proposal_record({"treatment": "silent_cancel", "count": 1})
        errors = validate_event_full(bad)
        self.assertTrue(any("treatment" in e for e in errors))

    def test_recovery_requires_sustained_evidence(self) -> None:
        # 11:30 排队刚降到阈值内但持续不足，不能恢复
        self.assertIsNone(self.ctx["early_restore"])
        early = {s.condition_id: s.satisfied for s in self.ctx["early_status_scenic"]}
        self.assertFalse(early["rc-queue-down"])
        self.assertTrue(early["rc-safety-back"])
        # 11:40 全部条件满足，两个决定都已恢复
        self.assertTrue(all(r is not None for r in self.ctx["restorations"]))
        for decision in self.ctx["trace"]["stages"]["decisions"]:
            self.assertTrue(decision["restored"])
            for cond in decision["recovery_conditions"]:
                self.assertTrue(cond["current"]["satisfied"], cond["condition_id"])

    def test_recovery_evidence_is_metric_level(self) -> None:
        restored = self.ctx["restorations"][0]
        evidence = restored.record["payload"]["evidence"]
        self.assertTrue(all(e["metric"] and e["window"] and e["confidence"] for e in evidence))


class ProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = scenario.build()

    def test_duty_view_ranks_next_to_fail(self) -> None:
        duty = self.ctx["duty_view"]
        watchlist = duty["watchlist"]
        # 已越限排在最前
        self.assertIn(watchlist[0]["severity"], ("critical", "warning"))
        self.assertEqual(watchlist[0]["minutes_to_breach"], 0)
        # 多语急救以 watch 出现，并带有预计失守时间
        medical = [
            w for w in watchlist
            if w["source_id"] == "zjj-medical" and w["metric"] == "wait_time_minutes"
        ]
        self.assertEqual(len(medical), 1)
        self.assertEqual(medical[0]["severity"], "watch")
        self.assertIsNotNone(medical[0]["minutes_to_breach"])
        self.assertGreater(medical[0]["minutes_to_breach"], 0)
        # 每条都带口径与可信度
        self.assertTrue(all(w["basis"] and w["confidence"] for w in watchlist))
        self.assertIn("下一处可能失守", duty["summary"])

    def test_public_view_shows_accessibility_and_community_without_itinerary(self) -> None:
        public = self.ctx["public_view"]
        rendered = json.dumps(public, ensure_ascii=False)
        for forbidden in ("itinerary", "passport", "phone", "booking_ref", "person_ref"):
            self.assertNotIn(forbidden, rendered)
        # 可达性：限流入口状态 + 已预约提示
        names = {e["name"]: e for e in public["accessibility"]}
        self.assertEqual(names["张家界森林公园票务"]["status"], "entry_held")
        self.assertTrue(any("已预约" in n for n in names["张家界森林公园票务"]["notes"]))
        # 社区压力单独呈现
        self.assertTrue(any(i["impact_level"] == 2 for i in public["community_pressure"]))
        # 余票只显示状态分档而非精确数字
        ticket = names["张家界森林公园票务"]
        self.assertIn("tickets", ticket)
        self.assertNotIn("8600", rendered)

    def test_public_view_at_1010_does_not_know_future(self) -> None:
        store = self.ctx["store"]
        board = CapacityBoard(store.as_of_occurred(TS("10:10")))
        view = PublicProjection(board).build(TS("10:10"))
        # 10:10 决定尚未发生，没有入口被限流
        self.assertTrue(all(e["status"] == "open" for e in view["accessibility"]))


class PrivacyTest(unittest.TestCase):
    def test_ephemeral_signals_k_anonymity_and_purge(self) -> None:
        bucket = EphemeralVisitorSignals(ttl_minutes=60)
        for _ in range(10):
            bucket.add(grid="grid:a", language="en", at=TS("10:00"))
        for _ in range(2):
            bucket.add(grid="grid:a", language="fr", at=TS("10:00"))
        needs = bucket.language_needs(TS("10:05"))
        self.assertIn("en", needs)
        self.assertNotIn("fr", needs)  # 小样本不单独输出
        # 超时清除
        self.assertEqual(bucket.language_needs(TS("12:00")), {})

    def test_exact_location_rejected(self) -> None:
        bucket = EphemeralVisitorSignals()
        with self.assertRaises(ValueError):
            bucket.add(grid="38.91N,110.48E", language="en", at=TS("10:00"))

    def test_scene_events_contain_no_personal_data(self) -> None:
        ctx = scenario.build()
        for e in ctx["store"].replay():
            assert_clean_event(e.record)  # 不抛异常即通过

    def test_forbidden_keys_detected(self) -> None:
        with self.assertRaises(PrivacyViolation):
            assert_clean_event({"payload": {"booking_ref": "X1", "value": 1}})

    def test_partner_packets_are_minimized(self) -> None:
        ctx = scenario.build()
        trace = ctx["trace"]
        community = ctx["public_view"]["community_pressure"]
        medical = build_partner_packet(
            "medical_point", trace_view=trace, public_community=community, language_needs=ctx["language_needs"]
        )
        rendered = json.dumps(medical, ensure_ascii=False)
        self.assertNotIn("reservation_policy", medical)
        self.assertNotIn("1320", rendered)
        self.assertIn("language_service_bands", medical)  # 只有档位

        shuttle = build_partner_packet(
            "shuttle_operator", trace_view=trace, public_community=community, language_needs=ctx["language_needs"]
        )
        self.assertNotIn("language_service_bands", shuttle)
        self.assertIn("suggested_routes", shuttle)

        office = build_partner_packet(
            "community_office", trace_view=trace, public_community=community, language_needs=ctx["language_needs"]
        )
        self.assertIn("community_pressure", office)
        self.assertNotIn("bottlenecks", office)
        # 社区办只看到社区道路相关的分流线，看不到接驳备用线等他域方案
        office_routes = {r["route_id"] for r in office.get("suggested_routes", [])}
        self.assertNotIn("route-pianzhen", office_routes)

        # 医疗点只收到医疗维度的资源（第二多语急救组），收不到接驳车/外卡 POS
        medical_resources = {r["kind"] for r in medical.get("resource_requests", [])}
        self.assertEqual(medical_resources, {"multilingual_first_aid_team"})
        medical_bottlenecks = {b["dimension"] for b in medical.get("bottlenecks", [])}
        self.assertTrue(medical_bottlenecks <= {"medical_point"})

        # 接驳公司只收到交通维度
        shuttle_routes = {r["route_id"] for r in shuttle.get("suggested_routes", [])}
        self.assertIn("route-pianzhen", shuttle_routes)
        shuttle_resources = {r["kind"] for r in shuttle.get("resource_requests", [])}
        self.assertEqual(shuttle_resources, {"shuttle_bus"})


class EnvelopeCompatibilityTest(unittest.TestCase):
    LEGACY_EVENT = {
        "event_id": "092102-010-sample-001",
        "event_type": "CAPACITY_REPORTED",
        "aggregate_type": "capacity_source",
        "aggregate_id": "inbound_capacity_coordination-001",
        "occurred_at": "2026-09-20T12:00:00+08:00",
        "version": 1,
        "summary": "入境游承载协调领域样例",
    }

    def test_sample_file_is_valid(self) -> None:
        sample = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_event(sample), [])
        self.assertEqual(validate_event_full(sample), [])

    def test_v01_envelope_without_payload_still_accepted_by_basic_validator(self) -> None:
        # 旧生产方的无 payload 信封：信封级校验保持兼容，可被读取与路由
        self.assertEqual(validate_event(self.LEGACY_EVENT), [])

    def test_naive_datetime_rejected(self) -> None:
        record = dict(self.LEGACY_EVENT)
        record["occurred_at"] = "2026-10-03T10:00:00"
        self.assertTrue(any("时区" in e for e in validate_event(record)))


@unittest.skipUnless(HAS_JSONSCHEMA, "环境未安装 jsonschema（可选依赖）")
class JsonSchemaConformanceTest(unittest.TestCase):
    def test_schema_validates_sample_and_every_scenario_event(self) -> None:
        import jsonschema

        schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        jsonschema.validate(json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8")), schema)
        data = json.loads((ROOT / "data" / "scenario.json").read_text(encoding="utf-8"))
        for record in data["events"]:
            jsonschema.validate(record, schema)

    def test_schema_rejects_silent_cancel_and_unconfirmed_aggregate(self) -> None:
        import jsonschema

        schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        bad = {
            "event_id": "x", "event_type": "DIVERSION_PROPOSED", "aggregate_type": "diversion_plan",
            "aggregate_id": "p", "occurred_at": "2026-10-03T10:00:00+08:00", "version": 1, "summary": "bad",
            "payload": {
                "trigger_signal_ids": ["s"],
                "alternatives": {"time_slots": [{
                    "window": {"start": "2026-10-03T11:00:00+08:00", "end": "2026-10-03T12:00:00+08:00"},
                    "estimated_relief_pct": 10}]},
                "affected_reservations": [{"count": 1, "treatment": "silent_cancel"}]},
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)


# --- 测试辅助构造 ---

def _proposal_record(reservation_item: dict) -> dict:
    return {
        "event_id": "bad-1",
        "event_type": "DIVERSION_PROPOSED",
        "aggregate_type": "diversion_plan",
        "aggregate_id": "plan:bad",
        "occurred_at": "2026-10-03T10:00:00+08:00",
        "version": 1,
        "summary": "bad",
        "payload": {
            "trigger_signal_ids": ["sig-1"],
            "alternatives": {"time_slots": [{"window": WIN("11:00", "12:00"), "estimated_relief_pct": 10}]},
            "affected_reservations": [reservation_item],
        },
    }


if __name__ == "__main__":
    unittest.main()
