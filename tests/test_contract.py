import json
import unittest
from pathlib import Path

from src.scenario import load_sample, load_zhangjiajie
from src.validator import AGGREGATE_TYPES, EVENT_TYPES, validate_event

ROOT = Path(__file__).parents[1]


class ContractTest(unittest.TestCase):
    def test_sample_matches_envelope(self) -> None:
        sample = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_event(sample), [])

    def test_every_scenario_event_validates(self) -> None:
        # 装载本身会逐条校验，这里再显式断言一次
        store = load_zhangjiajie()
        for stored in store.all_stored():
            self.assertEqual(validate_event(stored.event), [], stored.event_id)

    def test_schema_json_enums_match_validator(self) -> None:
        schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        self.assertEqual(set(schema["properties"]["event_type"]["enum"]), EVENT_TYPES)
        self.assertEqual(set(schema["properties"]["aggregate_type"]["enum"]), AGGREGATE_TYPES)

    def test_event_aggregate_pairing_enforced(self) -> None:
        base = {
            "event_id": "x-1", "event_type": "PRESSURE_DETECTED",
            "aggregate_type": "capacity_source",  # 错误配对
            "aggregate_id": "a", "occurred_at": "2026-10-03T12:00:00+08:00",
            "version": 1, "summary": "s",
        }
        self.assertTrue(any("aggregate_type" in e for e in validate_event(base)))

    def test_occurred_at_requires_timezone(self) -> None:
        record = {
            "event_id": "x-2", "event_type": "CAPACITY_REPORTED",
            "aggregate_type": "capacity_source", "aggregate_id": "a",
            "occurred_at": "2026-10-03T12:00:00", "version": 1, "summary": "s",
        }
        self.assertTrue(any("时区" in e for e in validate_event(record)))

    def test_throttle_requires_recovery_condition(self) -> None:
        record = {
            "event_id": "x-3", "event_type": "ACTION_CONFIRMED",
            "aggregate_type": "operating_decision", "aggregate_id": "d",
            "occurred_at": "2026-10-03T12:45:00+08:00", "version": 1, "summary": "s",
            "payload": {
                "decision": "throttle_entry",
                "confirmed_by": {"owner": "transport", "role": "经理", "party": "p-1"},
                "pressure_event_id": "sig-1",
            },
        }
        self.assertTrue(any("recovery_condition" in e for e in validate_event(record)))

    def test_system_cannot_confirm_and_restore_must_evaluate(self) -> None:
        record = {
            "event_id": "x-4", "event_type": "ACTION_CONFIRMED",
            "aggregate_type": "operating_decision", "aggregate_id": "d",
            "occurred_at": "2026-10-03T12:45:00+08:00", "version": 1, "summary": "s",
            "payload": {
                "decision": "throttle_entry",
                "confirmed_by": {"owner": "transport", "role": "自动", "party": "coordination-system"},
                "pressure_event_id": "sig-1",
                "recovery_condition": {"metric_code": "h", "restore_below": 1, "hold_minutes": 5},
            },
        }
        self.assertTrue(any("不能是协调系统自身" in e for e in validate_event(record)))

        restore = dict(record, event_id="x-5", event_type="NORMAL_SERVICE_RESTORED", version=2)
        restore["payload"] = {
            "decision": "lift_throttle",
            "confirmed_by": {"owner": "transport", "role": "经理", "party": "p-1"},
            "restores_decision_event_id": "x-4",
            "pressure_event_id": "sig-1",
            # 故意不提供 satisfied=true 的 evaluation
        }
        errors = validate_event(restore)
        self.assertTrue(any("evaluation" in e for e in errors))

    def test_protected_commitment_rejects_silent_cancel(self) -> None:
        record = {
            "event_id": "x-6", "event_type": "DIVERSION_PROPOSED",
            "aggregate_type": "diversion_plan", "aggregate_id": "plan-1",
            "occurred_at": "2026-10-03T12:42:00+08:00", "version": 1, "summary": "s",
            "payload": {
                "proposed_by": "coordination-system", "pressure_event_id": "sig-1",
                "options": [{"option_type": "alternative_route", "description": "走东线"}],
                "protected_commitments": [
                    {"commitment_ref": "BK-1", "handling": "cancel"}  # 不存在取消语义
                ],
            },
        }
        self.assertTrue(any("静默取消" in e for e in validate_event(record)))

    def test_load_sample_helper(self) -> None:
        self.assertEqual(len(load_sample().all_stored()), 1)


if __name__ == "__main__":
    unittest.main()
