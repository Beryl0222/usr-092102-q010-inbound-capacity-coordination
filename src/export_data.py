"""把张家界场景的完整事件流导出为 data/scenario.json，供跨机构联调。

导出内容：
- events：按 occurred_at 重放后的领域事件（含断网补报，received_at 保留真实接收时刻）；
- late_arrivals：迟到（补报）事件 id 及滞后分钟数；
- correlation：本次限流协同链路 id。
"""

from __future__ import annotations

import json
from pathlib import Path

from . import scenario
from .timeutil import minutes_between


def export() -> dict:
    ctx = scenario.build()
    store = ctx["store"]
    events = [e.record for e in store.replay()]
    late = [
        {
            "event_id": e.event_id,
            "occurred_at": e.occurred_at,
            "received_at": e.received_at,
            "lag_minutes": round(minutes_between(e.occurred_at, e.received_at), 1),
            "sensor_id": e.record.get("source", {}).get("sensor_id"),
        }
        for e in store.late_arrivals(lag_minutes=5)
    ]
    return {
        "description": "2026-10-03 张家界入境游承载协调事件流（含断网补报与一次限流全链路）",
        "schema": "contracts/domain.schema.json",
        "correlation_id": scenario.CORRELATION,
        "late_arrivals": late,
        "event_count": len(events),
        "events": events,
    }


def main() -> None:
    out = Path(__file__).resolve().parents[1] / "data" / "scenario.json"
    payload = export()
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"已导出 {payload['event_count']} 条事件到 {out}")


if __name__ == "__main__":
    main()
