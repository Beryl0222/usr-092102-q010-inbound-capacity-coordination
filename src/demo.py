"""命令行演示：python3 -m src.demo

按“发生时间真相 → 触发信号 → 协同提案 → 责任方决定 → 恢复”的顺序，
打印一次张家界限流的完整链路、两类页面投影与合作机构最小信息包。
"""

from __future__ import annotations

import json

from . import scenario
from .privacy import build_partner_packet


def main() -> None:
    ctx = scenario.build()
    store = ctx["store"]

    print("=" * 78)
    print("一、断网传感器的正确重放")
    print("=" * 78)
    late = store.late_arrivals(lag_minutes=5)
    for e in late:
        p = e.record["payload"]
        print(
            f"  {e.record['source'].get('sensor_id')} 读数发生于 {e.occurred_at[11:16]}"
            f"（{p['readings'][0]['metric']}={p['readings'][0]['value']}），"
            f"断网后 {e.received_at[11:16]} 才补报；重放位置按发生时间归位"
        )
    received_order = [e for e in store.all_events() if e.is_backfill]
    print(f"  接收顺序里两条补报在 10:15 连续到达；重放顺序：")
    for e in store.replay():
        if e.event_type == "CAPACITY_REPORTED" and e.record["payload"]["scope"]["source_id"] == "zjj-shuttle-queue":
            tag = "（补报归位）" if e.is_backfill else ""
            print(f"    {e.occurred_at[11:16]}  排队 {e.record['payload']['readings'][0]['value']:>4} 分钟 {tag}")

    print()
    print("=" * 78)
    print("二、值班视角：下一处将要失守的环节（10:25）")
    print("=" * 78)
    for item in ctx["duty_view"]["watchlist"]:
        print(
            f"  [{item['severity'].upper():8}] {item['name']}｜{item['metric']}={item['observed']}"
            f"｜口径：{item['basis']}｜可信度：{item['confidence']}｜{item['alert']}"
        )
    print(f"  研判：{ctx['duty_view']['summary']}")

    print()
    print("=" * 78)
    print("三、限流全链路追踪：触发 → 提案 → 决定 → 恢复")
    print("=" * 78)
    trace = ctx["trace"]
    print(f"  关联号：{trace['correlation_id']}　最终状态：{trace['status']}")
    for node in trace["timeline"]:
        print(f"  {node['at'][11:16]}  {node['event_type']:<26} {node['summary']}")

    print()
    print("  恢复条件核对：")
    for decision in trace["stages"]["decisions"]:
        who = decision["confirmed_by"]["org_id"]
        for cond in decision["recovery_conditions"]:
            cur = cond["current"]
            print(
                f"    {who}｜{cond['metric']} {cond['operator']} {cond['threshold']}"
                f"（持续 {cond['sustain_minutes']} 分钟）→ "
                f"{'已满足' if cur and cur['satisfied'] else '未满足'}：{cur['detail'] if cur else '无评估'}"
            )

    print()
    print("  11:30 首次恢复评估（持续不足，系统未解除）：")
    for status in ctx["early_status_scenic"]:
        print(f"    {status.condition_id}: {'满足' if status.satisfied else '未满足'} — {status.detail}")
    print(f"  11:30 是否产生恢复事件：{'是' if ctx['early_restore'] else '否（正确：证据未齐）'}")

    print()
    print("=" * 78)
    print("四、公众页（10:40）：可达性 + 社区压力，不含个人行程")
    print("=" * 78)
    public = ctx["public_view"]
    print(f"  头条：{public['headline']}")
    for entry in public["accessibility"]:
        if entry["dimension"] == "community_road":
            continue
        print(f"  · {entry['name']}：{entry['status']} {'／'.join(entry['notes'])}")
    for item in public["community_pressure"]:
        print(f"  · 社区道路：{item['impact_text']}（{item['name']}）")
    print(f"  隐私声明：{public['privacy_note']}")

    print()
    print("=" * 78)
    print("五、合作机构最小信息包（同一事实，各取所需）")
    print("=" * 78)
    for role in ("shuttle_operator", "medical_point", "community_office"):
        packet = build_partner_packet(
            role,
            trace_view=trace,
            public_community=public["community_pressure"],
            language_needs=ctx["language_needs"],
        )
        print(f"  【{role}】")
        print("    " + json.dumps(packet, ensure_ascii=False, indent=2).replace("\n", "\n    "))

    print()
    print("  当次会话的匿名语种需求（小样本不输出，会话结束即清除）：")
    print(f"    {ctx['language_needs']}")

    print()
    print("=" * 78)
    print("六、预约承诺保护")
    print("=" * 78)
    proposal_payload = ctx["proposal"].record["payload"]
    for item in proposal_payload.get("affected_reservations", []):
        treatment = {"honor": "继续兑现", "offer_choice": "提供等价选择、原预约保留"}[item["treatment"]]
        print(f"  涉及预约 {item['count']} 个，处理原则：{treatment}；系统无静默取消原语")


if __name__ == "__main__":
    main()
