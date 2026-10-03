"""对外视图与当次匿名上下文。

两层视图严格分离：
- PublicView：面向公众页面。只给局部点位的聚合可达性与社区压力分档，不含任何
  个人/团队行程、预约引用或精确内部数值；公众由此知道‘哪里挤、哪条路影响居民’，
  但看不到任何人的行程。
- PartnerView：面向合作机构。按事件上的 data_sharing 最小授权过滤：public 人人
  可见；partners 仅对 allowed_partners 内机构开放；restricted 仅对本机构开放。
  协调系统默认不附带共享块的跨机构事件按 restricted 处理。

匿名位置与语言偏好只服务当次保障：EphemeralContext 纯内存、短时过期，绝不写入
事件流，合作机构只能得到完成本次调整所需的那部分上下文。
"""

import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import monotonic

from .event_store import EventStore
from .monitoring import DutyBoard, Projection

# 公众分档标签（中英双语由语言偏好选择，默认中文）
_ACCESS_LABELS = {
    "open": {"zh": "通畅", "en": "Open"},
    "busy": {"zh": "拥挤", "en": "Busy"},
    "very_busy": {"zh": "非常拥挤", "en": "Very busy"},
    "throttled": {"zh": "正在限流·请按引导分时前往", "en": "Entry throttled · follow timed guidance"},
    "unknown": {"zh": "数据更新中", "en": "Status updating"},
}
_COMMUNITY_LABELS = {
    "none": {"zh": "居民出行暂无影响", "en": "No impact on residents"},
    "noticeable": {"zh": "周边居民出行略有压力", "en": "Some pressure on local residents"},
    "disrupted": {"zh": "社区道路受影响·请绕行外环", "en": "Local roads affected · please use the bypass"},
}
_SELLABLE_LABELS = {
    "available": {"zh": "可约", "en": "Available"},
    "tight": {"zh": "余票紧张", "en": "Few left"},
    "sold_out": {"zh": "当前时段暂无可售", "en": "Sold out for this slot"},
    "unknown": {"zh": "—", "en": "—"},
}


def _event_owner(event: dict, store: EventStore | None = None) -> str | None:
    etype = event["event_type"]
    payload = event.get("payload", {})
    if etype in ("CAPACITY_REPORTED", "PRESSURE_DETECTED"):
        return payload.get("owner")
    if etype in ("ACTION_CONFIRMED", "NORMAL_SERVICE_RESTORED"):
        return payload.get("confirmed_by", {}).get("owner")
    if etype == "DIVERSION_PROPOSED" and store is not None:
        pressure = store.get(payload["pressure_event_id"])
        return pressure["payload"].get("owner")
    return None


def _is_visible_to(event: dict, partner: str, store: EventStore) -> bool:
    sharing = event.get("data_sharing")
    if sharing is None:
        # 无共享声明：仅本机构可见
        return _event_owner(event, store) == partner
    visibility = sharing["visibility"]
    if visibility == "public":
        return True
    if visibility == "restricted":
        return _event_owner(event, store) == partner
    if visibility == "partners":
        allowed = sharing.get("allowed_partners", [])
        return partner in allowed or _event_owner(event, store) == partner
    return False


class PublicView:
    """公众页只基于机构显式标记 visibility=public 的事件构建，不触碰 restricted 数据。"""

    def __init__(self, store: EventStore, now: datetime, known_at: datetime | None = None):
        self.store = store
        self.now = now
        self.known_at = known_at
        public_events = [
            s for s in store.replay(known_at=known_at)
            if (s.event.get("data_sharing") or {}).get("visibility") == "public"
        ]
        self.projection = Projection()
        for stored in public_events:
            self.projection.apply(stored)
        self.board = DutyBoard(self.projection, now)
        self._throttled_points = self._active_throttle_points()

    def _active_throttle_points(self) -> set[tuple[str, str | None]]:
        """生效中的限流所覆盖的 (责任机构, 局部点位)。

        压力信号本身可能不是 public 事件，但责任方确认的限流决定是 public 的；
        通过 store 内部关联回到点位身份，再与公众投影按 (owner, location_id) 对齐。
        """
        lifted = {
            e["payload"].get("restores_decision_event_id")
            for e in self.projection.decisions
            if e["event_type"] == "NORMAL_SERVICE_RESTORED"
        }
        points: set[tuple[str, str | None]] = set()
        for e in self.projection.decisions:
            if e["event_type"] == "ACTION_CONFIRMED" and e["event_id"] not in lifted:
                pressure = self.store.get(e["payload"]["pressure_event_id"])
                p = pressure["payload"]
                points.add((p["owner"], p.get("location_id")))
        return points

    @staticmethod
    def _sellable_band(value: float | None) -> str:
        if value is None:
            return "unknown"
        if value <= 0:
            return "sold_out"
        if value <= 50:  # 分档阈值应由各机构在口径中配置，这里取保守默认
            return "tight"
        return "available"

    def render(self, language: str = "zh") -> list[dict]:
        result = []
        for status in self.board.service_statuses():
            st = status.state
            if (st.owner, st.location_id) in self._throttled_points:
                accessibility = "throttled"
            elif status.severity == "critical":
                accessibility = "very_busy"
            elif status.severity in ("breach", "projected_watch"):
                accessibility = "busy"
            elif status.severity == "stale":
                accessibility = "unknown"
            else:
                accessibility = "open"
            sellable_band = self._sellable_band(st.sellable_quantity)
            result.append({
                # 只有点位类别与局部标识，不含任何行程/预约信息
                "point": {"owner": st.owner, "location_id": st.location_id},
                "accessibility": accessibility,
                "accessibility_text": _ACCESS_LABELS[accessibility][language],
                "tickets": sellable_band,
                "tickets_text": _SELLABLE_LABELS[sellable_band][language],
                "community_pressure": st.community_level,
                "community_text": _COMMUNITY_LABELS[st.community_level][language],
                "observed_at": st.observed_at.isoformat() if st.observed_at else None,
            })
        return result

    def render_text(self, language: str = "zh") -> str:
        sep = "：" if language == "zh" else ": "
        joiner = "，" if language == "zh" else "; "
        title = "张家界承载情况（公众页）" if language == "zh" else "Zhangjiajie status (public)"
        lines = [title]
        for row in self.render(language):
            point = row["point"]
            head = f"{point['owner']}/{point.get('location_id') or '—'}"
            parts = [row["accessibility_text"]]
            if row["tickets"] != "unknown":
                parts.append(row["tickets_text"])
            parts.append(row["community_text"])
            lines.append(f"- {head}{sep}{joiner.join(parts)}")
        return "\n".join(lines)


class PartnerView:
    def __init__(self, store: EventStore, partner: str):
        self.store = store
        self.partner = partner

    def visible_events(self) -> list[dict]:
        """该机构完成本次调整所需可见的事件（按事件时间），其余一律不可见。"""
        return [
            s.event
            for s in self.store.replay()
            if _is_visible_to(s.event, self.partner, self.store)
        ]

    def shared_signal(self, capacity_aggregate_id: str) -> dict | None:
        """机构视角下某个跨机构信号点的最小数值视图；无权访问时返回 None。"""
        for stored in reversed(self.store.history(capacity_aggregate_id)):
            event = stored.event
            if event["event_type"] != "CAPACITY_REPORTED" or not _is_visible_to(event, self.partner, self.store):
                continue
            p = event["payload"]
            # 只回传完成分流所需字段，剔除采集器身份等内部细节
            return {
                "capacity_id": capacity_aggregate_id,
                "owner": p["owner"],
                "location_id": p.get("location_id"),
                "window": p["window"],
                "metric_code": p["metric"]["code"],
                "sellable_quantity": p.get("sellable_quantity"),
                "realtime_occupancy": p.get("realtime_occupancy"),
                "safety_headroom": p.get("safety_headroom"),
                "service_level": (p.get("service_degradation") or {}).get("level", "normal"),
                "confidence": (p.get("confidence") or {}).get("level"),
            }
        return None


@dataclass
class _Session:
    token: str
    language_preference: str
    coarse_area: str          # 粗粒度区域码，例如 grid-A3，绝不是精确轨迹
    created_at: datetime
    expires_at: datetime
    purpose: str = "current_safeguard_only"


class EphemeralContext:
    """当次保障的匿名上下文：内存保存、短时过期、随会话结束即弃。

    与事件存储物理隔离；to_partner_hint 只给出完成一次分流所需的最小片段。
    """

    def __init__(self, ttl_minutes: int = 120):
        self.ttl = timedelta(minutes=ttl_minutes)
        self._sessions: dict[str, _Session] = {}

    def open_session(self, language_preference: str, coarse_area: str, now: datetime | None = None) -> str:
        now = now or datetime.now(timezone.utc)
        token = secrets.token_urlsafe(12)
        self._sessions[token] = _Session(
            token=token,
            language_preference=language_preference,
            coarse_area=coarse_area,
            created_at=now,
            expires_at=now + self.ttl,
        )
        return token

    def use(self, token: str, now: datetime | None = None) -> _Session | None:
        now = now or datetime.now(timezone.utc)
        session = self._sessions.get(token)
        if session is None:
            return None
        if now >= session.expires_at:
            self._sessions.pop(token, None)
            return None
        return session

    def close(self, token: str) -> None:
        self._sessions.pop(token, None)

    def to_partner_hint(self, token: str, now: datetime | None = None) -> dict | None:
        """给合作机构的最小提示：仅语言与粗区域，且标注一次性用途。"""
        session = self.use(token, now)
        if session is None:
            return None
        return {
            "language_preference": session.language_preference,
            "coarse_area": session.coarse_area,
            "purpose": session.purpose,
            "expires_at": session.expires_at.isoformat(),
        }
