"""把 data/ 下的事件 JSON 装入 EventStore。"""

import json
from pathlib import Path

from .event_store import EventStore, parse_dt

DATA_DIR = Path(__file__).parents[1] / "data"


def load_path(path: Path | str) -> EventStore:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    records = data if isinstance(data, list) else [data]
    return load_records(records)


def load_records(records: list[dict]) -> EventStore:
    """按文件顺序作为‘到达顺序’入库：ingested_at 优先取 collection.ingested_at。"""
    store = EventStore()
    for record in records:
        coll = record.get("payload", {}).get("collection")
        if isinstance(coll, dict) and coll.get("ingested_at"):
            ingested = parse_dt(coll["ingested_at"])
        else:
            # 实时事件按发生时刻入库；补传事件才会与发生时刻错位
            ingested = parse_dt(record["occurred_at"])
        store.append(record, ingested_at=ingested)
    return store


def load_sample() -> EventStore:
    return load_path(DATA_DIR / "sample.json")


def load_zhangjiajie() -> EventStore:
    return load_path(DATA_DIR / "scenario_zhangjiajie.json")
