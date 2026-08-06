"""推送状态：默认值、加载迁移、落盘快照，以及水位线判定与推进。

水位线（watermark + boundary_keys）表达「已处理进度」，取代旧版本基于 pushed_urls
的去重方式。本模块为纯函数集合，状态 dict 一律由调用方持有并传入。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from astrbot.api import logger

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"
STATE_SCHEMA = 4


def url_hash(url: str) -> str:
    """本地 URL 哈希。仅作 item_key 回退（后端旧版本未输出 item_key 时）使用。"""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def default_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        # watermark：已推帖的最大 published_at（epoch 秒，UTC）。
        # boundary_keys：published_at 恰好 == watermark 且已处理（推送/标记）过的 item_key。
        # 二者一起表达「已推进度」：published_at 严格大于 watermark 的一定是新帖；
        # 恰好等于 watermark 的，靠 item_key 是否在 boundary 里区分（防同秒帖漏推/重推）。
        "watermark": 0,
        "boundary_keys": [],
        "last_poll_at": 0,
        "last_error": "",
        "bootstrap_done": False,
        "was_quiet": False,
        "pending_deliveries": [],
        # digest_buffer：已收录但尚未结算投递的帖子。水位线在收录时就推进，
        # 因此「已收录未投递」的帖只存在于这里，与水位线在同一次落盘中写入。
        "digest_buffer": [],
        "last_flush_at": 0,
        # review_attempts / review_retry_at：审查连续失败次数与退避到期时间。
        "review_attempts": 0,
        "review_retry_at": 0,
        # filtered_recent：最近被审查毙掉的帖，供 /dracalon_feed filtered 调 prompt 用。
        "filtered_recent": [],
    }


# ----------------------------------------------------------------------
# 水位线判定 / 推进
# ----------------------------------------------------------------------
def item_ts(item: dict[str, Any]) -> int | None:
    """把 item.published_at（后端 gmdate('c') → ISO8601）解析成 epoch 秒。

    无发布时间或无法解析时返回 None —— 这类条目不参与水位线判定，也不会被推送。
    """
    raw = item.get("published_at")
    if not raw:
        return None
    # 防御：后端契约是 ISO8601 字符串，但万一改成数字 epoch 也能正确解析（而非静默丢帖）
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        ts = int(raw)
        return ts if ts > 0 else None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def item_key(item: dict[str, Any]) -> str | None:
    """稳定去重键。优先用后端下发的 item_key = sha256(normalize(url))；
    后端旧版本未输出时回退到本地 raw-url 哈希（仅用于同秒 tie-break，可接受）。
    """
    k = item.get("item_key")
    if isinstance(k, str) and k:
        return k
    url = item.get("url")
    if url:
        return url_hash(str(url))
    return None


def oldest_ts(items: list[dict[str, Any]]) -> int | None:
    """页内自末尾起第一条「有发布时间」条目的 ts。

    无发布时间的条目在后端 latest 排序里沉底，深翻页判断「是否已翻过水位线」时
    必须跳过它们，否则末条恰为无时间帖会让翻页提前停在 None 上。
    """
    for it in reversed(items):
        ts = item_ts(it)
        if ts is not None:
            return ts
    return None


def select_new(
    items: list[dict[str, Any]], watermark: int, boundary_keys: set[str]
) -> list[dict[str, Any]]:
    """挑出尚未推送过的新帖：published_at 严格大于水位线，或恰好等于水位线
    但 item_key 不在 boundary（同秒未推过的）。"""
    selected_keys: set[str] = set()
    out: list[dict[str, Any]] = []
    for it in items:
        ts = item_ts(it)
        if ts is None:
            continue
        key = item_key(it)
        if not key or key in selected_keys:
            continue
        if ts > watermark or (ts == watermark and key not in boundary_keys):
            out.append(it)
            selected_keys.add(key)
    return out


def advance_watermark(st: dict[str, Any], item: dict[str, Any]) -> None:
    """把一条已处理（推送或标记已读）的帖子并入水位线。

    必须按 published_at 升序调用，水位线才单调推进。同秒帖累积进 boundary，
    水位线一旦前进 boundary 就清空换新 —— 因此 boundary 至多保留同一秒的条目数，
    不会无限增长，无需额外清理（这正是替代旧 pushed_urls + 按天数 prune 的关键）。
    """
    ts = item_ts(item) or 0
    key = item_key(item)
    if not key:
        return
    wm = int(st.get("watermark", 0) or 0)
    if ts > wm:
        st["watermark"] = ts
        st["boundary_keys"] = [key]
    elif ts == wm:
        boundary = st.setdefault("boundary_keys", [])
        if key not in boundary:
            boundary.append(key)
    # ts < wm：更旧的条目，已在水位线之下，忽略


def apply_bootstrap(st: dict[str, Any], items: list[dict[str, Any]], mode: str) -> None:
    """首次启动设定水位线基线（不直接推送，推送交给统一流程）。

    - push_all：水位线归零，统一流程随后把当前全部帖子推一遍（慎用）。
    - mark_all：水位线设到最新帖、boundary 含最新秒全部 key → 当前帖全标记已读、0 推送。
    - latest_one（默认）：水位线设到最新帖，但 boundary 排除「最新那一条」的 key
      → 统一流程恰好推这 1 条；其余（含同秒的）都在水位线下或 boundary 内被跳过。
    迁移场景由调用方把 mode 算成 mark_all：旧版本升级且此前已运行过时零重推。
    """
    dated = sorted(
        (ts, k)
        for ts, k in ((item_ts(it), item_key(it)) for it in items)
        if ts is not None and k
    )
    if not dated:
        return

    st["bootstrap_done"] = True

    if mode == "push_all":
        st["watermark"] = 0
        st["boundary_keys"] = []
        return

    newest_ts, newest_key = dated[-1]
    keys_at_newest = [k for ts, k in dated if ts == newest_ts]
    st["watermark"] = newest_ts
    if mode == "mark_all":
        st["boundary_keys"] = keys_at_newest
    else:  # latest_one：放过最新 1 条
        st["boundary_keys"] = [k for k in keys_at_newest if k != newest_key]


# ----------------------------------------------------------------------
# state.json
# ----------------------------------------------------------------------
def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_state()
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("state.json is not an object")
    except Exception as e:
        logger.error(f"[{PLUGIN_NAME}] state.json corrupted ({e}), reset to default")
        return default_state()

    merged = default_state()
    old_schema = int(data.get("schema", 1) or 1)
    if old_schema >= 2 and "watermark" in data:
        merged.update(data)
        merged["schema"] = STATE_SCHEMA
        merged["boundary_keys"] = list(merged.get("boundary_keys") or [])
        merged["watermark"] = int(merged.get("watermark", 0) or 0)
        merged["pending_deliveries"] = [
            entry
            for entry in (merged.get("pending_deliveries") or [])
            if isinstance(entry, dict)
            and isinstance(entry.get("item"), dict)
            and isinstance(entry.get("targets"), list)
        ]
        merged["digest_buffer"] = [
            entry
            for entry in (merged.get("digest_buffer") or [])
            if isinstance(entry, dict) and entry.get("url")
        ]
        merged["filtered_recent"] = [
            entry
            for entry in (merged.get("filtered_recent") or [])
            if isinstance(entry, dict)
        ]
        merged["last_flush_at"] = int(merged.get("last_flush_at", 0) or 0)
        merged["review_attempts"] = int(merged.get("review_attempts", 0) or 0)
        merged["review_retry_at"] = int(merged.get("review_retry_at", 0) or 0)
        return merged

    # 旧版本（schema 1：基于 pushed_urls + 按天数 prune 去重）→ 迁移到 watermark。
    # 丢弃 pushed_urls；若此前已 bootstrap 过（在线上跑过），标记下轮静默 mark_all、
    # 把当前 feed 全部对齐已读，升级零重推。仅缺时间戳的极少数帖会在迁移窗口被跳过。
    had_run = bool(data.get("bootstrap_done")) or bool(data.get("pushed_urls"))
    merged.update(
        {
            k: v
            for k, v in data.items()
            if k in ("last_poll_at", "last_error", "was_quiet")
        }
    )
    merged["schema"] = STATE_SCHEMA
    merged["watermark"] = 0
    merged["boundary_keys"] = []
    merged["bootstrap_done"] = False
    merged["_needs_silent_migration"] = had_run
    logger.info(
        f"[{PLUGIN_NAME}] migrated state schema {old_schema} → {STATE_SCHEMA}"
        f"{'（静默 mark_all 对齐，零重推）' if had_run else ''}"
    )
    return merged


def snapshot_state(st: dict[str, Any]) -> dict[str, Any]:
    """取出需要落盘的字段，顺带做类型收敛。"""
    return {
        "schema": STATE_SCHEMA,
        "watermark": int(st.get("watermark", 0) or 0),
        "boundary_keys": list(st.get("boundary_keys", []) or []),
        "last_poll_at": int(st.get("last_poll_at", 0) or 0),
        "last_error": str(st.get("last_error", "") or ""),
        "bootstrap_done": bool(st.get("bootstrap_done", False)),
        "was_quiet": bool(st.get("was_quiet", False)),
        "pending_deliveries": list(st.get("pending_deliveries", []) or []),
        "digest_buffer": list(st.get("digest_buffer", []) or []),
        "last_flush_at": int(st.get("last_flush_at", 0) or 0),
        "review_attempts": int(st.get("review_attempts", 0) or 0),
        "review_retry_at": int(st.get("review_retry_at", 0) or 0),
        "filtered_recent": list(st.get("filtered_recent", []) or []),
    }


def write_state_file(path: Path, snapshot: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)
