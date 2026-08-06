"""摘要缓冲区：新帖先在这里攒着，够时长或够条数才结算投递。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DEFAULT_INTERVAL_SECONDS = 1800
DEFAULT_MAX_ITEMS = 10
# 结算判定挂在轮询末尾，窗口短于一个轮询间隔没有意义，下限设 60s
MIN_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class DigestSettings:
    enabled: bool
    interval_seconds: int
    max_items: int


def _clamped_int(config: Mapping[str, Any], key: str, *, default: int, floor: int) -> int:
    """读配置整数并夹到下限。

    缺键或值为 None 时取默认值；显式填 0 视为「填小了」而非「没填」，夹到 floor
    而不是回退默认值 —— 否则用户把上限设成 0 反而得到一个大得多的默认值。
    """
    raw = config.get(key, default)
    if raw is None:
        raw = default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(floor, value)


def settings_from_config(config: Mapping[str, Any]) -> DigestSettings:
    return DigestSettings(
        enabled=bool(config.get("digest_enabled", True)),
        interval_seconds=_clamped_int(
            config,
            "digest_interval_seconds",
            default=DEFAULT_INTERVAL_SECONDS,
            floor=MIN_INTERVAL_SECONDS,
        ),
        max_items=_clamped_int(
            config, "digest_max_items", default=DEFAULT_MAX_ITEMS, floor=1
        ),
    )


def enqueue(st: dict[str, Any], items: list[dict[str, Any]]) -> None:
    """把新帖收录进缓冲区（调用方需在同一把锁里推进水位线并落盘）。"""
    if not items:
        return
    st.setdefault("digest_buffer", []).extend(items)


def should_flush(
    st: dict[str, Any],
    settings: DigestSettings,
    *,
    now: int,
    in_quiet_hours: bool,
) -> bool:
    """是否该结算缓冲区。

    静默时段一律不结算，且必须先于「关闭窗口合并」判定 —— 否则关掉窗口合并会连夜间
    静默一起绕过，静默时段照样发消息。
    关闭窗口合并时返回 True，让残留 buffer 立刻投递干净，不留永不投递的帖。
    审查退避期间同样不结算；其余按「条数达标」或「周期到期」双触发。
    """
    if not (st.get("digest_buffer") or []):
        return False
    if in_quiet_hours:
        return False
    if not settings.enabled:
        return True
    if int(st.get("review_retry_at", 0) or 0) > now:
        return False
    if len(st["digest_buffer"]) >= settings.max_items:
        return True
    return now - int(st.get("last_flush_at", 0) or 0) >= settings.interval_seconds


def trim_for_quiet_catchup(
    buffer: list[dict[str, Any]], max_catchup: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """退出静默后的补推截断：只保留最新 max_catchup 条，返回 (保留, 丢弃)。

    丢弃项的水位线在收录时已推进，直接丢即可，不会在下轮复活。
    """
    if max_catchup <= 0 or len(buffer) <= max_catchup:
        return list(buffer), []
    return list(buffer[-max_catchup:]), list(buffer[:-max_catchup])
