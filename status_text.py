"""/dracalon_feed status 命令的状态文案拼装。

纯函数：只读 config / state 快照与调用方算好的两个布尔值，不碰网络、锁或
命令上下文，便于单测，也把 main.py 的 status 命令方法体瘦身到几行。
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from . import digest, review


def build_lines(
    config: Mapping[str, Any],
    st: Mapping[str, Any],
    *,
    in_quiet_hours: bool,
    review_provider_available: bool,
) -> list[str]:
    """拼装完整状态文案（不含首行以外的前缀/后缀，调用方直接 join 输出）。"""
    last_poll_at = int(st.get("last_poll_at", 0) or 0)
    last_poll_str = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_poll_at))
        if last_poll_at
        else "尚未轮询过"
    )
    enabled = bool(config.get("enabled", True))
    targets = config.get("targets", []) or []
    watermark = int(st.get("watermark", 0) or 0)
    watermark_str = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(watermark))
        if watermark
        else "尚未推送"
    )
    last_error = st.get("last_error") or "无"
    bootstrap_done = bool(st.get("bootstrap_done", False))
    pending_count = sum(
        len(entry.get("targets", []))
        for entry in st.get("pending_deliveries", [])
        if isinstance(entry, dict)
    )

    if config.get("quiet_hours_enabled", False):
        qs = int(config.get("quiet_hours_start", 0) or 0) % 24
        qe = int(config.get("quiet_hours_end", 0) or 0) % 24
        quiet_desc = (
            f"{qs:02d}:00–{qe:02d}:00（{'静默中' if in_quiet_hours else '非静默'}）"
        )
    else:
        quiet_desc = "未启用"

    if config.get("merge_push_enabled", True):
        merge_threshold = max(2, int(config.get("merge_push_threshold", 2) or 2))
        merge_batch_size = max(
            merge_threshold,
            min(20, int(config.get("merge_push_batch_size", 5) or 5)),
        )
        merge_desc = f"已启用（{merge_threshold} 条起，每批最多 {merge_batch_size} 条）"
    else:
        merge_desc = "未启用"

    digest_settings = digest.settings_from_config(config)
    buffered = len(st.get("digest_buffer") or [])
    if digest_settings.enabled:
        last_flush_at = int(st.get("last_flush_at", 0) or 0)
        next_flush_at = last_flush_at + digest_settings.interval_seconds
        next_flush = (
            time.strftime("%H:%M:%S", time.localtime(next_flush_at))
            if last_flush_at
            else "下次轮询即结算"
        )
        digest_desc = (
            f"已启用（{digest_settings.interval_seconds // 60} 分钟窗口／"
            f"满 {digest_settings.max_items} 条提前结算）"
        )
    else:
        next_flush = "未启用"
        digest_desc = "未启用（发现即推）"

    review_settings = review.settings_from_config(config)
    if not review_settings.enabled:
        review_desc = "未启用"
    elif not review_provider_available:
        review_desc = "未配置可用模型（全部放行）"
    else:
        attempts = int(st.get("review_attempts", 0) or 0)
        review_desc = (
            f"推迟重试中（{attempts}/{review_settings.max_attempts}）"
            if attempts
            else "正常"
        )
        if not review_settings.image_enabled:
            review_desc += "，配图审查已关"

    return [
        "Dracalon 新帖订阅 · 当前状态",
        f"  推送开关：{'已启用' if enabled else '已暂停'}",
        f"  夜间静默：{quiet_desc}",
        f"  合并推送：{merge_desc}",
        f"  窗口合并：{digest_desc}",
        f"  缓冲区：{buffered} 条待推，下次结算 {next_flush}",
        f"  AI 审查：{review_desc}",
        f"  上次轮询：{last_poll_str}",
        f"  上次错误：{last_error}",
        f"  推送水位线：{watermark_str}（早于此发布时间的帖子视为已推）",
        f"  绑定目标数：{len(targets)}",
        f"  待重试投递数：{pending_count}",
        f"  首次启动初始化：{'已完成' if bootstrap_done else '尚未完成'}",
    ]
