from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_dracalon_feed import main as feed
from astrbot_plugin_dracalon_feed import state as feed_state

PLUGIN_ROOT = Path(feed.__file__).resolve().parent


class _Event:
    unified_msg_origin = "aiocqhttp:GroupMessage:123"

    def plain_result(self, text):
        return text


def _plugin() -> feed.DracalonFeedPlugin:
    plugin = object.__new__(feed.DracalonFeedPlugin)
    plugin.config = {}
    plugin._state = feed_state.default_state()
    plugin._migrate_silent = False
    plugin._provider_warn_at = 0
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    return plugin


async def _collect(agen) -> list[str]:
    return [item async for item in agen]


def test_conf_schema_declares_every_new_key():
    schema = json.loads(
        (PLUGIN_ROOT / "_conf_schema.json").read_text(encoding="utf-8")
    )

    for key in (
        "digest_enabled",
        "digest_interval_seconds",
        "digest_max_items",
        "review_enabled",
        "review_provider_id",
        "review_extra_rules",
        "review_max_attempts",
        "review_timeout_seconds",
        "image_review_enabled",
        "image_review_max_per_batch",
    ):
        assert key in schema, key
        assert "description" in schema[key]
        assert "default" in schema[key]


@pytest.mark.asyncio
async def test_flush_command_reports_empty_buffer():
    plugin = _plugin()

    lines = await _collect(plugin.flush(_Event()))

    assert "没有待推送" in lines[0]


@pytest.mark.asyncio
async def test_flush_command_forces_delivery():
    plugin = _plugin()
    plugin.config = {"targets": ["group"], "review_enabled": False}
    plugin._state["digest_buffer"] = [{"url": "https://a", "title": "帖子"}]
    plugin._state["review_retry_at"] = 99_999_999
    plugin._flush_digest = AsyncMock(return_value=(1, 0, False))

    lines = await _collect(plugin.flush(_Event()))

    plugin._flush_digest.assert_awaited_once()
    assert plugin._state["review_retry_at"] == 0
    assert "1" in lines[0]


@pytest.mark.asyncio
async def test_filtered_command_lists_recent_drops():
    plugin = _plugin()
    plugin._state["filtered_recent"] = [
        {
            "title": "水帖",
            "url": "https://x",
            "reason": "灌水",
            "source": "text",
            "at": 0,
        }
    ]

    lines = await _collect(plugin.filtered(_Event()))

    assert "水帖" in lines[0]
    assert "灌水" in lines[0]


@pytest.mark.asyncio
async def test_filtered_command_when_empty():
    plugin = _plugin()

    lines = await _collect(plugin.filtered(_Event()))

    assert "还没有" in lines[0]


@pytest.mark.asyncio
async def test_status_reports_digest_and_review():
    plugin = _plugin()
    plugin.config = {"digest_enabled": True, "review_enabled": True}
    plugin._state["digest_buffer"] = [{"url": "https://a"}]
    plugin._state["review_attempts"] = 1

    lines = await _collect(plugin.status(_Event()))

    assert "缓冲区" in lines[0]
    assert "审查" in lines[0]
