from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_dracalon_feed import main as feed
from astrbot_plugin_dracalon_feed import state as feed_state


def _plugin() -> feed.DracalonFeedPlugin:
    plugin = object.__new__(feed.DracalonFeedPlugin)
    plugin.config = {}
    plugin._state = feed_state.default_state()
    plugin._migrate_silent = False
    return plugin


def _item(key: str, published_at: str = "2026-08-01T00:00:00+00:00") -> dict:
    return {
        "item_key": key,
        "title": key,
        "url": f"https://example.com/{key}",
        "published_at": published_at,
    }


@pytest.mark.asyncio
async def test_backlog_page_failure_aborts_whole_collection():
    plugin = _plugin()
    plugin._fetch_page = AsyncMock(return_value=None)

    collected = await plugin._collect_backlog([_item("new")], total=2, watermark=0)

    assert collected is None


def test_select_new_deduplicates_page_overlap():
    plugin = _plugin()
    duplicate = _item("same")

    selected = plugin._select_new([duplicate, dict(duplicate)], watermark=0)

    assert selected == [duplicate]


@pytest.mark.asyncio
async def test_delivery_tracks_false_return_and_exception(monkeypatch):
    plugin = _plugin()

    class Context:
        async def send_message(self, target, _chain):
            if target == "missing":
                return False
            if target == "broken":
                raise RuntimeError("adapter failed")
            return True

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    failed, error = await plugin._deliver_to_targets(
        _item("post"), ["ok", "missing", "broken"]
    )

    assert failed == ["missing", "broken"]
    assert "no matching platform" in error
    assert "adapter failed" in error


@pytest.mark.asyncio
async def test_poll_persists_only_the_failed_target(monkeypatch):
    plugin = _plugin()
    plugin.config = {"targets": ["ok", "missing"]}
    plugin._state["bootstrap_done"] = True
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([_item("post")], 1))

    class Context:
        async def send_message(self, target, _chain):
            return target == "ok"

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 1_000)

    await plugin._poll_once()

    assert plugin._state["watermark"] == feed_state.item_ts(_item("post"))
    assert plugin._state["pending_deliveries"] == [
        {
            "item": _item("post"),
            "targets": ["missing"],
            "attempts": 1,
            "next_retry_at": 1_120,
        }
    ]
    assert "no matching platform" in plugin._state["last_error"]


@pytest.mark.asyncio
async def test_poll_batches_multiple_items_into_merged_messages(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 2,
    }
    plugin._state["bootstrap_done"] = True
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    items = [_item(f"post-{index}") for index in range(5)]
    plugin._fetch_page = AsyncMock(return_value=(list(reversed(items)), len(items)))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)

    await plugin._poll_once()

    assert Context.send_message.await_count == 3
    sent_chains = [call.args[1] for call in Context.send_message.await_args_list]
    assert isinstance(sent_chains[0].chain[0], feed.Comp.Nodes)
    assert isinstance(sent_chains[1].chain[0], feed.Comp.Nodes)
    assert len(sent_chains[0].chain[0].nodes) == 2
    assert len(sent_chains[1].chain[0].nodes) == 2
    payload = await sent_chains[0].chain[0].to_dict()
    assert len(payload["messages"]) == 2
    assert payload["messages"][0]["data"]["nickname"] == "Dracalon 新帖"
    assert isinstance(sent_chains[2].chain[0], feed.Comp.Plain)
    assert plugin._state["pending_deliveries"] == []


@pytest.mark.asyncio
async def test_failed_merged_message_queues_every_item(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["missing"],
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 5,
    }
    plugin._state["bootstrap_done"] = True
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    items = [_item("post-a"), _item("post-b")]
    plugin._fetch_page = AsyncMock(return_value=(list(reversed(items)), len(items)))

    class Context:
        send_message = AsyncMock(return_value=False)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 1_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    assert len(plugin._state["pending_deliveries"]) == 2
    assert {
        entry["item"]["item_key"]
        for entry in plugin._state["pending_deliveries"]
    } == {"post-a", "post-b"}
    assert all(
        entry["targets"] == ["missing"]
        for entry in plugin._state["pending_deliveries"]
    )


@pytest.mark.asyncio
async def test_poll_respects_pending_retry_time(monkeypatch):
    plugin = _plugin()
    plugin.config = {"targets": ["missing"]}
    plugin._state.update(
        {
            "bootstrap_done": True,
            "pending_deliveries": [
                {
                    "item": _item("post"),
                    "targets": ["missing"],
                    "attempts": 2,
                    "next_retry_at": 2_000,
                }
            ],
            "last_error": "previous failure",
        }
    )
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([], 0))

    class Context:
        send_message = AsyncMock(return_value=True)

    plugin.context = Context()
    monkeypatch.setattr(feed.time, "time", lambda: 1_000)

    await plugin._poll_once()

    Context.send_message.assert_not_awaited()
    assert plugin._state["pending_deliveries"][0]["next_retry_at"] == 2_000
    assert plugin._state["last_error"] == "previous failure"


def test_schema_two_state_migrates_without_resetting_watermark(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema": 2,
                "watermark": 123,
                "boundary_keys": ["kept"],
                "bootstrap_done": True,
            }
        ),
        encoding="utf-8",
    )
    state = feed_state.load_state(state_path)

    assert state["schema"] == feed_state.STATE_SCHEMA
    assert state["watermark"] == 123
    assert state["boundary_keys"] == ["kept"]
    assert state["pending_deliveries"] == []


def test_empty_bootstrap_waits_for_a_valid_feed_item():
    plugin = _plugin()
    plugin._migrate_silent = True

    plugin._apply_bootstrap([])

    assert plugin._state["bootstrap_done"] is False
    assert plugin._migrate_silent is True

    plugin._apply_bootstrap([_item("first")])

    assert plugin._state["bootstrap_done"] is True
    assert plugin._migrate_silent is False
    assert plugin._state["boundary_keys"] == ["first"]
