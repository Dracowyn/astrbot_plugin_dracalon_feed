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
    plugin._provider_warn_at = 0
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
    plugin.config = {"targets": ["ok", "missing"], "digest_enabled": False}
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
        "digest_enabled": False,
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
        "digest_enabled": False,
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


@pytest.mark.asyncio
async def test_new_items_wait_in_buffer_until_window_elapses(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "digest_enabled": True,
        "digest_interval_seconds": 1800,
        "digest_max_items": 10,
    }
    plugin._state["bootstrap_done"] = True
    plugin._state["last_flush_at"] = 1_000
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([_item("post")], 1))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 1_100)

    await plugin._poll_once()

    Context.send_message.assert_not_awaited()
    assert len(plugin._state["digest_buffer"]) == 1
    assert plugin._state["watermark"] == feed_state.item_ts(_item("post"))


@pytest.mark.asyncio
async def test_buffer_flushes_as_one_merged_message_when_window_elapses(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "digest_enabled": True,
        "digest_interval_seconds": 1800,
        "digest_max_items": 10,
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 10,
    }
    plugin._state["bootstrap_done"] = True
    plugin._state["last_flush_at"] = 1_000
    plugin._state["digest_buffer"] = [_item("old-a"), _item("old-b")]
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([], 0))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 3_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    chain = Context.send_message.await_args.args[1]
    assert isinstance(chain.chain[0], feed.Comp.Nodes)
    assert len(chain.chain[0].nodes) == 2
    assert plugin._state["digest_buffer"] == []
    assert plugin._state["last_flush_at"] == 3_000


@pytest.mark.asyncio
async def test_item_count_trigger_flushes_before_window(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "digest_enabled": True,
        "digest_interval_seconds": 99_999,
        "digest_max_items": 2,
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 10,
    }
    plugin._state["bootstrap_done"] = True
    plugin._state["last_flush_at"] = 1_000
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    items = [_item("post-a"), _item("post-b")]
    plugin._fetch_page = AsyncMock(return_value=(list(reversed(items)), 2))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 1_100)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    assert plugin._state["digest_buffer"] == []


@pytest.mark.asyncio
async def test_quiet_hours_buffer_and_trim_on_exit(monkeypatch):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "digest_enabled": True,
        "digest_interval_seconds": 60,
        "digest_max_items": 99,
        "quiet_hours_max_catchup": 2,
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 10,
    }
    plugin._state["bootstrap_done"] = True
    plugin._state["was_quiet"] = True
    plugin._state["last_flush_at"] = 0
    plugin._state["digest_buffer"] = [_item(f"old-{i}") for i in range(4)]
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([], 0))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 5_000)

    await plugin._poll_once()

    chain = Context.send_message.await_args.args[1]
    titles = [n.content[0].text for n in chain.chain[0].nodes]
    assert len(chain.chain[0].nodes) == 2
    assert "old-2" in titles[0]
    assert "old-3" in titles[1]
    assert plugin._state["was_quiet"] is False
    assert plugin._state["digest_buffer"] == []


@pytest.mark.asyncio
async def test_digest_disabled_delivers_every_poll(monkeypatch):
    plugin = _plugin()
    plugin.config = {"targets": ["group"], "digest_enabled": False}
    plugin._state["bootstrap_done"] = True
    plugin._state["last_flush_at"] = 999_999
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([_item("post")], 1))

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 1_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    assert plugin._state["digest_buffer"] == []


@pytest.mark.asyncio
async def test_quiet_hours_send_nothing_even_with_digest_disabled(monkeypatch):
    """静默时段必须彻底静音：既不重试 pending，也不结算 buffer。

    关掉窗口合并（digest_enabled=False）时也不例外 —— 这条曾经会绕过静默判定。
    """
    plugin = _plugin()
    plugin.config = {"targets": ["group"], "digest_enabled": False}
    plugin._state["bootstrap_done"] = True
    plugin._state["pending_deliveries"] = [
        {
            "item": _item("old"),
            "targets": ["group"],
            "attempts": 1,
            "next_retry_at": 0,
        }
    ]
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([_item("fresh")], 1))
    plugin._in_quiet_hours = lambda _now_struct: True

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(feed.time, "time", lambda: 10_000)

    await plugin._poll_once()

    Context.send_message.assert_not_awaited()
    assert len(plugin._state["pending_deliveries"]) == 1
    assert len(plugin._state["digest_buffer"]) == 1
    assert plugin._state["watermark"] == feed_state.item_ts(_item("fresh"))
    assert plugin._state["was_quiet"] is True


def _flush_plugin(monkeypatch, *, provider, config_extra=None):
    plugin = _plugin()
    plugin.config = {
        "targets": ["group"],
        "digest_enabled": True,
        "digest_interval_seconds": 60,
        "digest_max_items": 99,
        "merge_push_enabled": True,
        "merge_push_threshold": 2,
        "merge_push_batch_size": 10,
        "review_enabled": True,
    }
    plugin.config.update(config_extra or {})
    plugin._state["bootstrap_done"] = True
    plugin._state["last_flush_at"] = 0
    plugin._lock = asyncio.Lock()
    plugin._save_state = AsyncMock()
    plugin._fetch_page = AsyncMock(return_value=([], 0))
    plugin._review_provider = lambda: provider

    class Context:
        send_message = AsyncMock(return_value=True)

    async def no_sleep(_delay):
        return None

    plugin.context = Context()
    monkeypatch.setattr(feed.asyncio, "sleep", no_sleep)
    return plugin, Context


class _FakeProvider:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    async def text_chat(self, **kwargs):
        self.calls.append(kwargs)
        reply = self.replies.pop(0) if self.replies else ""

        class Resp:
            completion_text = reply

        return Resp()


@pytest.mark.asyncio
async def test_flush_drops_rejected_items_and_records_them(monkeypatch):
    provider = _FakeProvider(
        ['[{"i":0,"keep":false,"reason":"灌水"},{"i":1,"keep":true}]']
    )
    plugin, Context = _flush_plugin(
        monkeypatch, provider=provider, config_extra={"image_review_enabled": False}
    )
    plugin._state["digest_buffer"] = [_item("spam"), _item("good")]
    monkeypatch.setattr(feed.time, "time", lambda: 5_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    chain = Context.send_message.await_args.args[1]
    assert isinstance(chain.chain[0], feed.Comp.Plain)
    assert "good" in chain.chain[0].text
    assert plugin._state["digest_buffer"] == []
    assert plugin._state["filtered_recent"][0]["reason"] == "灌水"
    assert plugin._state["filtered_recent"][0]["source"] == "text"


@pytest.mark.asyncio
async def test_review_failure_defers_buffer_and_sets_backoff(monkeypatch):
    provider = _FakeProvider(["模型抽风"])
    plugin, Context = _flush_plugin(
        monkeypatch, provider=provider, config_extra={"image_review_enabled": False}
    )
    plugin._state["digest_buffer"] = [_item("post")]
    monkeypatch.setattr(feed.time, "time", lambda: 5_000)

    await plugin._poll_once()

    Context.send_message.assert_not_awaited()
    assert len(plugin._state["digest_buffer"]) == 1
    assert plugin._state["review_attempts"] == 1
    assert plugin._state["review_retry_at"] == 5_000 + 120
    assert plugin._state["last_flush_at"] == 0


@pytest.mark.asyncio
async def test_review_releases_batch_after_max_attempts(monkeypatch):
    provider = _FakeProvider(["还是抽风"])
    plugin, Context = _flush_plugin(
        monkeypatch,
        provider=provider,
        config_extra={"image_review_enabled": False, "review_max_attempts": 3},
    )
    plugin._state["digest_buffer"] = [_item("post")]
    plugin._state["review_attempts"] = 2
    monkeypatch.setattr(feed.time, "time", lambda: 5_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    assert plugin._state["digest_buffer"] == []
    assert plugin._state["review_attempts"] == 0
    assert plugin._state["review_retry_at"] == 0


@pytest.mark.asyncio
async def test_missing_provider_pushes_everything(monkeypatch):
    plugin, Context = _flush_plugin(monkeypatch, provider=None)
    plugin._state["digest_buffer"] = [_item("post")]
    monkeypatch.setattr(feed.time, "time", lambda: 5_000)

    await plugin._poll_once()

    assert Context.send_message.await_count == 1
    assert plugin._state["review_attempts"] == 0
    assert plugin._state["digest_buffer"] == []


@pytest.mark.asyncio
async def test_filtered_recent_is_capped(monkeypatch):
    plugin = _plugin()
    plugin._lock = asyncio.Lock()
    plugin._state["filtered_recent"] = [
        {"title": f"t{i}", "url": "u", "reason": "r", "source": "text", "at": 0}
        for i in range(10)
    ]

    plugin._record_filtered(
        [{"item": _item("newest"), "reason": "广告", "source": "text"}]
    )

    assert len(plugin._state["filtered_recent"]) == 10
    assert plugin._state["filtered_recent"][0]["reason"] == "广告"
