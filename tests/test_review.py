from __future__ import annotations

import asyncio

import pytest

from astrbot_plugin_dracalon_feed import review


def _settings(**kw) -> review.ReviewSettings:
    base = {
        "enabled": True,
        "provider_id": "",
        "extra_rules": "",
        "max_attempts": 3,
        "timeout_seconds": 30,
        "image_enabled": False,
        "image_max_per_batch": 10,
    }
    base.update(kw)
    return review.ReviewSettings(**base)


def _item(key: str, **kw) -> dict:
    item = {
        "item_key": key,
        "title": key,
        "url": f"https://example.com/{key}",
        "community": "测试区",
    }
    item.update(kw)
    return item


class FakeProvider:
    """按调用顺序吐预设回复；replies 里的元素可以是字符串或异常实例。"""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    async def text_chat(self, **kwargs):
        self.calls.append(kwargs)
        reply = self.replies.pop(0) if self.replies else ""
        if isinstance(reply, BaseException):
            raise reply

        class Resp:
            completion_text = reply

        return Resp()


def test_settings_from_config_reads_all_keys():
    s = review.settings_from_config(
        {
            "review_enabled": True,
            "review_provider_id": "p1",
            "review_extra_rules": "额外规则",
            "review_max_attempts": 5,
            "review_timeout_seconds": 12,
            "image_review_enabled": False,
            "image_review_max_per_batch": 3,
        }
    )

    assert s.provider_id == "p1"
    assert s.extra_rules == "额外规则"
    assert s.max_attempts == 5
    assert s.timeout_seconds == 12
    assert s.image_enabled is False
    assert s.image_max_per_batch == 3


def test_system_prompt_appends_extra_rules():
    prompt = review.build_system_prompt("禁止推送同人图")

    assert "禁止推送同人图" in prompt
    assert len(prompt) > len("禁止推送同人图")


def test_text_user_prompt_numbers_items_from_zero():
    prompt = review.build_text_user_prompt([_item("a"), _item("b")])

    assert '"i": 0' in prompt or '"i":0' in prompt
    assert '"i": 1' in prompt or '"i":1' in prompt
    assert '"title": "a"' in prompt
    assert '"title": "b"' in prompt


def test_parse_text_verdicts_plain_json():
    verdicts = review.parse_text_verdicts(
        '[{"i":0,"keep":false,"reason":"纯表情"},{"i":1,"keep":true,"reason":""}]'
    )

    assert verdicts is not None
    assert verdicts[0].keep is False
    assert verdicts[0].reason == "纯表情"
    assert verdicts[0].source == "text"
    assert verdicts[1].keep is True


def test_parse_text_verdicts_strips_code_fence_and_prose():
    raw = '好的，结果如下：\n```json\n[{"i":0,"keep":false,"reason":"广告"}]\n```\n'

    verdicts = review.parse_text_verdicts(raw)

    assert verdicts is not None
    assert verdicts[0].keep is False


def test_parse_text_verdicts_returns_none_on_garbage():
    assert review.parse_text_verdicts("模型今天不想干活") is None
    assert review.parse_text_verdicts("") is None
    assert review.parse_text_verdicts('{"i":0}') is None


def test_parse_text_verdicts_skips_malformed_entries():
    verdicts = review.parse_text_verdicts(
        '[{"i":0,"keep":false},"junk",{"keep":false},{"i":true,"keep":false}]'
    )

    assert verdicts is not None
    assert set(verdicts) == {0}


def test_backoff_grows_and_caps():
    assert review.backoff_seconds(1) == 120
    assert review.backoff_seconds(2) == 240
    assert review.backoff_seconds(3) == 480
    assert review.backoff_seconds(99) == 1800


@pytest.mark.asyncio
async def test_missing_index_defaults_to_keep():
    provider = FakeProvider(['[{"i":0,"keep":false,"reason":"水帖"}]'])

    outcome = await review.review_batch(
        provider, [_item("a"), _item("b")], _settings(), max_images=1
    )

    assert outcome.deferred is False
    assert [i["item_key"] for i in outcome.kept] == ["b"]
    assert outcome.dropped[0]["item"]["item_key"] == "a"
    assert outcome.dropped[0]["source"] == "text"


@pytest.mark.asyncio
async def test_unparseable_response_defers_whole_batch():
    provider = FakeProvider(["我拒绝回答"])

    outcome = await review.review_batch(provider, [_item("a")], _settings(), max_images=1)

    assert outcome.deferred is True
    assert outcome.kept == []
    assert outcome.dropped == []


@pytest.mark.asyncio
async def test_provider_exception_defers_whole_batch():
    provider = FakeProvider([RuntimeError("boom")])

    outcome = await review.review_batch(provider, [_item("a")], _settings(), max_images=1)

    assert outcome.deferred is True


@pytest.mark.asyncio
async def test_timeout_defers_whole_batch():
    class SlowProvider:
        async def text_chat(self, **kwargs):
            await asyncio.sleep(10)

    outcome = await review.review_batch(
        SlowProvider(), [_item("a")], _settings(timeout_seconds=0), max_images=1
    )

    assert outcome.deferred is True


@pytest.mark.asyncio
async def test_no_provider_marks_unavailable_and_keeps_all():
    outcome = await review.review_batch(None, [_item("a")], _settings(), max_images=1)

    assert outcome.unavailable is True
    assert outcome.deferred is False
    assert len(outcome.kept) == 1


@pytest.mark.asyncio
async def test_disabled_review_keeps_all_without_calling_provider():
    provider = FakeProvider(["不该被调用"])

    outcome = await review.review_batch(
        provider, [_item("a")], _settings(enabled=False), max_images=1
    )

    assert outcome.kept == [_item("a")]
    assert provider.calls == []
