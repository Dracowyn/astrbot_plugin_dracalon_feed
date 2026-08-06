from __future__ import annotations

from astrbot_plugin_dracalon_feed import digest
from astrbot_plugin_dracalon_feed import state as feed_state


def _settings(**kw) -> digest.DigestSettings:
    base = {"enabled": True, "interval_seconds": 1800, "max_items": 10}
    base.update(kw)
    return digest.DigestSettings(**base)


def _st(**kw) -> dict:
    st = feed_state.default_state()
    st.update(kw)
    return st


def test_settings_from_config_clamps_bounds():
    s = digest.settings_from_config(
        {"digest_interval_seconds": 5, "digest_max_items": 0}
    )

    assert s.interval_seconds == 60
    assert s.max_items == 1


def test_enqueue_appends_and_preserves_order():
    st = _st()

    digest.enqueue(st, [{"url": "a"}])
    digest.enqueue(st, [{"url": "b"}, {"url": "c"}])

    assert [i["url"] for i in st["digest_buffer"]] == ["a", "b", "c"]


def test_empty_buffer_never_flushes():
    st = _st(last_flush_at=0)

    assert digest.should_flush(st, _settings(), now=10_000, in_quiet_hours=False) is False


def test_flushes_when_interval_elapsed():
    st = _st(digest_buffer=[{"url": "a"}], last_flush_at=1_000)

    assert digest.should_flush(st, _settings(), now=2_799, in_quiet_hours=False) is False
    assert digest.should_flush(st, _settings(), now=2_800, in_quiet_hours=False) is True


def test_flushes_early_when_item_count_reached():
    st = _st(
        digest_buffer=[{"url": str(i)} for i in range(3)],
        last_flush_at=9_999,
    )

    assert (
        digest.should_flush(st, _settings(max_items=3), now=10_000, in_quiet_hours=False)
        is True
    )


def test_quiet_hours_block_flush():
    st = _st(digest_buffer=[{"url": "a"}], last_flush_at=0)

    assert digest.should_flush(st, _settings(), now=10_000, in_quiet_hours=True) is False


def test_review_backoff_blocks_flush_until_due():
    st = _st(digest_buffer=[{"url": "a"}], last_flush_at=0, review_retry_at=5_000)

    assert digest.should_flush(st, _settings(), now=4_999, in_quiet_hours=False) is False
    assert digest.should_flush(st, _settings(), now=5_000, in_quiet_hours=False) is True


def test_disabled_digest_flushes_residual_buffer_immediately():
    st = _st(digest_buffer=[{"url": "a"}], last_flush_at=10_000, review_retry_at=99_999)

    assert (
        digest.should_flush(st, _settings(enabled=False), now=10_001, in_quiet_hours=False)
        is True
    )


def test_trim_keeps_latest_and_reports_dropped():
    buffer = [{"url": str(i)} for i in range(5)]

    kept, dropped = digest.trim_for_quiet_catchup(buffer, 2)

    assert [i["url"] for i in kept] == ["3", "4"]
    assert [i["url"] for i in dropped] == ["0", "1", "2"]


def test_trim_disabled_when_max_catchup_is_zero():
    buffer = [{"url": str(i)} for i in range(5)]

    kept, dropped = digest.trim_for_quiet_catchup(buffer, 0)

    assert kept == buffer
    assert dropped == []
