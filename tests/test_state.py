from __future__ import annotations

import json

from astrbot_plugin_dracalon_feed import state as feed_state


def test_default_state_has_digest_and_review_fields():
    st = feed_state.default_state()

    assert st["schema"] == 4
    assert st["digest_buffer"] == []
    assert st["last_flush_at"] == 0
    assert st["review_attempts"] == 0
    assert st["review_retry_at"] == 0
    assert st["filtered_recent"] == []


def test_schema_three_migrates_additively(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "schema": 3,
                "watermark": 123,
                "boundary_keys": ["kept"],
                "bootstrap_done": True,
                "pending_deliveries": [
                    {"item": {"url": "u"}, "targets": ["g"], "attempts": 1}
                ],
            }
        ),
        encoding="utf-8",
    )

    st = feed_state.load_state(path)

    assert st["schema"] == 4
    assert st["watermark"] == 123
    assert st["boundary_keys"] == ["kept"]
    assert len(st["pending_deliveries"]) == 1
    assert st["digest_buffer"] == []
    assert st["last_flush_at"] == 0


def test_load_state_drops_malformed_digest_entries(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "schema": 4,
                "watermark": 1,
                "digest_buffer": [{"url": "https://a"}, "junk", {"no_url": 1}],
                "filtered_recent": [{"title": "t", "url": "u"}, 42],
            }
        ),
        encoding="utf-8",
    )

    st = feed_state.load_state(path)

    assert st["digest_buffer"] == [{"url": "https://a"}]
    assert st["filtered_recent"] == [{"title": "t", "url": "u"}]


def test_snapshot_state_round_trips_new_fields():
    st = feed_state.default_state()
    st["digest_buffer"] = [{"url": "https://a"}]
    st["last_flush_at"] = 99
    st["review_attempts"] = 2
    st["review_retry_at"] = 500
    st["filtered_recent"] = [{"title": "x", "url": "u", "reason": "r", "source": "text"}]

    snap = feed_state.snapshot_state(st)

    assert snap["digest_buffer"] == [{"url": "https://a"}]
    assert snap["last_flush_at"] == 99
    assert snap["review_attempts"] == 2
    assert snap["review_retry_at"] == 500
    assert snap["filtered_recent"][0]["reason"] == "r"
