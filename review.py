"""AI 内容审查：结算前把一批帖子交给 LLM 判 keep/drop。

审查只做去留判定，不做摘要、点评或排序。判定失败一律推迟到下一批重试，
连续失败达上限后整批放行 —— 模型挂了不该导致静默丢帖。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from astrbot.api import logger

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"

RETRY_BASE_SECONDS = 120
RETRY_MAX_SECONDS = 1800

BASE_RULES = """你是社区新帖推送的内容审查员。给定一批帖子，判断每条是否值得推送到群里。

判定为「不推送」的情形：
- 灌水帖、签到帖、纯占位或无实质内容
- 测试帖、调试帖、误发帖
- 纯表情、纯符号、纯颜文字刷屏
- 广告、引流、拉群、推广链接
- 标题无任何信息量，看不出在讲什么

其余一律推送。拿不准时保留，宁可多推也不要误杀。

只输出 JSON 数组，不要任何解释文字，格式：
[{"i": 编号, "keep": true 或 false, "reason": "不推送的理由，keep 为 true 时留空"}]
每条帖子都要给出一个对象。"""

IMAGE_RULES = """你是社区新帖推送的配图审查员。给定一条帖子的标题与配图，判断这条帖子是否适合推送到群里。

判定为「不推送」的情形：
- 配图为色情、血腥、猎奇或其他不适合群聊公开展示的内容
- 配图是广告、二维码、引流图
- 配图是纯截图噪声、纯黑纯白或明显的无意义占位图

其余一律推送。拿不准时保留。

只输出 JSON 对象，不要任何解释文字，格式：
{"keep": true 或 false, "reason": "不推送的理由，keep 为 true 时留空"}"""


@dataclass(frozen=True)
class ReviewSettings:
    enabled: bool
    provider_id: str
    image_provider_id: str
    extra_rules: str
    # text_prompt / image_prompt 为空表示用内置的 BASE_RULES / IMAGE_RULES。
    # 非空则整段替换内置规则，extra_rules 仍照常追加在后面。
    text_prompt: str
    image_prompt: str
    max_attempts: int
    timeout_seconds: int
    image_enabled: bool
    image_max_per_batch: int


@dataclass(frozen=True)
class Verdict:
    keep: bool
    reason: str
    source: str


@dataclass(frozen=True)
class ReviewOutcome:
    deferred: bool
    kept: list[dict[str, Any]]
    dropped: list[dict[str, Any]]
    unavailable: bool


def settings_from_config(config: Mapping[str, Any]) -> ReviewSettings:
    return ReviewSettings(
        enabled=bool(config.get("review_enabled", True)),
        provider_id=str(config.get("review_provider_id", "") or "").strip(),
        image_provider_id=str(config.get("image_review_provider_id", "") or "").strip(),
        extra_rules=str(config.get("review_extra_rules", "") or "").strip(),
        text_prompt=str(config.get("review_prompt", "") or "").strip(),
        image_prompt=str(config.get("image_review_prompt", "") or "").strip(),
        max_attempts=max(1, int(config.get("review_max_attempts", 3) or 3)),
        timeout_seconds=max(1, int(config.get("review_timeout_seconds", 30) or 30)),
        image_enabled=bool(config.get("image_review_enabled", True)),
        image_max_per_batch=max(
            0, int(config.get("image_review_max_per_batch", 10) or 0)
        ),
    )


def build_system_prompt(base_rules: str, extra_rules: str) -> str:
    """拼装 system prompt：base_rules 是内置规则或用户的整段自定义，extra_rules 追加在后。"""
    return f"{base_rules}\n\n补充规则：\n{extra_rules}" if extra_rules else base_rules


def build_text_user_prompt(items: list[dict[str, Any]]) -> str:
    payload = [
        {
            "i": idx,
            "community": str(item.get("community") or ""),
            "title": str(item.get("title") or ""),
            "author": str(item.get("author_name") or ""),
        }
        for idx, item in enumerate(items)
    ]
    return "待审查帖子：\n" + json.dumps(payload, ensure_ascii=False, indent=1)


def strip_json_fence(raw: str) -> str:
    """去掉 ``` 或 ```json 围栏，返回围栏内的文本。"""
    text = (raw or "").strip()
    if not text.startswith("```"):
        return text
    lines = text.splitlines()[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def slice_json(text: str, opener: str, closer: str) -> str:
    """截取首个 opener 到末个 closer 之间的片段，用来剥掉模型的前后废话。"""
    start, end = text.find(opener), text.rfind(closer)
    if start != -1 and end > start:
        return text[start : end + 1]
    return text


def parse_text_verdicts(raw: str) -> dict[int, Verdict] | None:
    """解析文本审查返回。整体不可解析时返回 None（触发推迟）。

    单条格式不对只跳过该条 —— 缺失的编号由调用方按 keep 处理，模型漏答不丢帖。
    """
    try:
        data = json.loads(slice_json(strip_json_fence(raw), "[", "]"))
    except (TypeError, ValueError):
        return None
    if not isinstance(data, list):
        return None
    out: dict[int, Verdict] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        idx = entry.get("i")
        if not isinstance(idx, int) or isinstance(idx, bool):
            continue
        out[idx] = Verdict(
            keep=bool(entry.get("keep", True)),
            reason=str(entry.get("reason") or ""),
            source="text",
        )
    return out


def backoff_seconds(attempts: int) -> int:
    """审查失败后的退避秒数，避免每轮都撞一次挂掉的模型。"""
    return min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * 2 ** max(0, attempts - 1))


async def _call(
    provider: Any,
    settings: ReviewSettings,
    *,
    prompt: str,
    system_prompt: str,
    image_urls: list[str] | None = None,
) -> str | None:
    """调一次 LLM，超时或异常返回 None。绝不让异常冒泡到轮询循环。"""
    try:
        resp = await asyncio.wait_for(
            provider.text_chat(
                prompt=prompt,
                system_prompt=system_prompt,
                image_urls=image_urls,
            ),
            timeout=settings.timeout_seconds,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[{PLUGIN_NAME}] review call failed: {type(e).__name__}: {e}")
        return None
    return str(getattr(resp, "completion_text", "") or "")


async def review_batch(
    text_provider: Any,
    image_provider: Any,
    items: list[dict[str, Any]],
    settings: ReviewSettings,
    *,
    max_images: int,
) -> ReviewOutcome:
    """审查一批帖子：先文本批量判去留，再对保留下来且带图的帖逐帖复审配图。

    两段各用自己的 Provider —— 文本段可以挂便宜的纯文本模型，图片段必须是多模态模型。
    """
    if not items:
        return ReviewOutcome(False, [], [], False)
    if not settings.enabled:
        return ReviewOutcome(False, list(items), [], False)
    if text_provider is None:
        return ReviewOutcome(False, list(items), [], True)

    raw = await _call(
        text_provider,
        settings,
        prompt=build_text_user_prompt(items),
        system_prompt=build_system_prompt(
            settings.text_prompt or BASE_RULES, settings.extra_rules
        ),
    )
    verdicts = parse_text_verdicts(raw) if raw is not None else None
    if verdicts is None:
        logger.warning(f"[{PLUGIN_NAME}] text review unusable, deferring batch")
        return ReviewOutcome(True, [], [], False)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        verdict = verdicts.get(idx)
        if verdict is not None and not verdict.keep:
            dropped.append({"item": item, "reason": verdict.reason, "source": "text"})
        else:
            kept.append(item)

    if not settings.image_enabled:
        return ReviewOutcome(False, kept, dropped, False)
    if image_provider is None:
        # 配了图片审查却拿不到可用模型：文本段已判完，这里跳过而不是丢帖
        logger.warning(
            f"[{PLUGIN_NAME}] no usable provider for image review, "
            "posts pushed without image review"
        )
        return ReviewOutcome(False, kept, dropped, False)
    return await _review_images(image_provider, kept, dropped, settings, max_images)


def images_for_item(item: dict[str, Any], max_images: int) -> list[str]:
    """取该帖真正会被发出去的图，取图逻辑与 render.build_chain 保持一致。"""
    if max_images <= 0:
        return []
    images = item.get("images")
    if not isinstance(images, list):
        images = []
    urls = [
        img
        for img in images
        if isinstance(img, str) and img.startswith(("http://", "https://"))
    ]
    if not urls:
        cover = item.get("cover_image")
        if isinstance(cover, str) and cover.startswith(("http://", "https://")):
            urls = [cover]
    return urls[:max_images]


def parse_image_verdict(raw: str) -> Verdict | None:
    """解析图片审查返回，不可解析时返回 None（该帖 fail-open 保留）。"""
    try:
        data = json.loads(slice_json(strip_json_fence(raw), "{", "}"))
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    return Verdict(
        keep=bool(data.get("keep", True)),
        reason=str(data.get("reason") or ""),
        source="image",
    )


async def _review_images(
    provider: Any,
    kept: list[dict[str, Any]],
    dropped: list[dict[str, Any]],
    settings: ReviewSettings,
    max_images: int,
) -> ReviewOutcome:
    """逐帖复审配图。图不过则丢整帖；单帖失败保留该帖，过半失败视为 provider 异常。"""
    system_prompt = build_system_prompt(
        settings.image_prompt or IMAGE_RULES, settings.extra_rules
    )
    survivors: list[dict[str, Any]] = []
    attempted = 0
    failures = 0
    skipped = 0
    for item in kept:
        urls = images_for_item(item, max_images)
        if not urls:
            survivors.append(item)
            continue
        if attempted >= settings.image_max_per_batch:
            skipped += 1
            survivors.append(item)
            continue
        attempted += 1
        raw = await _call(
            provider,
            settings,
            prompt=f"帖子标题：{item.get('title') or ''}",
            system_prompt=system_prompt,
            image_urls=urls,
        )
        verdict = parse_image_verdict(raw) if raw is not None else None
        if verdict is None:
            failures += 1
            survivors.append(item)  # 单帖 fail-open，一张图挂了不牵连整批
            continue
        if verdict.keep:
            survivors.append(item)
        else:
            dropped.append({"item": item, "reason": verdict.reason, "source": "image"})

    # 过半失败：判定为 provider 异常而非个别图片问题，整批推迟重来
    if attempted and failures * 2 > attempted:
        logger.warning(
            f"[{PLUGIN_NAME}] image review failed {failures}/{attempted}, deferring batch"
        )
        return ReviewOutcome(True, [], [], False)
    if skipped:
        logger.warning(
            f"[{PLUGIN_NAME}] image review budget reached, "
            f"{skipped} post(s) pushed without image review"
        )
    return ReviewOutcome(False, survivors, dropped, False)
