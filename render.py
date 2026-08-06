"""消息渲染：把 feed item 拼成 MessageChain，以及会话标识的人类可读化。"""

from __future__ import annotations

import time
from typing import Any

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import MessageChain

from . import state

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"


def friendly_umo(umo: str) -> str:
    """把 unified_msg_origin 转成人类可读，识别失败原样返回。

    示例：
        aiocqhttp:GroupMessage:123456 → QQ 群 123456
        aiocqhttp:FriendMessage:123456 → QQ 私聊 123456
        qq_official:GroupMessage:xxxx → QQ 频道 xxxx
    """
    if not umo:
        return "(未知会话)"
    parts = umo.split(":", 2)
    if len(parts) != 3:
        return umo
    platform, msg_type, session_id = parts
    if platform == "aiocqhttp":
        if msg_type == "GroupMessage":
            return f"QQ 群 {session_id}"
        if msg_type in ("FriendMessage", "PrivateMessage"):
            return f"QQ 私聊 {session_id}"
    if platform == "qq_official":
        if msg_type == "GroupMessage":
            return f"QQ 频道 {session_id}"
        if msg_type == "DirectMessage":
            return f"QQ 频道私信 {session_id}"
    return umo


def build_chain(item: dict[str, Any], *, style: str, max_images: int) -> MessageChain:
    """把一条帖子拼成消息链。

    Args:
        item: feed 条目。
        style: 消息风格，rich 时附带作者/发布时间/互动数据。
        max_images: 单帖最多附几张图。
    """
    chain: list = []
    community = str(item.get("community") or "社区")
    title = str(item.get("title") or "(无标题)")
    chain.append(Comp.Plain(f"【{community}】{title}\n"))

    if style == "rich":
        author = item.get("author_name")
        if author:
            chain.append(Comp.Plain(f"作者：{author}\n"))
        published_ts = state.item_ts(item)
        if published_ts:
            published = time.strftime("%Y-%m-%d %H:%M", time.localtime(published_ts))
            chain.append(Comp.Plain(f"发布：{published}\n"))

    images = item.get("images") or []
    if not isinstance(images, list):
        images = []
    if not images and item.get("cover_image"):
        images = [item["cover_image"]]
    for img in images[:max_images]:
        if isinstance(img, str) and img.startswith(("http://", "https://")):
            try:
                chain.append(Comp.Image.fromURL(img))
            except Exception as e:
                logger.warning(f"[{PLUGIN_NAME}] image url invalid {img}: {e}")

    if style == "rich":
        stats: list[str] = []
        for key, label in (
            ("reply_count", "回复"),
            ("like_count", "点赞"),
            ("view_count", "浏览"),
        ):
            v = item.get(key)
            if isinstance(v, (int, float)) and v > 0:
                stats.append(f"{label} {int(v)}")
        if stats:
            chain.append(Comp.Plain("\n" + " · ".join(stats)))

    url = item.get("url")
    if url:
        chain.append(Comp.Plain(f"\n{url}"))

    return MessageChain(chain=chain)


def build_merged_chain(
    items: list[dict[str, Any]], *, style: str, max_images: int
) -> MessageChain:
    """把多条帖子拼成一条 QQ 合并转发消息。"""
    nodes = [
        Comp.Node(
            content=build_chain(item, style=style, max_images=max_images).chain,
            name="Dracalon 新帖",
            uin="0",
        )
        for item in items
    ]
    return MessageChain(chain=[Comp.Nodes(nodes)])
