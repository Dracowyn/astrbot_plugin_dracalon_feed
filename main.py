from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"
USER_AGENT = "Dracalon-AstrBot-Feed/0.2"
INTER_TARGET_DELAY = 0.3
INTER_ITEM_DELAY = 0.5

STATE_SCHEMA = 2
# 单页拉取条数（后端 FeedService::PAGE_SIZE_MAX = 50，取满）
FETCH_PAGE_SIZE = 50
# 深翻页页数上限：覆盖停机后的积压补推；超出说明积压超 PAGE_SIZE*MAX_PAGES 条，
# 更老的新帖会被跳过（水位线直接跳到最新），仅在极端长时间停机时发生，会打日志。
MAX_FETCH_PAGES = 5


def _url_hash(url: str) -> str:
    """本地 URL 哈希。仅作 item_key 回退（后端旧版本未输出 item_key 时）使用。"""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _friendly_umo(umo: str) -> str:
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


def _default_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        # watermark：已推帖的最大 published_at（epoch 秒，UTC）。
        # boundary_keys：published_at 恰好 == watermark 且已处理（推送/标记）过的 item_key。
        # 二者一起表达「已推进度」：published_at 严格大于 watermark 的一定是新帖；
        # 恰好等于 watermark 的，靠 item_key 是否在 boundary 里区分（防同秒帖漏推/重推）。
        "watermark": 0,
        "boundary_keys": [],
        "last_poll_at": 0,
        "last_error": "",
        "bootstrap_done": False,
        "was_quiet": False,
    }


class DracalonFeedPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config

        self._session: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

        state_dir = StarTools.get_data_dir(PLUGIN_NAME)
        self._state_path = state_dir / "state.json"
        self._state = self._load_state()
        # 旧版本（基于 pushed_urls 去重）升级而来且此前已运行过时，下一轮 bootstrap
        # 强制走 mark_all：把当前 feed 全部对齐为已读、零重推。非持久化、用一次即弃。
        self._migrate_silent = bool(self._state.pop("_needs_silent_migration", False))

    # ------------------------------------------------------------------
    # 命令组（必须在 class 内定义；子命令通过 @<group_method>.command 注册）
    # ------------------------------------------------------------------
    @filter.command_group("dracalon_feed")
    def dracalon_feed(self):
        """Dracalon 新帖订阅"""
        pass

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("bind")
    async def bind(self, event: AstrMessageEvent):
        """把当前群加入新帖推送列表"""
        umo = event.unified_msg_origin
        friendly = _friendly_umo(umo)
        targets = list(self.config.get("targets", []) or [])
        if umo in targets:
            yield event.plain_result(f"当前 {friendly} 已经在推送列表里啦~")
            return
        self.config["targets"] = targets + [umo]
        self.config.save_config()
        yield event.plain_result(
            f"已绑定到 {friendly}，之后新帖会自动推送到这里。"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("unbind")
    async def unbind(self, event: AstrMessageEvent):
        """把当前群从推送列表移除"""
        umo = event.unified_msg_origin
        friendly = _friendly_umo(umo)
        targets = list(self.config.get("targets", []) or [])
        if umo not in targets:
            yield event.plain_result(f"当前 {friendly} 还未绑定推送")
            return
        targets.remove(umo)
        self.config["targets"] = targets
        self.config.save_config()
        yield event.plain_result(f"已取消 {friendly} 的推送绑定")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("list")
    async def list_targets(self, event: AstrMessageEvent):
        """查看已绑定的推送目标"""
        targets = list(self.config.get("targets", []) or [])
        if not targets:
            yield event.plain_result(
                "当前还没有绑定任何推送目标。\n"
                "在想推送到的群里发 /dracalon_feed bind 即可绑定。"
            )
            return
        lines = [f"共 {len(targets)} 个推送目标："]
        lines.extend(
            f"  {idx}. {_friendly_umo(t)}" for idx, t in enumerate(targets, 1)
        )
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("status")
    async def status(self, event: AstrMessageEvent):
        """查看推送系统运行状态"""
        last_poll_at = int(self._state.get("last_poll_at", 0) or 0)
        last_poll_str = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_poll_at))
            if last_poll_at
            else "尚未轮询过"
        )
        enabled = bool(self.config.get("enabled", True))
        targets = self.config.get("targets", []) or []
        watermark = int(self._state.get("watermark", 0) or 0)
        watermark_str = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(watermark))
            if watermark
            else "尚未推送"
        )
        last_error = self._state.get("last_error") or "无"
        bootstrap_done = bool(self._state.get("bootstrap_done", False))

        if self.config.get("quiet_hours_enabled", False):
            qs = int(self.config.get("quiet_hours_start", 0) or 0) % 24
            qe = int(self.config.get("quiet_hours_end", 0) or 0) % 24
            in_quiet = self._in_quiet_hours(time.localtime())
            quiet_desc = f"{qs:02d}:00–{qe:02d}:00（{'静默中' if in_quiet else '非静默'}）"
        else:
            quiet_desc = "未启用"

        lines = [
            "Dracalon 新帖订阅 · 当前状态",
            f"  推送开关：{'已启用' if enabled else '已暂停'}",
            f"  夜间静默：{quiet_desc}",
            f"  上次轮询：{last_poll_str}",
            f"  上次错误：{last_error}",
            f"  推送水位线：{watermark_str}（早于此发布时间的帖子视为已推）",
            f"  绑定目标数：{len(targets)}",
            f"  首次启动初始化：{'已完成' if bootstrap_done else '尚未完成'}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("test")
    async def test(self, event: AstrMessageEvent):
        """立即推一条最新帖到当前群（测试用）"""
        if self._session is None or self._session.closed:
            yield event.plain_result("系统还在启动中，请等几秒后再试")
            return
        page = await self._fetch_page(1, FETCH_PAGE_SIZE)
        if page is None:
            yield event.plain_result(
                f"拉取失败：{self._state.get('last_error') or '未知错误'}"
            )
            return
        items, _total = page
        if not items:
            yield event.plain_result("已成功访问后端，但当前没有可推送的帖子")
            return
        # 后端 sort=latest 已按发布时间倒序，items[0] 即最新
        chain = self._build_chain(items[0])
        try:
            await self.context.send_message(event.unified_msg_origin, chain)
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] test send failed: {e}")
            yield event.plain_result(f"发送失败：{e}")
            return
        yield event.plain_result("已推送 1 条测试帖")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("pause")
    async def pause(self, event: AstrMessageEvent):
        """暂停自动推送（不影响命令）"""
        self.config["enabled"] = False
        self.config.save_config()
        yield event.plain_result("已暂停推送")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("resume")
    async def resume(self, event: AstrMessageEvent):
        """恢复自动推送"""
        self.config["enabled"] = True
        self.config.save_config()
        yield event.plain_result("已恢复推送")

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        """官方生命周期入口：此时 db/platform 已 ready，建 session 并起轮询。"""
        timeout = aiohttp.ClientTimeout(
            total=int(self.config.get("request_timeout_seconds", 15) or 15)
        )
        self._session = aiohttp.ClientSession(
            headers={
                "User-Agent": USER_AGENT,
                "Server": "true",
            },
            timeout=timeout,
        )
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(f"[{PLUGIN_NAME}] initialized, polling started")

    async def terminate(self) -> None:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"[{PLUGIN_NAME}] task cleanup error: {e}")
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning(f"[{PLUGIN_NAME}] session close error: {e}")
        # 置空：避免 terminate 后仍在运行的命令拿到已关闭的 session 报误导性错误
        self._session = None

    async def _poll_loop(self) -> None:
        try:
            while True:
                interval = max(
                    30, int(self.config.get("poll_interval_seconds", 120) or 120)
                )
                if self.config.get("enabled", True):
                    try:
                        await self._poll_once()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning(f"[{PLUGIN_NAME}] poll failed: {e}")
                        async with self._lock:
                            self._state["last_error"] = f"{type(e).__name__}: {e}"
                            # 已退出静默却轮询失败时清掉 was_quiet：避免遗留标志让后续正常
                            # 轮询误用「退出静默补推」逻辑而丢弃积压（catch-up 仅应在真正
                            # 刚退出静默时生效）
                            if not self._in_quiet_hours(time.localtime()):
                                self._state["was_quiet"] = False
                        await self._save_state()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return

    def _in_quiet_hours(self, now_struct: time.struct_time) -> bool:
        """当前是否处于夜间静默时段（本地时间，小时粒度，支持跨午夜）。"""
        if not self.config.get("quiet_hours_enabled", False):
            return False
        start = int(self.config.get("quiet_hours_start", 0) or 0) % 24
        end = int(self.config.get("quiet_hours_end", 0) or 0) % 24
        if start == end:
            return False  # 空区间，视为不静默
        hour = now_struct.tm_hour
        if start < end:
            return start <= hour < end
        # 跨午夜：如 23 → 7
        return hour >= start or hour < end

    # ------------------------------------------------------------------
    # 轮询主流程
    # ------------------------------------------------------------------
    async def _poll_once(self) -> None:
        page = await self._fetch_page(1, FETCH_PAGE_SIZE)
        if page is None:
            return  # fetch 已记录 last_error
        items, total = page
        self._state.setdefault("boundary_keys", [])

        # 首次启动 / 旧版本迁移：设定水位线基线
        if not self._state.get("bootstrap_done"):
            async with self._lock:
                self._apply_bootstrap(items)
            await self._save_state()

        watermark = int(self._state.get("watermark", 0) or 0)

        # 深翻页：page1 是最新一页。若其最旧一条仍 > 水位线且还有更多页，继续往后翻，
        # 直到某页最旧条目 <= 水位线（追平积压）或到上限。覆盖停机一段时间后的补推。
        collected = await self._collect_backlog(items, total, watermark)

        new_items = self._select_new(collected, watermark)
        # 升序推送（旧→新），保证水位线单调推进、同秒帖按 item_key 稳定排序
        new_items.sort(key=lambda it: (self._ts(it) or 0, str(self._item_key(it) or "")))

        # 夜间静默：窗口内不发送、不推进水位线（积压留到窗口结束自然补推，不丢帖）
        if self._in_quiet_hours(time.localtime()):
            async with self._lock:
                self._state["was_quiet"] = True
                self._state["last_poll_at"] = int(time.time())
                self._state["last_error"] = ""
            await self._save_state()
            return

        # 刚退出静默且积压超上限：只推最新 N 条，其余靠推进水位线标记已读（防早高峰刷屏）
        dropped: list[dict[str, Any]] = []
        max_catchup = int(self.config.get("quiet_hours_max_catchup", 5) or 0)
        if (
            bool(self._state.get("was_quiet", False))
            and max_catchup > 0
            and len(new_items) > max_catchup
        ):
            dropped = new_items[:-max_catchup]
            new_items = new_items[-max_catchup:]
            logger.info(
                f"[{PLUGIN_NAME}] quiet-hours catch-up: push latest {len(new_items)}, "
                f"skip {len(dropped)} backlog"
            )

        targets = list(self.config.get("targets", []) or [])
        for item in new_items:
            chain = self._build_chain(item)
            for umo in targets:
                try:
                    await self.context.send_message(umo, chain)
                except Exception as e:
                    logger.error(
                        f"[{PLUGIN_NAME}] send_message to {umo} failed: {e}"
                    )
                await asyncio.sleep(INTER_TARGET_DELAY)
            async with self._lock:
                self._advance(item)
            await self._save_state()
            await asyncio.sleep(INTER_ITEM_DELAY)

        # 被丢弃的积压（比已推条目更旧）并入水位线标记已读，防止下轮重新当新帖
        if dropped:
            async with self._lock:
                for item in dropped:
                    self._advance(item)

        async with self._lock:
            self._state["was_quiet"] = False
            self._state["last_poll_at"] = int(time.time())
            self._state["last_error"] = ""
        await self._save_state()

    async def _collect_backlog(
        self, first_page: list[dict[str, Any]], total: int, watermark: int
    ) -> list[dict[str, Any]]:
        """从 page1 起按需深翻页，收齐所有 published_at >= 水位线的条目（newest-first）。

        停止条件用 >= 而非 >：同一秒的帖可能跨页（page 末尾与下页开头同为水位线那一秒），
        必须把整秒翻完，否则下页同秒未推的帖会被漏掉。oldest 为 None 表示该页末尾已是
        无发布时间的沉底条目（列表尾），继续翻无意义，停止。
        """
        collected = list(first_page)
        if not first_page or len(first_page) >= total:
            return collected
        oldest = self._oldest_ts(first_page)
        page = 2
        while (
            oldest is not None
            and oldest >= watermark
            and page <= MAX_FETCH_PAGES
            and len(collected) < total
        ):
            res = await self._fetch_page(page, FETCH_PAGE_SIZE)
            if res is None:
                break  # 后续页拉取失败：用已收集的部分，下轮再补
            more, total = res
            if not more:
                break
            collected.extend(more)
            oldest = self._oldest_ts(more)
            page += 1
        if oldest is not None and oldest >= watermark and len(collected) < total:
            logger.warning(
                f"[{PLUGIN_NAME}] backlog exceeds {MAX_FETCH_PAGES} pages "
                f"({FETCH_PAGE_SIZE * MAX_FETCH_PAGES} posts); older new posts skipped"
            )
        return collected

    # ------------------------------------------------------------------
    # 水位线判定 / 推进
    # ------------------------------------------------------------------
    @staticmethod
    def _oldest_ts(items: list[dict[str, Any]]) -> int | None:
        """页内自末尾起第一条「有发布时间」条目的 ts。

        无发布时间的条目在后端 latest 排序里沉底，深翻页判断「是否已翻过水位线」时
        必须跳过它们，否则末条恰为无时间帖会让翻页提前停在 None 上。
        """
        for it in reversed(items):
            ts = DracalonFeedPlugin._ts(it)
            if ts is not None:
                return ts
        return None

    @staticmethod
    def _ts(item: dict[str, Any]) -> int | None:
        """把 item.published_at（后端 gmdate('c') → ISO8601）解析成 epoch 秒。

        无发布时间或无法解析时返回 None —— 这类条目不参与水位线判定，也不会被推送。
        """
        raw = item.get("published_at")
        if not raw:
            return None
        # 防御：后端契约是 ISO8601 字符串，但万一改成数字 epoch 也能正确解析（而非静默丢帖）
        if isinstance(raw, bool):
            return None
        if isinstance(raw, (int, float)):
            ts = int(raw)
            return ts if ts > 0 else None
        s = str(raw).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    @staticmethod
    def _item_key(item: dict[str, Any]) -> str | None:
        """稳定去重键。优先用后端下发的 item_key = sha256(normalize(url))；
        后端旧版本未输出时回退到本地 raw-url 哈希（仅用于同秒 tie-break，可接受）。
        """
        k = item.get("item_key")
        if isinstance(k, str) and k:
            return k
        url = item.get("url")
        if url:
            return _url_hash(str(url))
        return None

    def _select_new(
        self, items: list[dict[str, Any]], watermark: int
    ) -> list[dict[str, Any]]:
        """挑出尚未推送过的新帖：published_at 严格大于水位线，或恰好等于水位线
        但 item_key 不在 boundary（同秒未推过的）。"""
        boundary = set(self._state.get("boundary_keys", []) or [])
        out: list[dict[str, Any]] = []
        for it in items:
            ts = self._ts(it)
            if ts is None:
                continue
            key = self._item_key(it)
            if not key:
                continue
            if ts > watermark or (ts == watermark and key not in boundary):
                out.append(it)
        return out

    def _advance(self, item: dict[str, Any]) -> None:
        """把一条已处理（推送或标记已读）的帖子并入水位线。

        必须按 published_at 升序调用，水位线才单调推进。同秒帖累积进 boundary，
        水位线一旦前进 boundary 就清空换新 —— 因此 boundary 至多保留同一秒的条目数，
        不会无限增长，无需额外清理（这正是替代旧 pushed_urls + 按天数 prune 的关键）。
        """
        ts = self._ts(item) or 0
        key = self._item_key(item)
        if not key:
            return
        wm = int(self._state.get("watermark", 0) or 0)
        if ts > wm:
            self._state["watermark"] = ts
            self._state["boundary_keys"] = [key]
        elif ts == wm:
            boundary = self._state.setdefault("boundary_keys", [])
            if key not in boundary:
                boundary.append(key)
        # ts < wm：更旧的条目，已在水位线之下，忽略

    def _apply_bootstrap(self, items: list[dict[str, Any]]) -> None:
        """首次启动设定水位线基线（不直接推送，推送交给统一流程）。

        - push_all：水位线归零，统一流程随后把当前全部帖子推一遍（慎用）。
        - mark_all：水位线设到最新帖、boundary 含最新秒全部 key → 当前帖全标记已读、0 推送。
        - latest_one（默认）：水位线设到最新帖，但 boundary 排除「最新那一条」的 key
          → 统一流程恰好推这 1 条；其余（含同秒的）都在水位线下或 boundary 内被跳过。
        迁移场景（_migrate_silent）强制 mark_all：旧版本升级且此前已运行过时零重推。
        """
        mode = "mark_all" if self._migrate_silent else str(
            self.config.get("bootstrap_mode", "latest_one")
        )
        self._migrate_silent = False
        self._state["bootstrap_done"] = True

        if mode == "push_all":
            self._state["watermark"] = 0
            self._state["boundary_keys"] = []
            return

        dated = sorted(
            (ts, k)
            for ts, k in ((self._ts(it), self._item_key(it)) for it in items)
            if ts is not None and k
        )
        if not dated:
            self._state["watermark"] = 0
            self._state["boundary_keys"] = []
            return

        newest_ts, newest_key = dated[-1]
        keys_at_newest = [k for ts, k in dated if ts == newest_ts]
        self._state["watermark"] = newest_ts
        if mode == "mark_all":
            self._state["boundary_keys"] = keys_at_newest
        else:  # latest_one：放过最新 1 条
            self._state["boundary_keys"] = [
                k for k in keys_at_newest if k != newest_key
            ]

    # ------------------------------------------------------------------
    # 后端拉取
    # ------------------------------------------------------------------
    async def _fetch_page(
        self, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int] | None:
        """拉取一页 feed（sort=latest，按发布时间倒序）。返回 (items, total)，失败返回 None。"""
        if self._session is None or self._session.closed:
            return None
        api_base = str(self.config.get("api_base", "")).rstrip("/")
        feed_key = str(self.config.get("feed_key", "community_hot"))
        if not api_base:
            async with self._lock:
                self._state["last_error"] = "api_base 未配置"
            return None
        url = f"{api_base}/api/homepage/feed"
        params = {
            "key": feed_key,
            "page": page,
            "page_size": page_size,
            "sort": "latest",
        }
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    msg = f"HTTP {resp.status}"
                    logger.warning(f"[{PLUGIN_NAME}] {msg}")
                    async with self._lock:
                        self._state["last_error"] = msg
                    return None
                payload = await resp.json(content_type=None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[{PLUGIN_NAME}] fetch error: {e}")
            async with self._lock:
                self._state["last_error"] = f"{type(e).__name__}: {e}"
            return None

        if not isinstance(payload, dict) or payload.get("code") != 1:
            err = f"code={payload.get('code') if isinstance(payload, dict) else 'N/A'} msg={payload.get('msg', '') if isinstance(payload, dict) else ''}"
            logger.warning(f"[{PLUGIN_NAME}] api {err}")
            async with self._lock:
                self._state["last_error"] = err
            return None

        data = payload.get("data") or {}
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []
        items = [it for it in items if isinstance(it, dict) and it.get("url")]
        total = int(data.get("total") or len(items))
        return items, total

    # ------------------------------------------------------------------
    # MessageChain 构造
    # ------------------------------------------------------------------
    def _build_chain(self, item: dict[str, Any]) -> MessageChain:
        style = str(self.config.get("message_style", "rich"))
        max_images = max(0, int(self.config.get("max_images_per_post", 3) or 0))

        chain: list = []
        community = str(item.get("community") or "社区")
        title = str(item.get("title") or "(无标题)")
        chain.append(Comp.Plain(f"【{community}】{title}\n"))

        if style == "rich":
            author = item.get("author_name")
            if author:
                chain.append(Comp.Plain(f"作者：{author}\n"))

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

    # ------------------------------------------------------------------
    # state.json
    # ------------------------------------------------------------------
    def _load_state(self) -> dict[str, Any]:
        if not self._state_path.exists():
            return _default_state()
        try:
            raw = self._state_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("state.json is not an object")
        except Exception as e:
            logger.error(
                f"[{PLUGIN_NAME}] state.json corrupted ({e}), reset to default"
            )
            return _default_state()

        merged = _default_state()
        old_schema = int(data.get("schema", 1) or 1)
        if old_schema >= STATE_SCHEMA and "watermark" in data:
            merged.update(data)
            merged["boundary_keys"] = list(merged.get("boundary_keys") or [])
            merged["watermark"] = int(merged.get("watermark", 0) or 0)
            return merged

        # 旧版本（schema 1：基于 pushed_urls + 按天数 prune 去重）→ 迁移到 watermark。
        # 丢弃 pushed_urls；若此前已 bootstrap 过（在线上跑过），标记下轮静默 mark_all、
        # 把当前 feed 全部对齐已读，升级零重推。仅缺时间戳的极少数帖会在迁移窗口被跳过。
        had_run = bool(data.get("bootstrap_done")) or bool(data.get("pushed_urls"))
        merged.update(
            {
                k: v
                for k, v in data.items()
                if k in ("last_poll_at", "last_error", "was_quiet")
            }
        )
        merged["schema"] = STATE_SCHEMA
        merged["watermark"] = 0
        merged["boundary_keys"] = []
        merged["bootstrap_done"] = False
        merged["_needs_silent_migration"] = had_run
        logger.info(
            f"[{PLUGIN_NAME}] migrated state schema {old_schema} → {STATE_SCHEMA}"
            f"{'（静默 mark_all 对齐，零重推）' if had_run else ''}"
        )
        return merged

    async def _save_state(self) -> None:
        async with self._lock:
            snapshot = {
                "schema": STATE_SCHEMA,
                "watermark": int(self._state.get("watermark", 0) or 0),
                "boundary_keys": list(self._state.get("boundary_keys", []) or []),
                "last_poll_at": int(self._state.get("last_poll_at", 0) or 0),
                "last_error": str(self._state.get("last_error", "") or ""),
                "bootstrap_done": bool(self._state.get("bootstrap_done", False)),
                "was_quiet": bool(self._state.get("was_quiet", False)),
            }
        try:
            # 文件 IO 是同步阻塞调用，丢到线程池避免阻塞 event loop
            await asyncio.to_thread(self._write_state_file, snapshot)
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] save state failed: {e}")

    def _write_state_file(self, snapshot: dict[str, Any]) -> None:
        tmp_path = self._state_path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self._state_path)
