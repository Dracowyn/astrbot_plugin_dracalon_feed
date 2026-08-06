from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

# 消息组件在 render 模块里构造；此处保留导入，供类型判定与测试断言引用
import astrbot.api.message_components as Comp  # noqa: F401
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools

from . import digest, render, state
from .state import item_key, item_ts

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"
USER_AGENT = "Dracalon-AstrBot-Feed/0.4"
INTER_TARGET_DELAY = 0.3
INTER_ITEM_DELAY = 0.5
RETRY_BASE_SECONDS = 120
RETRY_MAX_SECONDS = 3600

# 单页拉取条数（后端 FeedService::PAGE_SIZE_MAX = 50，取满）
FETCH_PAGE_SIZE = 50
# 深翻页页数上限：覆盖停机后的积压补推；超出说明积压超 PAGE_SIZE*MAX_PAGES 条，
# 更老的新帖会被跳过（水位线直接跳到最新），仅在极端长时间停机时发生，会打日志。
MAX_FETCH_PAGES = 5


class DracalonFeedPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config

        self._session: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

        state_dir = StarTools.get_data_dir(PLUGIN_NAME)
        self._state_path = state_dir / "state.json"
        self._state = state.load_state(self._state_path)
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
        friendly = render.friendly_umo(umo)
        targets = list(self.config.get("targets", []) or [])
        if umo in targets:
            yield event.plain_result(f"当前 {friendly} 已经在推送列表里啦~")
            return
        self.config["targets"] = targets + [umo]
        self.config.save_config()
        yield event.plain_result(f"已绑定到 {friendly}，之后新帖会自动推送到这里。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("unbind")
    async def unbind(self, event: AstrMessageEvent):
        """把当前群从推送列表移除"""
        umo = event.unified_msg_origin
        friendly = render.friendly_umo(umo)
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
        lines.extend(f"  {idx}. {render.friendly_umo(t)}" for idx, t in enumerate(targets, 1))
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
        pending_count = sum(
            len(entry.get("targets", []))
            for entry in self._state.get("pending_deliveries", [])
            if isinstance(entry, dict)
        )

        if self.config.get("quiet_hours_enabled", False):
            qs = int(self.config.get("quiet_hours_start", 0) or 0) % 24
            qe = int(self.config.get("quiet_hours_end", 0) or 0) % 24
            in_quiet = self._in_quiet_hours(time.localtime())
            quiet_desc = (
                f"{qs:02d}:00–{qe:02d}:00（{'静默中' if in_quiet else '非静默'}）"
            )
        else:
            quiet_desc = "未启用"

        if self.config.get("merge_push_enabled", True):
            merge_threshold = max(
                2, int(self.config.get("merge_push_threshold", 2) or 2)
            )
            merge_batch_size = max(
                merge_threshold,
                min(20, int(self.config.get("merge_push_batch_size", 5) or 5)),
            )
            merge_desc = (
                f"已启用（{merge_threshold} 条起，每批最多 {merge_batch_size} 条）"
            )
        else:
            merge_desc = "未启用"

        lines = [
            "Dracalon 新帖订阅 · 当前状态",
            f"  推送开关：{'已启用' if enabled else '已暂停'}",
            f"  夜间静默：{quiet_desc}",
            f"  合并推送：{merge_desc}",
            f"  上次轮询：{last_poll_str}",
            f"  上次错误：{last_error}",
            f"  推送水位线：{watermark_str}（早于此发布时间的帖子视为已推）",
            f"  绑定目标数：{len(targets)}",
            f"  待重试投递数：{pending_count}",
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
        chain = render.build_chain(
            items[0], style=self._style(), max_images=self._max_images()
        )
        failed, error = await self._deliver_to_targets(
            items[0], [event.unified_msg_origin], chain=chain
        )
        if failed:
            yield event.plain_result(f"发送失败：{error or '平台未接受消息'}")
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
                        await self._save_state()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return

    def _style(self) -> str:
        return str(self.config.get("message_style", "rich"))

    def _max_images(self) -> int:
        return max(0, int(self.config.get("max_images_per_post", 1) or 0))

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

    async def _deliver_to_targets(
        self,
        item: dict[str, Any],
        targets: list[str],
        chain: MessageChain | None = None,
    ) -> tuple[list[str], str]:
        """Deliver one feed item and report targets that need retrying.

        Args:
            item: Feed item used to build the message and identify log entries.
            targets: Unified message origins that should receive the item.
            chain: Optional prebuilt message chain.

        Returns:
            A tuple containing failed targets and a concise error description.
        """
        message = chain or render.build_chain(
            item, style=self._style(), max_images=self._max_images()
        )
        failed: list[str] = []
        errors: list[str] = []
        for umo in dict.fromkeys(targets):
            try:
                sent = await self.context.send_message(umo, message)
                if not sent:
                    raise RuntimeError("no matching platform")
            except Exception as e:
                failed.append(umo)
                errors.append(f"{umo}: {type(e).__name__}: {e}")
                logger.error(
                    f"[{PLUGIN_NAME}] delivery to {umo} failed "
                    f"for item {item_key(item) or '(unknown)'}: {e}"
                )
            await asyncio.sleep(INTER_TARGET_DELAY)
        return failed, "; ".join(errors[-3:])

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
        if collected is None:
            return  # Abort the poll so an incomplete backlog cannot advance state.

        new_items = self._select_new(collected, watermark)
        # 升序推送（旧→新），保证水位线单调推进、同秒帖按 item_key 稳定排序
        new_items.sort(key=lambda it: (item_ts(it) or 0, str(item_key(it) or "")))
        in_quiet = self._in_quiet_hours(time.localtime())

        targets = list(dict.fromkeys(self.config.get("targets", []) or []))
        delivery_error = (
            str(self._state.get("last_error", "") or "")
            if self._state.get("pending_deliveries")
            else ""
        )

        # Retry failed targets first without resending to targets that succeeded.
        pending: list[dict[str, Any]] = []
        configured_targets = set(targets)
        now = int(time.time())
        if in_quiet:
            # 静默期一律不发消息，重试队列原样留到窗口结束
            pending = list(self._state.get("pending_deliveries", []) or [])
        else:
            for entry in list(self._state.get("pending_deliveries", []) or []):
                if not isinstance(entry, dict) or not isinstance(
                    entry.get("item"), dict
                ):
                    continue
                retry_targets = [
                    umo
                    for umo in entry.get("targets", [])
                    if isinstance(umo, str) and umo in configured_targets
                ]
                if not retry_targets:
                    continue
                if int(entry.get("next_retry_at", 0) or 0) > now:
                    pending.append({**entry, "targets": retry_targets})
                    continue
                failed, error = await self._deliver_to_targets(
                    entry["item"], retry_targets
                )
                if failed:
                    attempts = int(entry.get("attempts", 0) or 0) + 1
                    pending.append(
                        {
                            "item": entry["item"],
                            "targets": failed,
                            "attempts": attempts,
                            "next_retry_at": now
                            + min(
                                RETRY_MAX_SECONDS,
                                RETRY_BASE_SECONDS * 2 ** min(attempts - 1, 5),
                            ),
                        }
                    )
                    delivery_error = error or delivery_error
                await asyncio.sleep(INTER_ITEM_DELAY)

        async with self._lock:
            self._state["pending_deliveries"] = pending
        await self._save_state()

        digest_settings = digest.settings_from_config(self.config)

        # 收录：新帖入缓冲区的同时推进水位线。二者在同一把锁、同一次落盘里写入，
        # 因此不存在「水位线推进了但帖子丢了」的撕裂窗口。
        async with self._lock:
            digest.enqueue(self._state, new_items)
            for item in new_items:
                state.advance_watermark(self._state, item)
            if in_quiet:
                self._state["was_quiet"] = True
            self._state["last_poll_at"] = int(time.time())
        await self._save_state()

        now = int(time.time())
        if not digest.should_flush(
            self._state, digest_settings, now=now, in_quiet_hours=in_quiet
        ):
            async with self._lock:
                if not self._state.get("pending_deliveries"):
                    delivery_error = ""
                self._state["last_error"] = delivery_error
            await self._save_state()
            return

        await self._flush_digest(targets)

        async with self._lock:
            if not self._state.get("pending_deliveries"):
                delivery_error = ""
            elif delivery_error:
                self._state["last_error"] = delivery_error
        await self._save_state()

    async def _flush_digest(self, targets: list[str]) -> tuple[int, int, bool]:
        """结算缓冲区：取出全部待推帖子，投递后清空。

        Args:
            targets: 已去重的推送目标列表。

        Returns:
            (已投递帖数, 被审查丢弃帖数, 是否推迟到下轮)。本阶段审查尚未接入，
            丢弃数恒为 0、推迟恒为 False；后续任务会在此处插入审查。
        """
        async with self._lock:
            buffer = list(self._state.get("digest_buffer") or [])
            was_quiet = bool(self._state.get("was_quiet", False))
        if not buffer:
            return 0, 0, False

        # 刚退出静默且积压超上限：只推最新 N 条，其余直接丢（水位线已在收录时推进）
        if was_quiet:
            max_catchup = int(self.config.get("quiet_hours_max_catchup", 5) or 0)
            buffer, backlog_dropped = digest.trim_for_quiet_catchup(buffer, max_catchup)
            if backlog_dropped:
                logger.info(
                    f"[{PLUGIN_NAME}] quiet-hours catch-up: push latest {len(buffer)}, "
                    f"skip {len(backlog_dropped)} backlog"
                )

        await self._deliver_batch(buffer, targets)

        async with self._lock:
            self._state["digest_buffer"] = []
            self._state["last_flush_at"] = int(time.time())
            self._state["was_quiet"] = False
        await self._save_state()
        return len(buffer), 0, False

    async def _deliver_batch(
        self, items: list[dict[str, Any]], targets: list[str]
    ) -> None:
        """按合并推送配置把一批帖子切片投递，失败的目标进重试队列。

        水位线已在收录阶段推进，这里不再推进 —— 重复推进会跳过尚未投递的帖。
        """
        merge_enabled = bool(self.config.get("merge_push_enabled", True))
        merge_threshold = max(2, int(self.config.get("merge_push_threshold", 2) or 2))
        merge_batch_size = max(
            merge_threshold,
            min(20, int(self.config.get("merge_push_batch_size", 5) or 5)),
        )
        if merge_enabled and len(items) >= merge_threshold:
            batches = [
                items[offset : offset + merge_batch_size]
                for offset in range(0, len(items), merge_batch_size)
            ]
        else:
            batches = [[item] for item in items]

        for batch in batches:
            chain = (
                render.build_merged_chain(
                    batch, style=self._style(), max_images=self._max_images()
                )
                if len(batch) >= merge_threshold
                else render.build_chain(
                    batch[0], style=self._style(), max_images=self._max_images()
                )
            )
            failed, error = await self._deliver_to_targets(
                batch[-1], targets, chain=chain
            )
            if failed:
                async with self._lock:
                    for item in batch:
                        self._state.setdefault("pending_deliveries", []).append(
                            {
                                "item": item,
                                "targets": failed,
                                "attempts": 1,
                                "next_retry_at": int(time.time()) + RETRY_BASE_SECONDS,
                            }
                        )
                    self._state["last_error"] = error or self._state.get(
                        "last_error", ""
                    )
                await self._save_state()
            await asyncio.sleep(INTER_ITEM_DELAY)

    async def _collect_backlog(
        self, first_page: list[dict[str, Any]], total: int, watermark: int
    ) -> list[dict[str, Any]] | None:
        """从 page1 起按需深翻页，收齐所有 published_at >= 水位线的条目（newest-first）。

        停止条件用 >= 而非 >：同一秒的帖可能跨页（page 末尾与下页开头同为水位线那一秒），
        必须把整秒翻完，否则下页同秒未推的帖会被漏掉。oldest 为 None 表示该页末尾已是
        无发布时间的沉底条目（列表尾），继续翻无意义，停止。
        """
        collected = list(first_page)
        if not first_page or len(first_page) >= total:
            return collected
        oldest = state.oldest_ts(first_page)
        page = 2
        while (
            oldest is not None
            and oldest >= watermark
            and page <= MAX_FETCH_PAGES
            and len(collected) < total
        ):
            res = await self._fetch_page(page, FETCH_PAGE_SIZE)
            if res is None:
                return None
            more, total = res
            if not more:
                break
            collected.extend(more)
            oldest = state.oldest_ts(more)
            page += 1
        if oldest is not None and oldest >= watermark and len(collected) < total:
            logger.warning(
                f"[{PLUGIN_NAME}] backlog exceeds {MAX_FETCH_PAGES} pages "
                f"({FETCH_PAGE_SIZE * MAX_FETCH_PAGES} posts); older new posts skipped"
            )
        return collected

    # ------------------------------------------------------------------
    # 水位线判定 / 推进（纯逻辑在 state.py，这里只做「读配置 + 传状态」的适配）
    # ------------------------------------------------------------------
    def _select_new(
        self, items: list[dict[str, Any]], watermark: int
    ) -> list[dict[str, Any]]:
        """挑出尚未推送过的新帖（boundary 从当前状态取）。"""
        return state.select_new(
            items, watermark, set(self._state.get("boundary_keys", []) or [])
        )

    def _apply_bootstrap(self, items: list[dict[str, Any]]) -> None:
        """首次启动设定水位线基线。

        迁移场景（_migrate_silent）强制 mark_all：旧版本升级且此前已运行过时零重推。
        基线一旦落定即复位 _migrate_silent，避免下轮再走一次静默对齐。
        """
        mode = (
            "mark_all"
            if self._migrate_silent
            else str(self.config.get("bootstrap_mode", "latest_one"))
        )
        state.apply_bootstrap(self._state, items, mode)
        if self._state.get("bootstrap_done"):
            self._migrate_silent = False

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
    # state.json
    # ------------------------------------------------------------------
    async def _save_state(self) -> None:
        async with self._lock:
            snapshot = state.snapshot_state(self._state)
        try:
            # 文件 IO 是同步阻塞调用，丢到线程池避免阻塞 event loop
            await asyncio.to_thread(state.write_state_file, self._state_path, snapshot)
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] save state failed: {e}")
