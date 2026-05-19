from __future__ import annotations

import asyncio
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import aiohttp

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star

PLUGIN_NAME = "astrbot_plugin_dracalon_feed"
USER_AGENT = "Dracalon-AstrBot-Feed/0.1"
INTER_TARGET_DELAY = 0.3
INTER_ITEM_DELAY = 0.5


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _resolve_data_dir() -> Path:
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        return Path(get_astrbot_data_path())
    except Exception:
        return Path.cwd() / "data"


def _default_state() -> dict[str, Any]:
    return {
        "pushed_urls": {},
        "last_poll_at": 0,
        "last_error": "",
        "bootstrap_done": False,
    }


class DracalonFeedPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config

        self._session: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task | None = None
        self._start_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

        state_dir = _resolve_data_dir() / "plugin_data" / PLUGIN_NAME
        state_dir.mkdir(parents=True, exist_ok=True)
        self._state_path = state_dir / "state.json"
        self._state = self._load_state()

        self._start_task = asyncio.create_task(self._start())

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
        """把当前会话加入推送列表"""
        umo = event.unified_msg_origin
        targets = list(self.config.get("targets", []) or [])
        if umo in targets:
            yield event.plain_result(f"当前会话已绑定：{umo}")
            return
        self.config["targets"] = targets + [umo]
        self.config.save_config()
        yield event.plain_result(f"已绑定推送目标：{umo}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("unbind")
    async def unbind(self, event: AstrMessageEvent):
        """把当前会话从推送列表移除"""
        umo = event.unified_msg_origin
        targets = list(self.config.get("targets", []) or [])
        if umo not in targets:
            yield event.plain_result(f"当前会话未绑定：{umo}")
            return
        targets.remove(umo)
        self.config["targets"] = targets
        self.config.save_config()
        yield event.plain_result(f"已解绑：{umo}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("list")
    async def list_targets(self, event: AstrMessageEvent):
        """列出全部推送目标"""
        targets = list(self.config.get("targets", []) or [])
        if not targets:
            yield event.plain_result("当前未绑定任何推送目标")
            return
        lines = [f"共 {len(targets)} 个推送目标："]
        lines.extend(f"  {idx}. {t}" for idx, t in enumerate(targets, 1))
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("status")
    async def status(self, event: AstrMessageEvent):
        """显示插件运行状态"""
        last_poll_at = int(self._state.get("last_poll_at", 0) or 0)
        last_poll_str = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_poll_at))
            if last_poll_at
            else "从未"
        )
        enabled = self.config.get("enabled", True)
        targets = self.config.get("targets", []) or []
        pushed_count = len(self._state.get("pushed_urls", {}) or {})
        last_error = self._state.get("last_error") or "无"
        bootstrap_done = self._state.get("bootstrap_done", False)

        lines = [
            f"[{PLUGIN_NAME}] 状态",
            f"  启用：{enabled}",
            f"  上次轮询：{last_poll_str}",
            f"  上次错误：{last_error}",
            f"  已推送条目：{pushed_count}",
            f"  推送目标数：{len(targets)}",
            f"  首次启动完成：{bootstrap_done}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("test")
    async def test(self, event: AstrMessageEvent):
        """绕过去重，立刻拉最新 1 条推到当前会话"""
        if self._session is None:
            yield event.plain_result("插件尚未完成初始化，请稍后再试")
            return
        try:
            items = await self._fetch()
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] test fetch failed: {e}")
            yield event.plain_result(f"拉取失败：{e}")
            return
        if not items:
            yield event.plain_result("拉取成功但当前列表为空")
            return
        latest = sorted(
            items, key=lambda x: str(x.get("published_at") or ""), reverse=True
        )[0]
        chain = self._build_chain(latest)
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
        """暂停推送（不卸载插件）"""
        self.config["enabled"] = False
        self.config.save_config()
        yield event.plain_result("已暂停推送")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @dracalon_feed.command("resume")
    async def resume(self, event: AstrMessageEvent):
        """恢复推送"""
        self.config["enabled"] = True
        self.config.save_config()
        yield event.plain_result("已恢复推送")

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def terminate(self) -> None:
        for task in (self._poll_task, self._start_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"[{PLUGIN_NAME}] task cleanup error: {e}")
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning(f"[{PLUGIN_NAME}] session close error: {e}")

    # ------------------------------------------------------------------
    # 启动 & 主循环
    # ------------------------------------------------------------------
    async def _start(self) -> None:
        try:
            delay = int(self.config.get("startup_delay_seconds", 10) or 0)
            if delay > 0:
                await asyncio.sleep(delay)
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
            logger.info(f"[{PLUGIN_NAME}] started, polling will begin")
            self._poll_task = asyncio.create_task(self._poll_loop())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] _start failed: {e}")

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

    async def _poll_once(self) -> None:
        items = await self._fetch()
        if items is None:
            return  # fetch 已记录 last_error

        self._state.setdefault("pushed_urls", {})

        if not self._state.get("bootstrap_done"):
            items_to_push = self._apply_bootstrap(items)
        else:
            pushed_snapshot = self._state["pushed_urls"]
            items_to_push = [
                it
                for it in items
                if it.get("url") and _url_hash(it["url"]) not in pushed_snapshot
            ]

        items_to_push.sort(key=lambda x: str(x.get("published_at") or ""))

        targets = list(self.config.get("targets", []) or [])
        for item in items_to_push:
            url = item.get("url") or ""
            if not url:
                continue
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
                # 通过 self._state["pushed_urls"] 间接写，避免持有局部引用
                # 在 _prune_old_dedup 替换 dict 后导致写入丢失
                self._state["pushed_urls"][_url_hash(url)] = int(time.time())
            await self._save_state()
            await asyncio.sleep(INTER_ITEM_DELAY)

        self._prune_old_dedup()
        async with self._lock:
            self._state["last_poll_at"] = int(time.time())
            self._state["last_error"] = ""
        await self._save_state()

    async def _fetch(self) -> list[dict[str, Any]] | None:
        if self._session is None:
            return None
        api_base = str(self.config.get("api_base", "")).rstrip("/")
        feed_key = str(self.config.get("feed_key", "community_hot"))
        if not api_base:
            async with self._lock:
                self._state["last_error"] = "api_base 未配置"
            return None
        url = f"{api_base}/api/homepage/feed"
        params = {"key": feed_key}
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
            return []
        return [it for it in items if isinstance(it, dict) and it.get("url")]

    def _apply_bootstrap(
        self, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        mode = str(self.config.get("bootstrap_mode", "latest_one"))
        now = int(time.time())
        self._state["bootstrap_done"] = True
        pushed = self._state["pushed_urls"]

        if mode == "push_all":
            return list(items)

        if mode == "mark_all":
            for item in items:
                url = item.get("url")
                if url:
                    pushed[_url_hash(url)] = now
            return []

        # latest_one
        sorted_items = sorted(
            [it for it in items if it.get("url")],
            key=lambda x: str(x.get("published_at") or ""),
            reverse=True,
        )
        if not sorted_items:
            return []
        latest = sorted_items[0]
        for item in sorted_items[1:]:
            pushed[_url_hash(item["url"])] = now
        return [latest]

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
        merged.update(data)
        if not isinstance(merged.get("pushed_urls"), dict):
            merged["pushed_urls"] = {}
        return merged

    async def _save_state(self) -> None:
        async with self._lock:
            snapshot = {
                "pushed_urls": dict(self._state.get("pushed_urls", {})),
                "last_poll_at": int(self._state.get("last_poll_at", 0) or 0),
                "last_error": str(self._state.get("last_error", "") or ""),
                "bootstrap_done": bool(self._state.get("bootstrap_done", False)),
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

    def _prune_old_dedup(self) -> None:
        retention_days = max(
            1, int(self.config.get("dedup_retention_days", 30) or 30)
        )
        cutoff = int(time.time()) - retention_days * 86400
        pushed = self._state.get("pushed_urls", {}) or {}
        if not pushed:
            return
        self._state["pushed_urls"] = {
            k: int(v)
            for k, v in pushed.items()
            if isinstance(v, (int, float)) and int(v) > cutoff
        }
