import asyncio
import aiohttp
import json
import os
from typing import Dict, List, Optional
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from .bili_login import BilibiliLoginManager

@register("bili_live_notice", "Binbim", "B站UP主开播订阅插件", "1.2.1", "https://github.com/BB0813/astrbot_plugin_bilibiliobs")
class BiliLiveNoticePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.check_interval = int(self.config.get("check_interval", 60)) if isinstance(self.config, dict) else 60
        self.max_monitors = int(self.config.get("max_monitors", 50)) if isinstance(self.config, dict) else 50
        self.enable_notifications = bool(self.config.get("enable_notifications", True)) if isinstance(self.config, dict) else True
        self.enable_end_notifications = bool(self.config.get("enable_end_notifications", True)) if isinstance(self.config, dict) else True
        # 按会话隔离的订阅数据: {unified_msg_origin: {uid: {uname, room_id, added_by, added_time, at_all}}}
        self.monitored_uids: Dict[str, Dict[str, Dict]] = {}
        self.live_status_cache: Dict[str, int] = {}
        self.uid_error_counts: Dict[str, int] = {}
        self.uid_skip_until: Dict[str, float] = {}
        self.current_interval = self.check_interval
        self.monitor_task = None
        self.session = None
        self._pending_tasks: set = set()          # 保存错峰发送任务
        self.stagger_interval = int(self.config.get("stagger_interval", 15)) if isinstance(self.config, dict) else 15
        self._last_rate_limited = False
        self._init_lock = asyncio.Lock()
        self._initialized = False
        # 配置文件路径
        self.config_file = os.path.join(self._get_data_dir(), "monitor_config.json")
        # 扫码登录管理器（管理员私聊可扫码续期Cookie）
        self.login_manager = BilibiliLoginManager(context, self._save_cookie_to_config)
        admin_umo = self.config.get("admin_umo", "") if isinstance(self.config, dict) else ""
        if admin_umo:
            self.login_manager.set_admin_id(admin_umo)
        # 启动初始化任务
        asyncio.create_task(self.initialize())

    def _get_data_dir(self) -> str:
        """获取数据存储目录，优先使用 AstrBot 数据目录"""
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
            base = os.path.join(get_astrbot_data_path(), "plugins", "bili_live_notice")
        except Exception:
            base = os.path.join(os.path.expanduser("~"), ".astrbot", "bili_live_notice")
        os.makedirs(base, exist_ok=True)
        return base

    async def ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
            logger.info("HTTP会话已创建")

    async def initialize(self):
        """插件初始化方法"""
        async with self._init_lock:
            if self._initialized:
                logger.info("插件已初始化，跳过")
                return
            try:
                logger.info("正在初始化B站开播订阅插件...")

                # 初始化HTTP会话
                await self.ensure_session()

                # 加载配置文件
                await self.load_config()

                # 统计总订阅数
                total = sum(len(uids) for uids in self.monitored_uids.values())
                logger.info(f"已加载 {total} 个订阅配置")

                # 启动订阅任务
                if not self.monitor_task or self.monitor_task.done():
                    self.monitor_task = asyncio.create_task(self.monitor_live_status())
                    logger.info("订阅检测任务已启动")

                self._initialized = True
                logger.info("B站开播订阅插件初始化完成")

            except Exception as e:
                logger.error(f"插件初始化失败: {e}")
                await self._cleanup_resources()
                raise

    async def load_config(self):
        """加载订阅配置文件，支持旧格式自动迁移"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    raw = data.get('monitored_uids', {})
                    # 检测旧格式并自动迁移
                    if raw and _is_old_format(raw):
                        logger.info("检测到旧版订阅格式，正在自动迁移...")
                        self.monitored_uids = _migrate_old_format(raw)
                        logger.info(f"迁移完成，共 {sum(len(v) for v in self.monitored_uids.values())} 条订阅")
                    else:
                        self.monitored_uids = raw
                    self.live_status_cache = data.get('live_status_cache', {})
                    self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                    self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                    admin_umo = data.get('admin_umo', "")
                    if admin_umo:
                        self.login_manager.set_admin_id(admin_umo)
                    total = sum(len(uids) for uids in self.monitored_uids.values())
                    logger.info(f"已加载 {total} 个订阅配置")
            else:
                # 兼容旧路径迁移
                legacy_file = os.path.join(os.path.dirname(__file__), "monitor_config.json")
                if os.path.exists(legacy_file):
                    with open(legacy_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        raw = data.get('monitored_uids', {})
                        if raw and _is_old_format(raw):
                            self.monitored_uids = _migrate_old_format(raw)
                        else:
                            self.monitored_uids = raw
                        self.live_status_cache = data.get('live_status_cache', {})
                        self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                        self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                        admin_umo = data.get('admin_umo', "")
                        if admin_umo:
                            self.login_manager.set_admin_id(admin_umo)
                    await self.save_config()
                    logger.info("已从旧路径迁移配置")
                else:
                    logger.info("配置文件不存在，使用默认配置")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")

    async def save_config(self):
        """保存订阅配置到文件"""
        try:
            data = {
                'monitored_uids': self.monitored_uids,
                'live_status_cache': self.live_status_cache,
                'enable_notifications': self.enable_notifications,
                'enable_end_notifications': self.enable_end_notifications,
                'admin_umo': self.login_manager.get_admin_id() or ""
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug("配置文件已保存")
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")

    def _get_bilibili_headers(self) -> Dict[str, str]:
        """获取B站API请求头，支持Cookie"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        cookie = self.config.get("bilibili_cookie", "") if isinstance(self.config, dict) else ""
        if cookie:
            headers["Cookie"] = cookie
        return headers

    async def _save_cookie_to_config(self, cookie: str):
        """扫码登录成功回调：把新的Cookie写回插件配置并持久化"""
        try:
            if isinstance(self.config, dict):
                self.config["bilibili_cookie"] = cookie
            else:
                self.config = {"bilibili_cookie": cookie}

            saved = False
            save_async = getattr(self.config, "save_config_async", None)
            if callable(save_async):
                saved = bool(await save_async())
            if not saved:
                # 兜底：按AstrBot配置目录结构直接写文件
                config_path = os.path.join(
                    self._get_data_dir(), "..", "..", "config", "bili_live_notice_config.json"
                )
                os.makedirs(os.path.dirname(config_path), exist_ok=True)
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(dict(self.config), f, ensure_ascii=False, indent=2)
                logger.info(f"Cookie已写入配置文件: {os.path.abspath(config_path)}")
            else:
                logger.info("Cookie已保存到插件配置")
        except Exception as e:
            logger.error(f"保存Cookie到配置失败: {e}")

    async def get_live_status(self, uid: str) -> Dict:
        """获取指定UID的直播状态"""
        try:
            batch = await self.get_live_status_batch([uid])
            if uid in batch:
                return batch[uid]
        except asyncio.TimeoutError:
            logger.error(f"获取UID {uid} 直播状态超时")
        except aiohttp.ClientError as e:
            logger.error(f"网络请求错误 (UID: {uid}): {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误 (UID: {uid}): {e}")
        except ValueError as e:
            logger.error(f"UID格式错误: {uid}, {e}")
        except Exception as e:
            logger.error(f"获取UID {uid} 直播状态失败: {e}")

        return {"live_status": 0, "room_id": 0, "title": "", "uname": "", "cover": ""}

    async def get_live_status_batch(self, uids: list[str]) -> Dict[str, Dict]:
        """批量获取多个UID的直播状态，返回以字符串UID为键的字典"""
        result_map: Dict[str, Dict] = {}
        try:
            await self.ensure_session()
            url = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
            data = {"uids": [int(u) for u in uids]}
            headers = self._get_bilibili_headers()
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.session.post(url, json=data, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    body = await response.json()
                    if body.get("code") == 0:
                        self._last_rate_limited = False
                        data_obj = body.get("data", {})
                        if isinstance(data_obj, dict):
                            for u in uids:
                                key = str(u)
                                user_data = data_obj.get(key)
                                if user_data:
                                    result_map[str(u)] = {
                                        "live_status": user_data.get("live_status", 0),
                                        "room_id": user_data.get("room_id", 0),
                                        "title": user_data.get("title", ""),
                                        "uname": user_data.get("uname", ""),
                                        "cover": user_data.get("cover_from_user", "")
                                    }
                        elif isinstance(data_obj, list):
                            by_uid = {}
                            for entry in data_obj:
                                uid_val = str(entry.get("uid") or entry.get("mid") or "")
                                if uid_val:
                                    by_uid[uid_val] = entry
                            for u in uids:
                                entry = by_uid.get(str(u))
                                if entry:
                                    result_map[str(u)] = {
                                        "live_status": entry.get("live_status", 0),
                                        "room_id": entry.get("room_id", 0),
                                        "title": entry.get("title", ""),
                                        "uname": entry.get("uname", ""),
                                        "cover": entry.get("cover_from_user", "")
                                    }
                    else:
                        logger.warning(f"B站API返回错误码: {body.get('code')}, 消息: {body.get('message', '未知错误')}")
                        # -101: 账号未登录，说明配置的Cookie已失效，通知管理员扫码续期
                        if body.get('code') == -101:
                            cookie = self.config.get("bilibili_cookie", "") if isinstance(self.config, dict) else ""
                            if cookie:
                                asyncio.create_task(
                                    self.login_manager.check_and_notify_cookie_invalid(
                                        cookie, "B站API返回-101错误"
                                    )
                                )
                elif response.status == 429:
                    self._last_rate_limited = True
                    logger.warning(f"B站API请求频率限制，状态码: {response.status}")
                else:
                    logger.warning(f"B站API请求失败，状态码: {response.status}")
        except Exception as e:
            logger.error(f"批量获取直播状态失败: {e}")
        finally:
            for u in uids:
                if str(u) not in result_map:
                    result_map[str(u)] = {"live_status": 0, "room_id": 0, "title": "", "uname": "", "cover": ""}
        return result_map

    async def monitor_live_status(self):
        """检测直播状态的后台任务"""
        consecutive_errors = 0
        max_consecutive_errors = 5

        while True:
            try:
                if not self.monitored_uids:
                    await asyncio.sleep(self.check_interval)
                    continue

                # 收集所有需要检查的UID（去重）
                all_uids = set()
                for origin_uids in self.monitored_uids.values():
                    all_uids.update(origin_uids.keys())

                if not all_uids:
                    await asyncio.sleep(self.check_interval)
                    continue

                # 批量查询状态
                now = asyncio.get_running_loop().time()
                uids_to_check = [uid for uid in all_uids
                                 if self.uid_skip_until.get(uid, 0) <= now]
                status_map = await self.get_live_status_batch(uids_to_check)

                # ---------- 第一步：集中计算每个 UID 的状态变化（只算一次） ----------
                status_changes: Dict[str, str] = {}   # uid -> "start" / "end"
                for uid in all_uids:
                    current_status = status_map.get(uid, {"live_status": 0})
                    previous_status = self.live_status_cache.get(uid, 0)
                    cur_live = current_status.get("live_status", 0)

                    if cur_live == 1 and previous_status != 1:
                        status_changes[uid] = "start"
                    elif previous_status == 1 and cur_live != 1:
                        status_changes[uid] = "end"

                # ---------- 第二步：立即更新全局缓存，防止本轮重复触发 ----------
                for uid in all_uids:
                    self.live_status_cache[uid] = status_map.get(uid, {}).get("live_status", 0)

                # ---------- 第三步：按会话派发通知，同一 UID 多会话错峰 ----------
                # pending: {uid: [(origin, monitor_info, change, status_info), ...]}
                pending: Dict[str, list] = {}
                for origin, origin_uids in list(self.monitored_uids.items()):
                    for uid, monitor_info in list(origin_uids.items()):
                        change = status_changes.get(uid)
                        if not change:
                            continue
                        current_status = status_map.get(
                            uid,
                            {"live_status": 0, "room_id": 0, "title": "", "uname": "", "cover": ""}
                        )
                        pending.setdefault(uid, []).append(
                            (origin, monitor_info, change, current_status)
                        )

                for uid, targets in pending.items():
                    # 按添加时间排序，保证错峰顺序稳定
                    targets.sort(key=lambda t: t[1].get("added_time", 0))
                    total = len(targets)
                    for idx, (origin, monitor_info, change, current_status) in enumerate(targets):
                        delay = idx * self.stagger_interval
                        if change == "start":
                            task = asyncio.create_task(
                                self._delayed_send_live(delay, uid, current_status, origin, monitor_info)
                            )
                        else:
                            task = asyncio.create_task(
                                self._delayed_send_end(delay, uid, current_status, origin, monitor_info)
                            )
                        self._pending_tasks.add(task)
                        task.add_done_callback(self._pending_tasks.discard)

                    if total > 1:
                        logger.info(
                            f"UID {uid} 有 {total} 个会话订阅，将按 0/{self.stagger_interval}s/... 错峰推送"
                        )

                # ---------- 第四步：错误统计与退避 ----------
                for uid in uids_to_check:
                    current_status = status_map.get(uid, {"live_status": 0})
                    is_empty = (not current_status.get("uname")) and current_status.get("room_id", 0) == 0
                    if is_empty:
                        cnt = self.uid_error_counts.get(uid, 0) + 1
                        self.uid_error_counts[uid] = cnt
                        self.uid_skip_until[uid] = now + min(300, 30 * cnt)
                    else:
                        self.uid_error_counts.pop(uid, None)
                        self.uid_skip_until.pop(uid, None)

                consecutive_errors = 0

                # 基于限流动态调整间隔
                await asyncio.sleep(self.current_interval)
                if self._last_rate_limited:
                    self.current_interval = min(300, max(self.check_interval, int(self.current_interval * 2)))
                else:
                    self.current_interval = max(self.check_interval, int(self.current_interval * 0.75))

            except asyncio.CancelledError:
                logger.info("订阅检测任务被取消")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"订阅检测任务出错 (第{consecutive_errors}次): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    wait_time = min(300, 60 * consecutive_errors)
                    logger.warning(f"连续错误{consecutive_errors}次，等待{wait_time}秒后重试")
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(self.current_interval)

    def _build_message_chain(self, template: str, uname: str, title: str, room_id: int, cover: str, at_all: bool = False) -> MessageChain:
        """根据模板构建消息链"""
        # 替换占位符
        text = template.format(uname=uname, title=title, room_id=room_id, cover="")
        chain = MessageChain()
        if at_all:
            chain.at_all()
        chain.message(text)
        # 如果有封面图，单独添加（不放进模板替换）
        if cover and "{cover}" in template:
            chain.url_image(cover)
        elif cover:
            # 即使模板没写 {cover}，也附带封面
            chain.url_image(cover)
        return chain

    def _sanitize_template(self, template: str) -> str:
        """
        清洗模板字符串，统一处理换行符。
        解决用户在配置界面输入字面 \n (反斜杠+n) 导致消息不换行的问题。
        """
        if not isinstance(template, str):
            return str(template) if template else ""

        # 1. 处理 Windows 风格换行 \r\n -> \n (防止部分平台出现空行)
        sanitized = template.replace('\r\n', '\n')

        # 2. 核心修复：将字面的 "\n" (即字符 \ 和 n) 替换为真实换行符 (ASCII 10)
        # 注意：这里必须用 '\\n' 来匹配字符串中实际存在的反斜杠字符
        sanitized = sanitized.replace('\\n', '\n')

        # 3. (可选) 如果你也想支持字面的 \t (制表符)，可以取消下面这行的注释
        # sanitized = sanitized.replace('\\t', '\t')

        return sanitized

    def _get_notify_template(self, is_live: bool) -> str:
        """获取通知模板"""
        if is_live:
            # 默认值中的 \n 在 Python 源码中已经是真实换行符
            default = "🔴 {uname} 开播啦！\n📺 直播标题: {title}\n🔗 直播间: https://live.bilibili.com/{room_id}"
        else:
            default = "⚫ {uname} 已结束直播"

        key = "live_notify_template" if is_live else "end_notify_template"

        raw_template = default
        if isinstance(self.config, dict):
            # 从配置中获取，如果用户没配则使用 default
            raw_template = self.config.get(key, default)

        # 👇 关键步骤：返回前进行清洗，确保字面 \n 被转为真实换行
        return self._sanitize_template(raw_template)

    async def send_live_notification(self, uid: str, status_info: Dict, origin: str, monitor_info: Dict):
        """发送开播通知"""
        try:
            if not self.enable_notifications:
                logger.info("已禁用开播通知，跳过发送")
                return
            uname = status_info.get("uname", "未知UP主")
            title = status_info.get("title", "无标题")
            room_id = status_info.get("room_id", 0)
            cover = status_info.get("cover", "")
            at_all = monitor_info.get("at_all", False)

            template = self._get_notify_template(is_live=True)
            message_chain = self._build_message_chain(template, uname, title, room_id, cover, at_all)
            await self.context.send_message(origin, message_chain)
            logger.info(f"开播通知已发送: {uname} -> {origin}")
        except Exception as e:
            logger.error(f"发送开播通知失败: {e}")

    async def send_end_notification(self, uid: str, status_info: Dict, origin: str, monitor_info: Dict):
        """发送关播通知"""
        try:
            if not self.enable_notifications or not self.enable_end_notifications:
                return
            uname = status_info.get("uname", "未知UP主")
            room_id = status_info.get("room_id", 0)
            cover = status_info.get("cover", "")
            at_all = monitor_info.get("at_all", False)

            template = self._get_notify_template(is_live=False)
            message_chain = self._build_message_chain(template, uname, "", room_id, cover, at_all)
            await self.context.send_message(origin, message_chain)
            logger.info(f"关播通知已发送: {uname} -> {origin}")
        except Exception as e:
            logger.error(f"发送关播通知失败: {e}")

    async def _delayed_send_live(self, delay: float, uid: str, status_info: Dict,
                                 origin: str, monitor_info: Dict):
        """错峰延迟发送开播通知"""
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self.send_live_notification(uid, status_info, origin, monitor_info)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"延迟开播通知失败 (uid={uid}): {e}")

    async def _delayed_send_end(self, delay: float, uid: str, status_info: Dict,
                                origin: str, monitor_info: Dict):
        """错峰延迟发送关播通知"""
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self.send_end_notification(uid, status_info, origin, monitor_info)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"延迟关播通知失败 (uid={uid}): {e}")

    def _count_total_monitors(self) -> int:
        """统计所有会话的总订阅数"""
        return sum(len(uids) for uids in self.monitored_uids.values())

    def _get_current_origin_monitors(self, origin: str) -> Dict[str, Dict]:
        """获取当前会话的订阅列表"""
        return self.monitored_uids.get(origin, {})

    @filter.command("")
    async def handle_admin_login(self, event: AstrMessageEvent):
        """全局消息入口：优先处理管理员私聊的扫码登录命令"""
        # 非私聊直接放行，避免影响群聊指令
        if not event.is_private_chat():
            return

        # 私聊时记录管理员会话，便于后续推送Cookie失效通知
        if self.login_manager.record_admin(event):
            await self.save_config()

        # 扫码登录命令由登录管理器处理
        await self.login_manager.handle_admin_command(event)

    @filter.command("添加订阅")
    async def add_monitor(self, event: AstrMessageEvent):
        """添加UP主订阅"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /添加订阅 <UID> [at_all]\n例如: /添加订阅 123456\n可选参数 at_all 表示开播时@全体成员")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            origin = event.unified_msg_origin

            # 检查当前会话是否已订阅此UID
            origin_uids = self.monitored_uids.get(origin, {})
            if uid in origin_uids:
                yield event.plain_result(f"❌ 当前会话已订阅UID {uid}，请勿重复添加")
                return

            # 数量限制（按当前会话）
            if len(origin_uids) >= self.max_monitors:
                yield event.plain_result(f"❌ 当前会话订阅数量已达上限({self.max_monitors})")
                return

            # 检查UP主是否存在
            status_info = await self.get_live_status(uid)
            if not status_info.get("uname"):
                yield event.plain_result(f"❌ 未找到UID为 {uid} 的UP主")
                return

            # 解析 at_all 参数
            at_all = "at_all" in args

            # 添加到当前会话的订阅列表
            if origin not in self.monitored_uids:
                self.monitored_uids[origin] = {}
            self.monitored_uids[origin][uid] = {
                "uname": status_info.get("uname", ""),
                "room_id": status_info.get("room_id", 0),
                "added_by": event.get_sender_name(),
                "added_time": asyncio.get_running_loop().time(),
                "at_all": at_all
            }
            self.live_status_cache[uid] = status_info["live_status"]

            await self.save_config()

            uname = status_info.get("uname", "未知UP主")
            at_all_tip = "（开播时@全体成员）" if at_all else ""
            yield event.plain_result(f"✅ 已添加 {uname}(UID:{uid}) 到当前会话订阅列表{at_all_tip}")

        except Exception as e:
            logger.error(f"添加订阅失败: {e}")
            yield event.plain_result("❌ 添加订阅失败，请稍后重试")

    @filter.command("批量添加订阅")
    async def batch_add_monitor(self, event: AstrMessageEvent):
        """批量添加UP主订阅"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /批量添加订阅 <UID1> <UID2> [at_all]\n例如: /批量添加订阅 123456 789012")
                return

            at_all = "at_all" in args
            uids = [a for a in args[1:] if a.isdigit()]
            if not uids:
                yield event.plain_result("❌ 未找到有效的UID")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})
            added = []
            skipped = []
            failed = []

            for uid in uids:
                if uid in origin_uids:
                    skipped.append(uid)
                    continue
                if len(origin_uids) + len(added) >= self.max_monitors:
                    failed.append(f"{uid}(达上限)")
                    continue

                status_info = await self.get_live_status(uid)
                if not status_info.get("uname"):
                    failed.append(f"{uid}(未找到)")
                    continue

                if origin not in self.monitored_uids:
                    self.monitored_uids[origin] = {}
                self.monitored_uids[origin][uid] = {
                    "uname": status_info.get("uname", ""),
                    "room_id": status_info.get("room_id", 0),
                    "added_by": event.get_sender_name(),
                    "added_time": asyncio.get_running_loop().time(),
                    "at_all": at_all
                }
                self.live_status_cache[uid] = status_info["live_status"]
                added.append(f"{status_info.get('uname', '')}(UID:{uid})")

            await self.save_config()

            parts = []
            if added:
                parts.append(f"✅ 已添加: {', '.join(added)}")
            if skipped:
                parts.append(f"⏭ 已跳过(重复): {', '.join(skipped)}")
            if failed:
                parts.append(f"❌ 失败: {', '.join(failed)}")
            yield event.plain_result("\n".join(parts) if parts else "没有可处理的UID")

        except Exception as e:
            logger.error(f"批量添加订阅失败: {e}")
            yield event.plain_result("❌ 批量添加订阅失败，请稍后重试")

    @filter.command("移除订阅")
    async def remove_monitor(self, event: AstrMessageEvent):
        """移除UP主订阅"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /移除订阅 <UID>\n例如: /移除订阅 123456")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})

            if uid in origin_uids:
                del self.monitored_uids[origin][uid]
                # 如果该会话没有订阅了，清理空字典
                if not self.monitored_uids[origin]:
                    del self.monitored_uids[origin]
                # 注意：不清理live_status_cache，因为其他会话可能还在用
                await self.save_config()
                yield event.plain_result(f"✅ 已移除UID {uid} 的订阅")
            else:
                yield event.plain_result(f"❌ 当前会话中UID {uid} 不在订阅列表中")

        except Exception as e:
            logger.error(f"移除订阅失败: {e}")
            yield event.plain_result("❌ 移除订阅失败，请稍后重试")

    @filter.command("批量移除订阅")
    async def batch_remove_monitor(self, event: AstrMessageEvent):
        """批量移除UP主订阅"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /批量移除订阅 <UID1> <UID2>\n例如: /批量移除订阅 123456 789012")
                return

            uids = [a for a in args[1:] if a.isdigit()]
            if not uids:
                yield event.plain_result("❌ 未找到有效的UID")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})
            removed = []
            not_found = []

            for uid in uids:
                if uid in origin_uids:
                    del self.monitored_uids[origin][uid]
                    removed.append(uid)
                else:
                    not_found.append(uid)

            # 清理空字典
            if origin in self.monitored_uids and not self.monitored_uids[origin]:
                del self.monitored_uids[origin]

            await self.save_config()

            parts = []
            if removed:
                parts.append(f"✅ 已移除: {', '.join(removed)}")
            if not_found:
                parts.append(f"❌ 未找到: {', '.join(not_found)}")
            yield event.plain_result("\n".join(parts) if parts else "没有可处理的UID")

        except Exception as e:
            logger.error(f"批量移除订阅失败: {e}")
            yield event.plain_result("❌ 批量移除订阅失败，请稍后重试")

    @filter.command("订阅列表")
    async def list_monitors(self, event: AstrMessageEvent):
        """查看当前会话的订阅列表"""
        try:
            origin = event.unified_msg_origin
            origin_uids = self._get_current_origin_monitors(origin)

            if not origin_uids:
                yield event.plain_result("📝 当前会话没有订阅任何UP主")
                return

            message = "📝 当前会话订阅列表:\n"
            for uid, info in origin_uids.items():
                status_info = await self.get_live_status(uid)
                uname = info.get("uname", status_info.get("uname", "未知UP主"))
                live_status = "🔴 直播中" if status_info.get("live_status") == 1 else "⚫ 未开播"
                at_all_tip = " 📢@all" if info.get("at_all") else ""
                message += f"• {uname}(UID:{uid}) - {live_status}{at_all_tip}\n"

            yield event.plain_result(message.strip())

        except Exception as e:
            logger.error(f"获取订阅列表失败: {e}")
            yield event.plain_result("❌ 获取订阅列表失败，请稍后重试")

    @filter.command("检查直播")
    async def check_live(self, event: AstrMessageEvent):
        """手动检查指定UP主的直播状态"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /检查直播 <UID>\n例如: /检查直播 123456")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            status_info = await self.get_live_status(uid)
            if not status_info.get("uname"):
                yield event.plain_result(f"❌ 未找到UID为 {uid} 的UP主")
                return

            uname = status_info.get("uname", "未知UP主")
            live_status = status_info.get("live_status", 0)

            if live_status == 1:
                title = status_info.get("title", "无标题")
                room_id = status_info.get("room_id", 0)
                cover = status_info.get("cover", "")
                message = f"🔴 {uname} 正在直播\n"
                message += f"📺 直播标题: {title}\n"
                message += f"🔗 直播间: https://live.bilibili.com/{room_id}"
                if cover:
                    yield event.make_result().message(message).url_image(cover)
                    return
            else:
                message = f"⚫ {uname} 当前未开播"

            yield event.plain_result(message)

        except Exception as e:
            logger.error(f"检查直播状态失败: {e}")
            yield event.plain_result("❌ 检查直播状态失败，请稍后重试")

    async def _cleanup_resources(self):
        """清理插件资源"""
        try:
            if self.monitor_task and not self.monitor_task.done():
                self.monitor_task.cancel()
                try:
                    await self.monitor_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"取消订阅检测任务时出错: {e}")
                finally:
                    self.monitor_task = None

            # 取消所有待发送的错峰任务
            if self._pending_tasks:
                for t in list(self._pending_tasks):
                    t.cancel()
                await asyncio.gather(*self._pending_tasks, return_exceptions=True)
                self._pending_tasks.clear()

            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None

        except Exception as e:
            logger.error(f"清理资源时出错: {e}")

    def get_plugin_status(self) -> Dict:
        """获取插件运行状态"""
        return {
            "session_active": self.session and not self.session.closed,
            "monitor_task_running": self.monitor_task and not self.monitor_task.done(),
            "monitored_count": self._count_total_monitors(),
            "session_count": len(self.monitored_uids),
            "config_file_exists": os.path.exists(self.config_file)
        }

    @filter.command("插件状态")
    async def plugin_status(self, event: AstrMessageEvent):
        """查看插件运行状态"""
        try:
            status = self.get_plugin_status()

            message = "🔧 插件运行状态:\n"
            message += f"• HTTP会话: {'✅ 正常' if status['session_active'] else '❌ 异常'}\n"
            message += f"• 订阅检测任务: {'✅ 运行中' if status['monitor_task_running'] else '❌ 已停止'}\n"
            message += f"• 总订阅数量: {status['monitored_count']} 个UP主\n"
            message += f"• 订阅会话数: {status['session_count']} 个\n"
            message += f"• 配置文件: {'✅ 存在' if status['config_file_exists'] else '❌ 缺失'}"

            yield event.plain_result(message)

        except Exception as e:
            logger.error(f"获取插件状态失败: {e}")
            yield event.plain_result("❌ 获取插件状态失败")

    @filter.command("订阅帮助")
    async def show_help(self, event: AstrMessageEvent):
        """查看所有可用指令及使用方式"""
        help_text = (
            "📋 B站UP主开播订阅插件 - 指令列表\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "【添加订阅】\n"
            "  用法: /添加订阅 <UID> [at_all]\n"
            "  说明: 添加一个UP主到当前会话的订阅列表\n"
            "  示例: /添加订阅 123456\n"
            "  可选: 末尾加 at_all 表示开播时@全体成员\n\n"
            "【批量添加订阅】\n"
            "  用法: /批量添加订阅 <UID1> <UID2> ... [at_all]\n"
            "  说明: 一次添加多个UP主订阅\n"
            "  示例: /批量添加订阅 123456 789012\n\n"
            "【移除订阅】\n"
            "  用法: /移除订阅 <UID>\n"
            "  说明: 从当前会话移除指定UP主的订阅\n"
            "  示例: /移除订阅 123456\n\n"
            "【批量移除订阅】\n"
            "  用法: /批量移除订阅 <UID1> <UID2> ...\n"
            "  说明: 一次移除多个UP主订阅\n"
            "  示例: /批量移除订阅 123456 789012\n\n"
            "【订阅列表】\n"
            "  用法: /订阅列表\n"
            "  说明: 查看当前会话已订阅的所有UP主及直播状态\n\n"
            "【检查直播】\n"
            "  用法: /检查直播 <UID>\n"
            "  说明: 手动查询指定UP主当前的直播状态\n"
            "  示例: /检查直播 123456\n\n"
            "【插件状态】\n"
            "  用法: /插件状态\n"
            "  说明: 查看插件运行状态（会话、任务、订阅数等）\n\n"
            "【开启通知】\n"
            "  用法: /开启通知\n"
            "  说明: 开启开播与关播通知\n\n"
            "【关闭通知】\n"
            "  用法: /关闭通知\n"
            "  说明: 关闭所有通知（开播+关播）\n\n"
            "【开启关播通知】\n"
            "  用法: /开启关播通知\n"
            "  说明: 单独开启关播通知\n\n"
            "【关闭关播通知】\n"
            "  用法: /关闭关播通知\n"
            "  说明: 单独关闭关播通知\n\n"
            "【订阅帮助】\n"
            "  用法: /订阅帮助\n"
            "  说明: 显示本帮助信息\n\n"
            "【扫码续期Cookie】(管理员私聊)\n"
            "  用法: 私聊发送 更新cookie / b站登录 / bilibili登录\n"
            "  说明: 返回B站登录二维码，扫码后自动更新Cookie；\n"
            "        Cookie失效时也会私聊提醒管理员\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 当前版本: 1.2.1 | 检查间隔: {self.check_interval}s | 单会话上限: {self.max_monitors} | 错峰间隔: {self.stagger_interval}s"
        )
        try:
            yield event.plain_result(help_text)
        except Exception as e:
            logger.error(f"显示帮助信息失败: {e}")
            yield event.plain_result("❌ 获取帮助信息失败")

    @filter.command("开启通知")
    async def enable_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_notifications = True
            await self.save_config()
            yield event.plain_result("✅ 已开启开播与关播通知")
        except Exception as e:
            logger.error(f"开启通知失败: {e}")
            yield event.plain_result("❌ 开启通知失败")

    @filter.command("关闭通知")
    async def disable_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_notifications = False
            await self.save_config()
            yield event.plain_result("✅ 已关闭所有通知")
        except Exception as e:
            logger.error(f"关闭通知失败: {e}")
            yield event.plain_result("❌ 关闭通知失败")

    @filter.command("开启关播通知")
    async def enable_end_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_end_notifications = True
            await self.save_config()
            yield event.plain_result("✅ 已开启关播通知")
        except Exception as e:
            logger.error(f"开启关播通知失败: {e}")
            yield event.plain_result("❌ 开启关播通知失败")

    @filter.command("关闭关播通知")
    async def disable_end_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_end_notifications = False
            await self.save_config()
            yield event.plain_result("✅ 已关闭关播通知")
        except Exception as e:
            logger.error(f"关闭关播通知失败: {e}")
            yield event.plain_result("❌ 关闭关播通知失败")

    async def terminate(self):
        """插件销毁方法"""
        try:
            logger.info("正在停止B站开播订阅插件...")

            if hasattr(self, 'monitored_uids') and self.monitored_uids:
                await self.save_config()
                logger.info("订阅配置已保存")

            await self._cleanup_resources()

            logger.info("B站开播订阅插件已完全停止")

        except Exception as e:
            logger.error(f"插件销毁时出错: {e}")
            try:
                await self._cleanup_resources()
            except Exception as cleanup_error:
                logger.error(f"强制清理资源时出错: {cleanup_error}")


def _is_old_format(data: Dict) -> bool:
    """检测是否为旧版数据格式（顶层key是UID数字）"""
    if not data:
        return False
    sample_key = next(iter(data))
    # 旧格式: key是UID数字，value包含 unified_msg_origin 字段
    if sample_key.isdigit() and isinstance(data[sample_key], dict):
        return "unified_msg_origin" in data[sample_key]
    return False


def _migrate_old_format(old_data: Dict) -> Dict[str, Dict[str, Dict]]:
    """将旧格式迁移到新格式: {uid: {.., unified_msg_origin}} -> {origin: {uid: {..}}}"""
    new_data: Dict[str, Dict[str, Dict]] = {}
    for uid, info in old_data.items():
        origin = info.pop("unified_msg_origin", "unknown")
        if origin not in new_data:
            new_data[origin] = {}
        # 确保不含旧字段
        info.pop("unified_msg_origin", None)
        info.setdefault("at_all", False)
        new_data[origin][uid] = info
    return new_data