"""B站扫码登录续期模块

管理员私聊 Bot 发送 `更新cookie` / `b站登录` 等命令后，
本模块会向 B 站申请登录二维码并轮询扫码结果，
扫码成功后自动把新的 Cookie 写回插件配置（无需重启）。
"""

import asyncio
import io
import time
from typing import Dict, Optional

import aiohttp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import Image, Plain

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 触发扫码登录的命令（私聊，忽略大小写）
LOGIN_COMMANDS = {
    "更新cookie",
    "更新b站cookie",
    "b站登录",
    "bilibili登录",
    "扫码登录",
}


class BilibiliLoginManager:
    """B站登录管理器（扫码登录 + Cookie 失效检测）"""

    QRCODE_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
    QRCODE_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
    NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
    QR_CODE_TTL_SECONDS = 180  # 二维码有效期（3 分钟）
    POLL_INTERVAL_SECONDS = 2

    def __init__(self, context, config_save_callback):
        """初始化登录管理器

        Args:
            context: AstrBot Context 对象
            config_save_callback: 保存 Cookie 的回调，签名 async def(cookie: str)
        """
        self.context = context
        self.config_save_callback = config_save_callback
        self._login_in_progress = False
        self._lock = asyncio.Lock()
        self._admin_id: Optional[str] = None  # 管理员会话，用于推送 Cookie 失效通知

    # ------------------------------------------------------------------ #
    # 管理员会话
    # ------------------------------------------------------------------ #
    def set_admin_id(self, admin_id: Optional[str]):
        """设置（或恢复）管理员会话 ID"""
        if admin_id and admin_id != self._admin_id:
            self._admin_id = admin_id
            logger.info(f"已记录管理员会话: {admin_id}")

    def get_admin_id(self) -> Optional[str]:
        return self._admin_id

    def record_admin(self, event: AstrMessageEvent) -> bool:
        """从私聊事件中记录管理员会话

        Returns:
            True: 本次记录到了新的管理员会话
        """
        if not self._is_private_message(event):
            return False
        origin = event.unified_msg_origin
        if not origin or origin == self._admin_id:
            return False
        self.set_admin_id(origin)
        return True

    # ------------------------------------------------------------------ #
    # Cookie 校验
    # ------------------------------------------------------------------ #
    async def validate_cookie(self, cookie: str) -> Optional[bool]:
        """验证 Cookie 是否有效

        Returns:
            True: Cookie 有效
            False: Cookie 无效（-101 未登录）
            None: 网络错误或其他异常
        """
        if not cookie or not cookie.strip():
            return False

        headers = {
            "User-Agent": UA,
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
            "Cookie": cookie.strip(),
        }

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.NAV_URL, headers=headers) as resp:
                    if resp.content_type != "application/json":
                        return None
                    data = await resp.json()

                    if data.get("code") != 0:
                        if int(data.get("code", 0)) == -101:
                            return False
                        return None

                    nav_data = data.get("data") or {}
                    if "isLogin" not in nav_data:
                        return None
                    return bool(nav_data.get("isLogin"))
        except Exception as e:
            logger.error(f"验证Cookie失败: {e}")
            return None

    async def check_and_notify_cookie_invalid(self, cookie: str, reason: str = ""):
        """检查 Cookie 是否失效，失效则私聊通知管理员"""
        if not self._admin_id:
            logger.warning("尚未记录管理员会话，无法发送Cookie失效通知（请管理员私聊Bot一次）")
            return

        is_valid = await self.validate_cookie(cookie)
        if is_valid is not False:
            return

        reason_text = reason or "Cookie已失效"
        message = (
            "⚠️ 检测到B站Cookie不可用\n"
            f"原因: {reason_text}\n\n"
            "请回复以下任一命令进行扫码登录续期：\n"
            "• 更新cookie\n"
            "• b站登录\n"
            "或在插件配置中手动更新 bilibili_cookie"
        )
        try:
            await self.context.send_message(self._admin_id, message)
            logger.info("已向管理员发送Cookie失效通知")
        except Exception as e:
            logger.error(f"发送Cookie失效通知失败: {e}")

    # ------------------------------------------------------------------ #
    # 命令入口
    # ------------------------------------------------------------------ #
    async def handle_admin_command(self, event: AstrMessageEvent) -> bool:
        """处理管理员扫码登录命令（仅私聊）

        Returns:
            True: 命令已被处理
            False: 不是扫码登录命令
        """
        if not self._is_private_message(event):
            return False

        message_text = (event.get_message_str() or "").strip()
        if message_text.lower() not in LOGIN_COMMANDS:
            return False

        # 记录管理员会话
        self.set_admin_id(event.unified_msg_origin)

        await self._start_login_flow(event)
        return True

    @staticmethod
    def _is_private_message(event: AstrMessageEvent) -> bool:
        """判断是否为私聊消息"""
        try:
            return bool(event.is_private_chat())
        except Exception:
            origin = event.unified_msg_origin or ""
            return "group" not in origin.lower() and "friend" in origin.lower()

    # ------------------------------------------------------------------ #
    # 扫码登录流程
    # ------------------------------------------------------------------ #
    async def _start_login_flow(self, event: AstrMessageEvent):
        """发起扫码登录"""
        async with self._lock:
            if self._login_in_progress:
                await event.send(event.plain_result("⚠️ 已有一轮扫码登录正在进行，请稍候"))
                return
            self._login_in_progress = True

        try:
            qr_bytes, login_url, qrcode_key = await self._generate_qrcode()
            await self._send_qrcode(event, qr_bytes, login_url)

            asyncio.create_task(
                self._poll_login_and_notify(
                    qrcode_key=qrcode_key,
                    unified_msg_origin=event.unified_msg_origin,
                )
            )
            logger.info("扫码登录流程已启动，等待管理员扫码")
        except Exception as e:
            self._login_in_progress = False
            logger.error(f"启动扫码登录失败: {e}")
            try:
                await event.send(event.plain_result(f"❌ 启动扫码登录失败: {e}"))
            except Exception:
                pass

    async def _generate_qrcode(self):
        """申请登录二维码，返回 (二维码图片字节, 登录链接, qrcode_key)"""
        headers = {
            "User-Agent": UA,
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
        }
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.QRCODE_GENERATE_URL, headers=headers) as resp:
                data = await resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"生成二维码失败: {data.get('message', '未知错误')}")

        payload = data.get("data") or {}
        login_url = str(payload.get("url", "")).strip()
        qrcode_key = str(payload.get("qrcode_key", "")).strip()
        if not login_url or not qrcode_key:
            raise RuntimeError("生成二维码失败: 响应数据不完整")

        qr_bytes = await asyncio.to_thread(self._build_qrcode_png, login_url)
        return qr_bytes, login_url, qrcode_key

    @staticmethod
    def _build_qrcode_png(login_url: str) -> bytes:
        """把登录链接渲染成 PNG 字节（缺依赖时抛 ImportError）"""
        import qrcode  # 延迟导入，未安装时仅影响扫码登录

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=8,
            border=4,
        )
        qr.add_data(login_url)
        qr.make(fit=True)
        image = qr.make_image(fill_color="black", back_color="white")

        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    async def _send_qrcode(self, event: AstrMessageEvent, qr_bytes: bytes, login_url: str):
        """发送登录二维码（图片失败时回退为纯文本链接）"""
        text = (
            "📱 请使用哔哩哔哩客户端扫描下方二维码完成登录\n"
            f"⏱ 二维码有效期: {self.QR_CODE_TTL_SECONDS // 60} 分钟\n\n"
            "若无法显示图片，可在手机浏览器打开：\n"
            f"{login_url}"
        )
        try:
            chain = [Plain(text), Image.fromBytes(qr_bytes)]
            await event.send(event.chain_result(chain))
            logger.info("登录二维码已发送")
        except Exception as e:
            logger.warning(f"发送二维码图片失败: {e}，回退为纯文本链接")
            await event.send(
                event.plain_result(
                    f"📱 请在手机浏览器打开以下链接完成登录：\n{login_url}\n"
                    f"⏱ 链接有效期: {self.QR_CODE_TTL_SECONDS // 60} 分钟"
                )
            )

    async def _poll_login_and_notify(self, qrcode_key: str, unified_msg_origin: str):
        """轮询扫码结果，成功后写回 Cookie"""
        headers = {
            "User-Agent": UA,
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
        }
        try:
            deadline = time.monotonic() + self.QR_CODE_TTL_SECONDS
            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                while time.monotonic() < deadline:
                    await asyncio.sleep(self.POLL_INTERVAL_SECONDS)

                    try:
                        async with session.get(
                            self.QRCODE_POLL_URL,
                            params={"qrcode_key": qrcode_key},
                            headers=headers,
                        ) as resp:
                            poll_data = await resp.json()
                            poll_result = poll_data.get("data", {}) or {}
                            code = poll_result.get("code")

                            if code == 0:
                                cookie = self._extract_cookie(resp)
                                if cookie:
                                    await self.config_save_callback(cookie)
                                    await self.context.send_message(
                                        unified_msg_origin,
                                        "✅ B站扫码登录成功，Cookie已自动更新！",
                                    )
                                    logger.info("B站扫码登录成功，Cookie已更新")
                                else:
                                    await self.context.send_message(
                                        unified_msg_origin,
                                        "❌ 登录成功但未获取到有效Cookie，请在插件配置中手动填写",
                                    )
                                    logger.error("登录成功但Cookie提取失败")
                                return

                            if code == 86038:
                                await self.context.send_message(
                                    unified_msg_origin,
                                    "⏰ 二维码已过期，请重新发送 `更新cookie` 发起登录",
                                )
                                logger.info("登录二维码已过期")
                                return

                            # 86090: 已扫码未确认 / 86101: 未扫码，继续轮询
                    except Exception as e:
                        logger.error(f"轮询登录状态失败: {e}")
                        await asyncio.sleep(self.POLL_INTERVAL_SECONDS)
                        continue

            await self.context.send_message(
                unified_msg_origin,
                "⏰ 扫码登录超时，请重新发送 `更新cookie` 发起登录",
            )
            logger.info("扫码登录超时")

        except Exception as e:
            logger.error(f"扫码登录流程异常: {e}")
            try:
                await self.context.send_message(unified_msg_origin, f"❌ 扫码登录失败: {e}")
            except Exception:
                pass
        finally:
            self._login_in_progress = False

    @staticmethod
    def _parse_set_cookie(resp: aiohttp.ClientResponse) -> Dict[str, str]:
        """解析响应头中的 Set-Cookie"""
        cookies: Dict[str, str] = {}
        for set_cookie in resp.headers.getall("Set-Cookie", []):
            head = set_cookie.split(";", 1)[0]
            kv = head.split("=", 1)
            if len(kv) == 2:
                key = kv[0].strip()
                value = kv[1].strip()
                if key and value:
                    cookies[key] = value
        return cookies

    @classmethod
    def _extract_cookie(cls, resp: aiohttp.ClientResponse) -> str:
        """从登录响应中提取关键 Cookie"""
        cookies = cls._parse_set_cookie(resp)
        required_keys = ["SESSDATA", "bili_jct", "DedeUserID", "DedeUserID__ckMd5"]
        cookie_parts = [f"{key}={cookies[key]}" for key in required_keys if key in cookies]
        return "; ".join(cookie_parts)

    def is_login_in_progress(self) -> bool:
        """当前是否有扫码登录流程正在进行"""
        return self._login_in_progress
