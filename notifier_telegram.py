import os, uuid, logging, requests, time
from typing import Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from templates import render_teaser
from storage import JsonStore, SignalCache, SignalPerfDB
from config import BOT_TOKEN, CHANNEL_ID, TEASER_SHOW_BUTTON, TEASER_UPGRADE_BUTTON, DATA_DIR
# Optional: đặt CHANNEL_USERNAME trong .env nếu kênh có username công khai (@yourchannel)
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

log = logging.getLogger("notifier")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
SAFE_LIMIT = 3900  # chừa biên so với trần 4096 của Telegram

def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"Content-Type": "application/json"})
    return s

class TelegramNotifier:
    """Sync notifier using requests.Session (pool lớn + retry)."""

    def __init__(self):
        self.session = _build_session()
        self.cache = SignalCache(JsonStore(DATA_DIR))
        # Lấy username một lần
        try:
            r = self.session.get(f"{API_BASE}/getMe", timeout=10)
            r.raise_for_status()
            self.username = r.json().get("result", {}).get("username")
            if not self.username:
                raise RuntimeError("Bot username not found in getMe")
            log.info(f"TelegramNotifier ready as @{self.username} -> channel {CHANNEL_ID}")
        except Exception as e:
            raise RuntimeError(f"getMe failed: {e}") from e

    def post_teaser(self, plan: Dict[str, Any]) -> tuple[str, int]:
        if not self.username:
            raise RuntimeError("Bot username is not available")
        signal_id = str(uuid.uuid4())[:8]

        # Teaser cho channel + cache PLAN để DM bot render full có watermark
        teaser = render_teaser(plan)
        # Cache PLAN (bot sẽ render full + watermark từ plan)
        self.cache.put_plan(signal_id, plan)
        # Dùng https deep-link để client luôn gửi /start <payload>
        url_show = f"https://t.me/{self.username}?start=show_{signal_id}"
        url_upgr = f"https://t.me/{self.username}?start=upgrade"
        kb = {"inline_keyboard": [[{"text": TEASER_SHOW_BUTTON, "url": url_show}],
                                  [{"text": TEASER_UPGRADE_BUTTON, "url": url_upgr}]]}
        payload = {
            "chat_id": int(CHANNEL_ID),
            "text": teaser,
            "parse_mode": "HTML",
            "reply_markup": kb
        }
        try:
            r = self.session.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
            r.raise_for_status()
            msg_id = int(r.json()["result"]["message_id"])
        except requests.RequestException as e:
            log.warning("teaser post failed: %s", e)
            raise
        # Sau khi gửi thành công (ra khỏi try/except) mới log & return
        log.info(
            "Posted teaser signal_id=%s symbol=%s dir=%s",
            signal_id, plan.get("symbol"), plan.get("DIRECTION")
        )
        return signal_id, msg_id

    def send_channel(self, html: str):
        payload = {"chat_id": int(CHANNEL_ID), "text": html, "parse_mode": "HTML"}
        r = self.session.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
        r.raise_for_status()

    def _build_origin_link(self, origin_message_id: int) -> str:
        """
        Ưu tiên dùng username: https://t.me/<username>/<mid>
        Nếu không có username, dùng channel-id: https://t.me/c/<id_without_-100>/<mid>
        """
        if CHANNEL_USERNAME:
            return f"https://t.me/{CHANNEL_USERNAME}/{origin_message_id}"
        cid = str(CHANNEL_ID)
        cid_clean = cid[4:] if cid.startswith("-100") else cid
        return f"https://t.me/c/{cid_clean}/{origin_message_id}"

    def send_channel_update(self, origin_message_id: int, html: str, buttons: list | None = None):
        # Trả lời trực tiếp lên message gốc để Telegram hiện snapshot/quote
        payload = {
            "chat_id": int(CHANNEL_ID),
            "text": html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_to_message_id": int(origin_message_id),
            "allow_sending_without_reply": True
        }
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        r = self.session.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
        r.raise_for_status()
        return r.json()["result"]["message_id"]

    def send_dm(self, user_id: int, html: str):
        payload = {"chat_id": int(user_id), "text": html, "parse_mode": "HTML"}
        r = self.session.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
        r.raise_for_status()

    # NEW: gửi KPI 24H với nút nâng cấp
    def send_kpi24(self, html: str):
        url_upgr = f"https://t.me/{self.username}?start=upgrade"

        text_to_send = html
        kb_rows = []

        # Nếu KPI quá dài: cache full và rút gọn phần đăng kênh + nút Xem thêm
        if html and len(html) > SAFE_LIMIT:
            import uuid
            kpi_id = f"kpi_{str(uuid.uuid4())[:8]}"
            # Lưu toàn văn KPI vào cache để bot DM gửi lại khi user bấm "Xem thêm"
            self.cache.put_full(kpi_id, html)

            suffix = "\n\n<i>KPI dài — bấm “Xem thêm” để đọc đầy đủ.</i>"
            keep = SAFE_LIMIT - len(suffix) - 3  # trừ chỗ cho "…"
            keep = max(0, keep)
            text_to_send = (html[:keep].rstrip() + "…") + suffix

            url_more = f"https://t.me/{self.username}?start={kpi_id}"
            kb_rows.append([{"text": "📖 Xem thêm", "url": url_more}])

        # Hàng nút “Nâng cấp Plus” luôn có
        kb_rows.append([{"text": "✨ Nâng cấp Plus", "url": url_upgr}])
        kb = {"inline_keyboard": kb_rows}

        payload = {
            "chat_id": int(CHANNEL_ID),
            "text": text_to_send,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": kb
        }
        r = self.session.post(f"{API_BASE}/sendMessage", json=payload, timeout=15)
        try:
            r.raise_for_status()
        except requests.RequestException as e:
            # log description/response để debug (nếu vẫn còn lỗi 400 do ký tự)
            try:
                desc = r.json().get("description", "")
            except Exception:
                desc = r.text[:300]
            log.warning("KPI-24H send failed: %s | %s", e, desc)
            raise
        return r.json()["result"]["message_id"]
