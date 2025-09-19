import os

def _ints(csv: str):
    return [int(x) for x in csv.split(",") if x.strip()]

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment.")

CHANNEL_ID = int(os.getenv("TELEGRAM_CHANNEL_ID", "0"))
OWNER_IDS = _ints(os.getenv("OWNER_IDS", ""))

BANK_INFO = {
    "name": os.getenv("BANK_NAME", ""),
    "account_name": os.getenv("BANK_ACCOUNT_NAME", ""),
    "account_number": os.getenv("BANK_ACCOUNT_NUMBER", ""),
    "qr_image_path": os.getenv("BANK_QR_PATH", ""),
    "note_format": os.getenv("BANK_NOTE_FORMAT", "PLUS {username} {months}M"),
}

DATA_DIR = os.getenv("DATA_DIR", "./data")

TEASER_SHOW_BUTTON = os.getenv("TEASER_SHOW_BUTTON", "Xem full")
TEASER_UPGRADE_BUTTON = os.getenv("TEASER_UPGRADE_BUTTON", "Nâng cấp Plus")

PLAN_DEFAULT_MONTHS = int(os.getenv("PLAN_DEFAULT_MONTHS", "1"))
PROTECT_CONTENT = os.getenv("PROTECT_CONTENT", "1").lower() not in ("0", "false", "no")
WATERMARK = os.getenv("WATERMARK", "1").lower() not in ("0", "false", "no")
