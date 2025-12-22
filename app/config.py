from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


def _getenv(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _split_csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    symbols: List[str]
    primary_exchange: str
    scan_interval_sec: int

    # Binance
    binance_api_key: str
    binance_api_secret: str

    # KuCoin
    kucoin_api_key: str
    kucoin_api_secret: str
    kucoin_api_passphrase: str

    # Telegram (Tầng sau mới dùng để gửi)
    telegram_bot_token: str
    telegram_channel_id: str
    telegram_dm_admin_ids: List[int]

    @staticmethod
    def load() -> "AppConfig":
        symbols = _split_csv(_getenv("SYMBOLS", "BTCUSDT,ETHUSDT"))
        dm_ids_raw = _getenv("TELEGRAM_DM_ADMIN_IDS", "")
        dm_ids = [int(x) for x in _split_csv(dm_ids_raw)] if dm_ids_raw.strip() else []

        return AppConfig(
            app_env=_getenv("APP_ENV", "dev"),
            symbols=symbols,
            primary_exchange=_getenv("PRIMARY_EXCHANGE", "binance").lower(),
            scan_interval_sec=int(_getenv("SCAN_INTERVAL_SEC", "900")),
            binance_api_key=_getenv("BINANCE_API_KEY", ""),
            binance_api_secret=_getenv("BINANCE_API_SECRET", ""),
            kucoin_api_key=_getenv("KUCOIN_API_KEY", ""),
            kucoin_api_secret=_getenv("KUCOIN_API_SECRET", ""),
            kucoin_api_passphrase=_getenv("KUCOIN_API_PASSPHRASE", ""),
            telegram_bot_token=_getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_channel_id=_getenv("TELEGRAM_CHANNEL_ID", ""),
            telegram_dm_admin_ids=dm_ids,
        )
