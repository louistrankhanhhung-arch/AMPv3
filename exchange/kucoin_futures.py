from __future__ import annotations

import requests
from typing import List, Optional, Dict

from app.config import AppConfig
from app.data.models import Candle, Derivatives1H
from app.exchange.base import ExchangeClient


class KucoinFuturesClient(ExchangeClient):
    name = "kucoin"

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.base = "https://api-futures.kucoin.com"

    def ping(self) -> bool:
        try:
            r = requests.get(f"{self.base}/api/v1/timestamp", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def fetch_ohlcv(self, symbol: str, interval: str, limit: int = 200) -> List[Candle]:
        """
        KuCoin futures symbols usually like: XBTUSDTM, ETHUSDTM...
        Bạn sẽ cần mapping symbol ở tầng 2 nếu muốn dùng KuCoin làm primary.
        Tầng 1: chỉ fallback, nên nếu symbol không mapping được -> raise để router quay lại Binance.
        """
        raise NotImplementedError("KuCoin OHLCV mapping not implemented in Tầng 1.")

    def fetch_mark_price(self, symbol: str) -> Optional[float]:
        # Best-effort; if not available return None
        return None

    def fetch_spread_bps(self, symbol: str) -> Optional[float]:
        return None

    def fetch_derivatives_1h(self, symbol: str) -> Derivatives1H:
        # Best-effort: return empty placeholders; later you'll implement real endpoints + auth if needed.
        return Derivatives1H(
            funding_rate=None,
            open_interest=None,
            long_short_ratio=None,
            meta={"source": "kucoin", "note": "fallback placeholders in Tầng 1"},
        )
