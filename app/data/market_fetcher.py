from __future__ import annotations

import time
from typing import List

from app.data.cache import TTLCache
from app.data.models import Candle
from app.exchange.base import ExchangeClient


class MarketFetcher:
    def __init__(self, client: ExchangeClient, cache: TTLCache) -> None:
        self.client = client
        self.cache = cache

    def get_candles(self, symbol: str, interval: str, limit: int = 200, ttl_sec: int = 30) -> List[Candle]:
        key = ("ohlcv", self.client.name, symbol, interval, str(limit))
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        candles = self.client.fetch_ohlcv(symbol=symbol, interval=interval, limit=limit)
        self.cache.set(key, candles, ttl_sec=ttl_sec)
        return candles
