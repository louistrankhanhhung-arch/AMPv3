from __future__ import annotations

from app.data.cache import TTLCache
from app.data.models import Derivatives1H
from app.exchange.base import ExchangeClient


class DerivativesFetcher:
    def __init__(self, client: ExchangeClient, cache: TTLCache) -> None:
        self.client = client
        self.cache = cache

    def get_derivatives_1h(self, symbol: str, ttl_sec: int = 30) -> Derivatives1H:
        key = ("deriv_1h", self.client.name, symbol)
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        d = self.client.fetch_derivatives_1h(symbol=symbol)
        self.cache.set(key, d, ttl_sec=ttl_sec)
        return d
