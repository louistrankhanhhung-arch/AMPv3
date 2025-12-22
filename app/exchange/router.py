from __future__ import annotations

from app.config import AppConfig
from app.exchange.base import ExchangeClient
from app.exchange.binance_futures import BinanceFuturesClient
from app.exchange.kucoin_futures import KucoinFuturesClient


class ExchangeRouter:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.primary = self._build_client(cfg.primary_exchange)
        self.fallback = self._build_client("kucoin" if cfg.primary_exchange == "binance" else "binance")

    def _build_client(self, name: str) -> ExchangeClient:
        name = name.lower()
        if name == "binance":
            return BinanceFuturesClient(self.cfg)
        if name == "kucoin":
            return KucoinFuturesClient(self.cfg)
        raise ValueError(f"Unsupported exchange: {name}")

    def get_client(self) -> ExchangeClient:
        if self.primary.ping():
            return self.primary
        if self.fallback.ping():
            return self.fallback
        raise RuntimeError("No exchange available: both primary and fallback ping failed.")
