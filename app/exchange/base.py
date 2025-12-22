from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

from app.data.models import Candle, Derivatives1H


class ExchangeClient(ABC):
    name: str

    @abstractmethod
    def ping(self) -> bool:
        """Return True if exchange is reachable and key endpoints work."""
        raise NotImplementedError

    @abstractmethod
    def fetch_ohlcv(self, symbol: str, interval: str, limit: int = 200) -> List[Candle]:
        raise NotImplementedError

    @abstractmethod
    def fetch_mark_price(self, symbol: str) -> Optional[float]:
        raise NotImplementedError

    @abstractmethod
    def fetch_spread_bps(self, symbol: str) -> Optional[float]:
        """Best-effort. Some futures APIs do not expose L1 easily without WS; return None if unavailable."""
        raise NotImplementedError

    @abstractmethod
    def fetch_derivatives_1h(self, symbol: str) -> Derivatives1H:
        raise NotImplementedError
