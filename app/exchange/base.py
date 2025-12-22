from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from app.data.models import Candle, Derivatives1H

class ExchangeClient(ABC):
    name: str

    @abstractmethod
    def ping(self) -> bool: ...

    @abstractmethod
    def fetch_ohlcv(self, symbol: str, interval: str, limit: int = 200) -> List[Candle]: ...

    @abstractmethod
    def fetch_mark_price(self, symbol: str) -> Optional[float]: ...

    @abstractmethod
    def fetch_spread_bps(self, symbol: str) -> Optional[float]: ...

    @abstractmethod
    def fetch_top_of_book(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Return (bid, ask) if available."""
        raise NotImplementedError

    @abstractmethod
    def fetch_derivatives_1h(self, symbol: str) -> Derivatives1H: ...
