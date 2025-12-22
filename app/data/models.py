from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict


@dataclass(frozen=True)
class Candle:
    ts: int          # epoch seconds
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass(frozen=True)
class Derivatives1H:
    funding_rate: Optional[float]          # per funding interval (normalized)
    open_interest: Optional[float]         # contracts or USD notionals (exchange dependent)
    long_short_ratio: Optional[float]      # % long or ratio (exchange dependent)
    meta: Dict[str, str]                   # keep raw fields / source notes


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    candles_15m: List[Candle]
    candles_1h: List[Candle]
    candles_4h: List[Candle]
    deriv_1h: Derivatives1H

    # Optional helpful proxies
    spread_bps: Optional[float] = None
    mark_price: Optional[float] = None
    last_updated_ts: Optional[int] = None
