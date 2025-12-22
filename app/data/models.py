# data/models.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict

@dataclass(frozen=True)
class Candle:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

@dataclass(frozen=True)
class Derivatives1H:
    funding_rate: Optional[float]
    open_interest: Optional[float]
    long_short_ratio: Optional[float]      # raw from exchange (may be 0-1 or 0-100 or L/S ratio)
    ratio_long_pct: Optional[float]        # normalized: 0-100 (% long)
    meta: Dict[str, str]

@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    candles_15m: List[Candle]
    candles_1h: List[Candle]
    candles_4h: List[Candle]
    deriv_1h: Derivatives1H
    spread_bps: Optional[float] = None
    spread_pct: Optional[float] = None     # normalized % spread
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark_price: Optional[float] = None
    last_updated_ts: Optional[int] = None
