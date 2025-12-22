from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
from data.models import Candle

@dataclass(frozen=True)
class LiquidityTargets:
    above: Optional[float]   # nearest swing high above price
    below: Optional[float]   # nearest swing low below price
    swing_highs: List[float]
    swing_lows: List[float]

def _pivots(candles: List[Candle], left: int = 2, right: int = 2) -> Tuple[List[float], List[float]]:
    highs = []
    lows = []
    for i in range(left, len(candles) - right):
        h = candles[i].h
        l = candles[i].l
        if all(h > candles[i - k].h for k in range(1, left + 1)) and all(h >= candles[i + k].h for k in range(1, right + 1)):
            highs.append(h)
        if all(l < candles[i - k].l for k in range(1, left + 1)) and all(l <= candles[i + k].l for k in range(1, right + 1)):
            lows.append(l)
    return highs, lows

def compute_liquidity_targets(candles_4h: List[Candle], lookback: int = 80) -> LiquidityTargets:
    recent = candles_4h[-lookback:] if len(candles_4h) >= lookback else candles_4h
    swing_highs, swing_lows = _pivots(recent, left=2, right=2)
    last_price = recent[-1].c

    above = None
    below = None

    # nearest above/below
    for h in sorted(swing_highs):
        if h > last_price:
            above = h
            break
    for l in sorted(swing_lows, reverse=True):
        if l < last_price:
            below = l
            break

    return LiquidityTargets(
        above=above,
        below=below,
        swing_highs=swing_highs[-10:],  # keep tail
        swing_lows=swing_lows[-10:],
    )
