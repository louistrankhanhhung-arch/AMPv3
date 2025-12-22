from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
from data.models import Candle

@dataclass(frozen=True)
class HTFBias:
    bias: str                 # "up" | "down" | "range"
    location: str             # "discount" | "mid" | "premium"
    pos_pct: float            # 0..1
    range_high: float
    range_low: float
    ema20: Optional[float]
    ema50: Optional[float]
    ema50_slope: Optional[float]

def _ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def _ema_series(values: List[float], period: int, last_n: int = 3) -> List[float]:
    """Return last_n EMA values (approx) to estimate slope."""
    if len(values) < period + last_n:
        return []
    series = []
    # crude but stable: compute EMA iteratively and store tail
    k = 2 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
        series.append(ema)
    return series[-last_n:]

def compute_htf_bias(candles_4h: List[Candle], window: int = 60) -> Optional[HTFBias]:
    if len(candles_4h) < max(80, window):
        return None

    recent = candles_4h[-window:]
    highs = [c.h for c in recent]
    lows = [c.l for c in recent]
    closes = [c.c for c in recent]

    rh = max(highs)
    rl = min(lows)
    last_close = closes[-1]

    rng = rh - rl
    if rng <= 0:
        return None

    pos = (last_close - rl) / rng  # 0..1
    if pos <= 0.30:
        loc = "discount"
    elif pos >= 0.70:
        loc = "premium"
    else:
        loc = "mid"

    ema20 = _ema(closes[-80:], 20)
    ema50 = _ema(closes[-80:], 50)
    ema50_tail = _ema_series(closes[-120:], 50, last_n=3)
    ema50_slope = None
    if len(ema50_tail) >= 2:
        ema50_slope = ema50_tail[-1] - ema50_tail[0]

    # Trend clarity heuristic
    bias = "range"
    if ema20 is not None and ema50 is not None and ema50_slope is not None:
        if ema20 > ema50 and ema50_slope > 0:
            bias = "up"
        elif ema20 < ema50 and ema50_slope < 0:
            bias = "down"
        else:
            bias = "range"

    return HTFBias(
        bias=bias,
        location=loc,
        pos_pct=float(pos),
        range_high=float(rh),
        range_low=float(rl),
        ema20=ema20,
        ema50=ema50,
        ema50_slope=ema50_slope,
    )
