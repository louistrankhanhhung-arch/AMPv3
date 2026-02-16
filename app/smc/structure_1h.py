from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from app.data.models import Candle


@dataclass(frozen=True)
class SwingPoint:
    ts: int
    price: float
    kind: str  # "H" or "L"


@dataclass(frozen=True)
class Structure1HResult:
    trend: str  # "up" | "down" | "range" | "unknown"
    last_swing_high: Optional[SwingPoint]
    last_swing_low: Optional[SwingPoint]
    bos: bool
    choch: bool
    break_level: Optional[float]
    reason: str


def _fractal_swings(candles: List[Candle], left: int = 2, right: int = 2) -> List[SwingPoint]:
    """
    Simple fractal swings:
    - swing high if candle.h is max within [i-left, i+right]
    - swing low if candle.l is min within [i-left, i+right]
    """
    n = len(candles)
    if n < left + right + 5:
        return []

    swings: List[SwingPoint] = []
    for i in range(left, n - right):
        window = candles[i - left : i + right + 1]
        hi = candles[i].h
        lo = candles[i].l
        if hi == max(c.h for c in window):
            swings.append(SwingPoint(ts=candles[i].ts, price=hi, kind="H"))
        if lo == min(c.l for c in window):
            swings.append(SwingPoint(ts=candles[i].ts, price=lo, kind="L"))

    # De-dup: keep last occurrence per timestamp/kind if any duplicates (rare)
    out: List[SwingPoint] = []
    seen = set()
    for s in swings:
        k = (s.ts, s.kind)
        if k not in seen:
            out.append(s)
            seen.add(k)
    return out


def _infer_trend(swings: List[SwingPoint]) -> str:
    """
    Very lightweight trend inference:
    - up if last 2 swing highs and lows are rising
    - down if last 2 swing highs and lows are falling
    - else range/unknown
    """
    highs = [s for s in swings if s.kind == "H"]
    lows = [s for s in swings if s.kind == "L"]
    if len(highs) < 2 or len(lows) < 2:
        return "unknown"

    h1, h2 = highs[-2], highs[-1]
    l1, l2 = lows[-2], lows[-1]

    if h2.price > h1.price and l2.price > l1.price:
        return "up"
    if h2.price < h1.price and l2.price < l1.price:
        return "down"
    return "range"


def analyze_structure_1h(
    candles_1h: List[Candle],
    close_confirm: bool = True,
    left: int = 2,
    right: int = 2,
) -> Structure1HResult:
    """
    Detect BOS/CHoCH v0:
    - Determine trend from swings
    - BOS: break in direction of trend (close beyond last swing)
    - CHoCH: break against trend (close beyond opposite swing)
    """
    if len(candles_1h) < 30:
        return Structure1HResult(
            trend="unknown",
            last_swing_high=None,
            last_swing_low=None,
            bos=False,
            choch=False,
            break_level=None,
            reason="insufficient_1h_candles",
        )

    swings = _fractal_swings(candles_1h, left=left, right=right)
    if len(swings) < 6:
        return Structure1HResult(
            trend="unknown",
            last_swing_high=None,
            last_swing_low=None,
            bos=False,
            choch=False,
            break_level=None,
            reason="insufficient_swings",
        )

    trend = _infer_trend(swings)
    last_high = next((s for s in reversed(swings) if s.kind == "H"), None)
    last_low = next((s for s in reversed(swings) if s.kind == "L"), None)

    if last_high is None or last_low is None:
        return Structure1HResult(
            trend="unknown",
            last_swing_high=last_high,
            last_swing_low=last_low,
            bos=False,
            choch=False,
            break_level=None,
            reason="missing_last_swing",
        )

    last = candles_1h[-1]
    px = last.c if close_confirm else last.h  # close confirm is safer
    bos = False
    choch = False
    brk: Optional[float] = None

    if trend == "up":
        if px > last_high.price:
            bos = True
            brk = last_high.price
            reason = "bos_up_close_break"
        elif px < last_low.price:
            choch = True
            brk = last_low.price
            reason = "choch_down_close_break"
        else:
            reason = "no_break"
    elif trend == "down":
        if px < last_low.price:
            bos = True
            brk = last_low.price
            reason = "bos_down_close_break"
        elif px > last_high.price:
            choch = True
            brk = last_high.price
            reason = "choch_up_close_break"
        else:
            reason = "no_break"
    else:
        reason = "range_no_signal"

    return Structure1HResult(
        trend=trend,
        last_swing_high=last_high,
        last_swing_low=last_low,
        bos=bos,
        choch=choch,
        break_level=brk,
        reason=reason,
    )
