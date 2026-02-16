from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from app.data.models import Candle


@dataclass(frozen=True)
class Zone:
    kind: str  # "FVG_BULL" | "FVG_BEAR"
    tf: str
    top: float
    bottom: float
    created_ts: int
    touched: bool
    fill_pct: float  # 0..1
    score: float
    reason: str


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def find_fvg_15m(candles_15m: List[Candle], lookback: int = 120) -> List[Zone]:
    """
    FVG v0:
    Bullish FVG when candle[i-1].h < candle[i+1].l (gap up)
    Bearish FVG when candle[i-1].l > candle[i+1].h (gap down)

    Score v0 = (1 - fill_pct) with small bonus if zone is recent.
    """
    if len(candles_15m) < 10:
        return []

    c = candles_15m[-lookback:] if len(candles_15m) > lookback else candles_15m
    zones: List[Zone] = []

    # i uses i-1 and i+1, so iterate 1..n-2
    for i in range(1, len(c) - 1):
        a = c[i - 1]
        b = c[i]
        d = c[i + 1]

        # Bull FVG: prior high < next low
        if a.h < d.l:
            top = d.l
            bottom = a.h
            zones.append(_zone_from_gap(kind="FVG_BULL", top=top, bottom=bottom, candles=c, created_ts=b.ts))
        # Bear FVG: prior low > next high
        if a.l > d.h:
            top = a.l
            bottom = d.h
            zones.append(_zone_from_gap(kind="FVG_BEAR", top=top, bottom=bottom, candles=c, created_ts=b.ts))

    # Prefer recent + higher score first
    zones.sort(key=lambda z: (z.score, z.created_ts), reverse=True)
    return zones


def _zone_from_gap(kind: str, top: float, bottom: float, candles: List[Candle], created_ts: int) -> Zone:
    """
    Determine touch/fill on subsequent candles after creation.
    Fill v0:
      - For bull zone: if price trades down into [bottom, top], compute min low depth.
      - For bear zone: if price trades up into [bottom, top], compute max high depth.
    """
    if top < bottom:
        top, bottom = bottom, top

    touched = False
    fill_pct = 0.0

    # Evaluate after created_ts
    post = [x for x in candles if x.ts >= created_ts]
    if len(post) < 3:
        return Zone(kind=kind, tf="15m", top=top, bottom=bottom, created_ts=created_ts, touched=False, fill_pct=0.0, score=0.0, reason="too_few_post_candles")

    height = max(1e-12, (top - bottom))
    if kind == "FVG_BULL":
        # Fill if lows go below top, depth measured toward bottom
        min_low = min(x.l for x in post)
        if min_low <= top:
            touched = True
            # depth is how far into the zone the price went: from top downwards
            depth = (top - max(min_low, bottom))
            fill_pct = _clamp01(depth / height)
    else:
        # Bear: fill if highs go above bottom, depth measured toward top
        max_high = max(x.h for x in post)
        if max_high >= bottom:
            touched = True
            depth = (min(max_high, top) - bottom)
            fill_pct = _clamp01(depth / height)

    # Score: prefer unfilled + recent
    unfilled = 1.0 - fill_pct
    recency_bonus = 0.1  # lightweight; you can refine later
    score = unfilled + recency_bonus

    reason = "fresh" if not touched else ("light_fill" if fill_pct <= 0.33 else ("mid_fill" if fill_pct <= 0.66 else "deep_fill"))
    return Zone(
        kind=kind,
        tf="15m",
        top=top,
        bottom=bottom,
        created_ts=created_ts,
        touched=touched,
        fill_pct=fill_pct,
        score=score,
        reason=reason,
    )
