from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from app.data.models import MarketSnapshot
from app.smc.htf_bias import compute_htf_bias, HTFBias
from app.smc.liquidity import compute_liquidity_targets, LiquidityTargets

@dataclass(frozen=True)
class Gate1Result:
    passed: bool
    reason: str
    htf: Optional[HTFBias]
    liq: Optional[LiquidityTargets]

def gate1_htf_clarity(snapshot: MarketSnapshot) -> Gate1Result:
    htf = compute_htf_bias(snapshot.candles_4h, window=60)
    if htf is None:
        return Gate1Result(False, "insufficient_4h_candles", None, None)

    liq = compute_liquidity_targets(snapshot.candles_4h, lookback=80)

    # Quality filter (A-mode): skip if spread is too wide (illiquid / wick-prone)
    sp = snapshot.spread_pct
    if sp is not None:
        sym = snapshot.symbol.upper()
        if sym in ("BTCUSDT", "ETHUSDT"):
            if sp > 0.01:
                return Gate1Result(False, "spread_too_wide_core", htf, liq)
        else:
            if sp > 0.05:
                return Gate1Result(False, "spread_too_wide_alt", htf, liq)

    # Crypto-friendly location rules:
    # - If HTF is range: require clear extremes (avoid wide mid).
    # - If HTF is trending: avoid only the "dead mid" band (narrow), allow edge zones.
    pos = float(getattr(htf, "pos_pct", 0.5))

    RANGE_EXTREME = 0.30
    TREND_DEAD_MID_LOW = 0.42
    TREND_DEAD_MID_HIGH = 0.58

    if htf.bias == "range":
        if not (pos <= RANGE_EXTREME or pos >= (1.0 - RANGE_EXTREME)):
            return Gate1Result(False, "mid_range_4h_range_regime", htf, liq)
    else:
        # up/down trend regime
        if TREND_DEAD_MID_LOW < pos < TREND_DEAD_MID_HIGH:
            return Gate1Result(False, "mid_range_4h_trend_dead_mid", htf, liq)

    # Rule: need clarity: trend OR range extreme
    clarity_ok = (htf.bias in ("up", "down")) or (htf.bias == "range" and htf.location in ("discount", "premium"))
    if not clarity_ok:
        return Gate1Result(False, "no_clarity", htf, liq)

    # Rule: must have a liquidity target
    if liq.above is None and liq.below is None:
        return Gate1Result(False, "no_liquidity_target", htf, liq)

    return Gate1Result(True, "pass", htf, liq)
