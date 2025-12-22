from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from data.models import MarketSnapshot
from smc.htf_bias import compute_htf_bias, HTFBias
from smc.liquidity import compute_liquidity_targets, LiquidityTargets

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

    # Rule: avoid mid-range
    if htf.location == "mid":
        return Gate1Result(False, "mid_range_4h", htf, liq)

    # Rule: need clarity: trend OR range extreme
    clarity_ok = (htf.bias in ("up", "down")) or (htf.bias == "range" and htf.location in ("discount", "premium"))
    if not clarity_ok:
        return Gate1Result(False, "no_clarity", htf, liq)

    # Rule: must have a liquidity target
    if liq.above is None and liq.below is None:
        return Gate1Result(False, "no_liquidity_target", htf, liq)

    return Gate1Result(True, "pass", htf, liq)
