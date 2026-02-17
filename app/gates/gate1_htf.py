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

    # --- Compatibility shims for downstream gates (Gate3 v0.x) ---
    # Gate3 currently expects g1.bias / g1.loc / g1.pos_pct.
    # Keep these as read-only properties to avoid changing all call-sites.
    @property
    def bias(self) -> Optional[str]:
        return getattr(self.htf, "bias", None) if self.htf is not None else None

    @property
    def loc(self) -> Optional[str]:
        return getattr(self.htf, "location", None) if self.htf is not None else None

    @property
    def pos_pct(self) -> Optional[float]:
        return getattr(self.htf, "pos_pct", None) if self.htf is not None else None

def gate1_htf_clarity(snapshot: MarketSnapshot) -> Gate1Result:
    htf = compute_htf_bias(snapshot.candles_4h, window=60)
    if htf is None:
        return Gate1Result(False, "insufficient_4h_candles", None, None)

    liq = compute_liquidity_targets(snapshot.candles_4h, lookback=80)

    # Quality filter (A-mode): skip if spread is too wide (illiquid / wick-prone)
    sp = snapshot.spread_pct
    if sp is not None:
        sym = snapshot.symbol.upper()
        # NOTE: spread_pct is in percent units (e.g., 0.10 = 0.10%).
        CORE = {"BTCUSDT", "ETHUSDT"}
        MAJORS = {"BNBUSDT", "SOLUSDT"}
        LOW_PRICE_ALTS = {"ARBUSDT", "NEARUSDT"}  # more wick-prone, allow a bit wider

        if sym in CORE:
            max_sp = 0.02   # 0.02% (2 bps)
            tag = "core"
        elif sym in MAJORS:
            max_sp = 0.06   # 0.06% (6 bps)
            tag = "major"
        elif sym in LOW_PRICE_ALTS:
            max_sp = 0.25   # 0.25% (25 bps)
            tag = "alt_low_price"
        else:
            max_sp = 0.15   # 0.15% (15 bps) default for mid alts
            tag = "alt"

        if sp > max_sp:
            return Gate1Result(False, f"spread_too_wide_{tag}", htf, liq)

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
