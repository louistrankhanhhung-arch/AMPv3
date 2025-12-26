from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.data.models import MarketSnapshot
from app.data.derivatives_fetcher import Gate2DerivativesCtx


@dataclass(frozen=True)
class Gate2Result:
    passed: bool
    reason: str
    regime: str  # "healthy_trend" | "crowded_squeeze" | "neutral"
    directional_bias_hint: str  # continuation/reversal preference hint (v1)
    confidence: str             # "HIGH"|"MED"|"LOW"
    confirm4h: bool
    confirm4h_reason: str
    ratio_skew: Optional[str]   # "LONG"|"SHORT"|None
    funding_extreme: bool
    oi_spike: bool
    ratio_long_pct: Optional[float]
    funding: Optional[float]
    funding_z: Optional[float]
    oi_delta_pct: Optional[float]
    oi_spike_z: Optional[float]
    oi_slope_4h_pct: Optional[float]

def _ratio_skew(rlp: Optional[float]) -> Optional[str]:
    if not isinstance(rlp, (int, float)):
        return None
    x = float(rlp)
    if x >= 65.0:
        return "LONG"
    if x <= 35.0:
        return "SHORT"
    return None


def _directional_hint(regime: str, skew: Optional[str]) -> str:
    # v1 hint without Gate1 HTF bias: keep generic but actionable
    if regime == "healthy_trend":
        return "continuation_preferred"
    if regime == "crowded_squeeze":
        if skew == "LONG":
            return "reversal_or_flush_risk"
        if skew == "SHORT":
            return "reversal_or_squeeze_up_risk"
        return "squeeze_risk"
    return "no_trade"

def gate2_derivatives_regime(snapshot: MarketSnapshot, ctx: Gate2DerivativesCtx) -> Gate2Result:
    """
    Gate 2 - Derivatives Regime (A-mode: strict, quality > quantity)
    Uses rolling stats (funding_z, oi_spike_z) + ratio_long_pct.
    """
    rlp = getattr(ctx.last, "ratio_long_pct", None)
    funding = ctx.last.funding_rate
    # --- Funding z-score (Option 2 fix) ---
    # Prefer recomputing from rolling mean/std on ctx; fallback to ctx.funding_z.
    funding_z = getattr(ctx, "funding_z", None)
    funding_z = getattr(ctx, "funding_z", None)
    _mu = getattr(ctx, "funding_mean", None)
    _sd = getattr(ctx, "funding_std", None)
    if isinstance(funding, (int, float)) and isinstance(_mu, (int, float)) and isinstance(_sd, (int, float)):
        if float(_sd) > 0.0:
            funding_z = (float(funding) - float(_mu)) / float(_sd)
    oi_delta_pct = ctx.oi_delta_pct
    oi_spike_z = ctx.oi_spike_z
    oi_slope_4h_pct = getattr(ctx, "oi_slope_4h_pct", None)
    confirm4h = bool(getattr(ctx, "confirm4h", False))
    confirm4h_reason = str(getattr(ctx, "confirm4h_reason", "na"))

    # --- A-mode hard guards (work even when rolling history is insufficient) ---
    # Goal: allow "crowded_squeeze" classification immediately if risk is obvious,
    # while keeping "healthy_trend" strict (requires rolling readiness).
    hard_crowded_ratio = False
    if isinstance(rlp, (int, float)):
        # stricter than soft crowding: extreme skew
        hard_crowded_ratio = float(rlp) >= 70.0 or float(rlp) <= 30.0

    hard_extreme_funding = False
    if isinstance(funding, (int, float)):
        # absolute funding guard (USD-M). Conservative defaults.
        # If you find it's too strict/loose, tune to 0.00015..0.00030.
        hard_extreme_funding = abs(float(funding)) >= 0.00020

    # Hard squeeze requires 2/3 (ratio + funding + oi_spike if available).
    hard_oi_spike = False
    if isinstance(oi_spike_z, (int, float)):
        hard_oi_spike = float(oi_spike_z) >= 3.0  # hard threshold

    ratio_skew = _ratio_skew(rlp)
    oi_spike = bool(hard_oi_spike)
    funding_extreme = bool(hard_extreme_funding)

    # confidence for hard-guard path
    confidence = "LOW"
    if ctx.ready:
        confidence = "MED"
        if isinstance(rlp, (int, float)) and isinstance(funding_z, (int, float)) and isinstance(oi_spike_z, (int, float)):
            confidence = "HIGH"
        if confirm4h and confidence != "HIGH":
            confidence = "MED"

    hard_hits = int(hard_crowded_ratio) + int(hard_extreme_funding) + int(hard_oi_spike)
    if hard_hits >= 2:
        # Provide the most informative reason for logs
        if hard_oi_spike and (hard_crowded_ratio or hard_extreme_funding):
            reason = "oi_spike_hard"
        elif hard_extreme_funding and hard_crowded_ratio:
            reason = "ratio_funding_hard"
        elif hard_crowded_ratio:
            reason = "ratio_crowded_hard"
        else:
            reason = "funding_extreme_hard"
        return Gate2Result(
            passed=True,
            reason=reason,
            regime="crowded_squeeze",
            directional_bias_hint=_directional_hint("crowded_squeeze", ratio_skew),
            confidence=confidence,
            confirm4h=confirm4h,
            confirm4h_reason=confirm4h_reason,
            ratio_skew=ratio_skew,
            funding_extreme=funding_extreme,
            oi_spike=oi_spike,
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            oi_slope_4h_pct=oi_slope_4h_pct,
        )

    if not ctx.ready:
        return Gate2Result(
            passed=False,
            reason=f"insufficient_history_{ctx.history_len}",
            regime="neutral",
            directional_bias_hint=_directional_hint("neutral", ratio_skew),
            confidence="LOW",
            confirm4h=confirm4h,
            confirm4h_reason=confirm4h_reason,
            ratio_skew=ratio_skew,
            funding_extreme=False,
            oi_spike=False,
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            oi_slope_4h_pct=oi_slope_4h_pct,
        )

    # --- Crowded / Squeeze risk (Regime B) ---
    crowded_ratio = False
    if isinstance(rlp, (int, float)):
        # A-mode: treat >=67.5% as crowded
        crowded_ratio = float(rlp) >= 67.5 or float(rlp) <= 32.5

    extreme_funding = False
    if isinstance(funding_z, (int, float)):
        extreme_funding = abs(float(funding_z)) >= 2.0
    else:
        # fallback: absolute funding guard (best-effort)
        if isinstance(funding, (int, float)):
            extreme_funding = abs(float(funding)) >= 0.00015

    oi_spike = False
    if isinstance(oi_spike_z, (int, float)):
        oi_spike = float(oi_spike_z) >= 2.5

    # Confidence upgrade when 4H confirm is present (v1)
    confidence = "MED"
    if isinstance(rlp, (int, float)) and isinstance(funding_z, (int, float)) and isinstance(oi_spike_z, (int, float)):
        confidence = "HIGH"
    if not confirm4h and confidence == "HIGH":
        confidence = "MED"

    # A-mode: crowded_squeeze must be 2/3 (ratio + funding + oi_spike)
    squeeze_hits = int(crowded_ratio) + int(extreme_funding) + int(oi_spike)
    if squeeze_hits >= 2 and (confirm4h or squeeze_hits == 3):
        # Prefer the most actionable reason for downstream logic/logging
        if oi_spike and extreme_funding:
            reason = "funding_extreme_oi_spike"
        elif oi_spike and crowded_ratio:
            reason = "ratio_crowded_oi_spike"
        elif extreme_funding and crowded_ratio:
            reason = "ratio_crowded_funding_extreme"
        elif oi_spike:
            reason = "oi_spike"
        elif extreme_funding:
            reason = "funding_extreme"
        else:
            reason = "ratio_crowded"
        return Gate2Result(
            passed=True,
            reason=reason,
            regime="crowded_squeeze",
            directional_bias_hint=_directional_hint("crowded_squeeze", ratio_skew),
            confidence=confidence,
            confirm4h=confirm4h,
            confirm4h_reason=confirm4h_reason,
            ratio_skew=ratio_skew,
            funding_extreme=bool(extreme_funding),
            oi_spike=bool(oi_spike),
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            oi_slope_4h_pct=oi_slope_4h_pct,
        )

    # --- Healthy Trend (Regime A) ---
    # Keep strict: requires rolling readiness AND no squeeze (above) AND conservative bands.
    ratio_ok = True
    if isinstance(rlp, (int, float)):
        ratio_ok = float(rlp) <= 65.0 and float(rlp) >= 35.0

    funding_ok = True
    if isinstance(funding_z, (int, float)):
        funding_ok = abs(float(funding_z)) <= 1.5
    else:
        if isinstance(funding, (int, float)):
            funding_ok = abs(float(funding)) <= 0.00010

    oi_ok = True
    if isinstance(oi_spike_z, (int, float)):
        oi_ok = float(oi_spike_z) < 2.0

    if ratio_ok and funding_ok and oi_ok:
        confidence = "MED"
        if isinstance(rlp, (int, float)) and isinstance(funding_z, (int, float)) and isinstance(oi_spike_z, (int, float)):
            confidence = "HIGH"
        return Gate2Result(
            passed=True,
            reason="pass",
            regime="healthy_trend",
            directional_bias_hint=_directional_hint("healthy_trend", ratio_skew),
            confidence=confidence,
            confirm4h=confirm4h,
            confirm4h_reason=confirm4h_reason,
            ratio_skew=ratio_skew,
            funding_extreme=bool(extreme_funding),
            oi_spike=bool(oi_spike),
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
            oi_slope_4h_pct=oi_slope_4h_pct,
        )

    return Gate2Result(
        passed=False,
        reason="neutral",
        regime="neutral",
        directional_bias_hint=_directional_hint("neutral", ratio_skew),
        confidence="MED" if ctx.ready else "LOW",
        confirm4h=confirm4h,
        confirm4h_reason=confirm4h_reason,
        ratio_skew=ratio_skew,
        funding_extreme=bool(extreme_funding),
        oi_spike=bool(oi_spike),
        ratio_long_pct=rlp,
        funding=funding,
        funding_z=funding_z,
        oi_delta_pct=oi_delta_pct,
        oi_spike_z=oi_spike_z,
        oi_slope_4h_pct=oi_slope_4h_pct,
    )
