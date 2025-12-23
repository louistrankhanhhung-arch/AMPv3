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
    ratio_long_pct: Optional[float]
    funding: Optional[float]
    funding_z: Optional[float]
    oi_delta_pct: Optional[float]
    oi_spike_z: Optional[float]


def gate2_derivatives_regime(snapshot: MarketSnapshot, ctx: Gate2DerivativesCtx) -> Gate2Result:
    """
    Gate 2 - Derivatives Regime (A-mode: strict, quality > quantity)
    Uses rolling stats (funding_z, oi_spike_z) + ratio_long_pct.
    """
    rlp = getattr(ctx.last, "ratio_long_pct", None)
    funding = ctx.last.funding_rate
    # --- Funding z-score (fix) ---
    # Prefer recomputing from rolling mean/std if available on ctx to avoid sign/abs bugs.
    # Falls back to ctx.funding_z if stats are not provided.
    funding_z = getattr(ctx, "funding_z", None)
    _mu = getattr(ctx, "funding_mean", None)
    _sd = getattr(ctx, "funding_std", None)
    if isinstance(funding, (int, float)) and isinstance(_mu, (int, float)) and isinstance(_sd, (int, float)):
        if float(_sd) > 0.0:
            funding_z = (float(funding) - float(_mu)) / float(_sd)
    oi_delta_pct = ctx.oi_delta_pct
    oi_spike_z = ctx.oi_spike_z

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
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
        )

    if not ctx.ready:
        return Gate2Result(
            passed=False,
            reason=f"insufficient_history_{ctx.history_len}",
            regime="neutral",
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
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

    # A-mode: crowded_squeeze must be 2/3 (ratio + funding + oi_spike)
    squeeze_hits = int(crowded_ratio) + int(extreme_funding) + int(oi_spike)
    if squeeze_hits >= 2:
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
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
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
        return Gate2Result(
            passed=True,
            reason="pass",
            regime="healthy_trend",
            ratio_long_pct=rlp,
            funding=funding,
            funding_z=funding_z,
            oi_delta_pct=oi_delta_pct,
            oi_spike_z=oi_spike_z,
        )

    return Gate2Result(
        passed=False,
        reason="neutral",
        regime="neutral",
        ratio_long_pct=rlp,
        funding=funding,
        funding_z=funding_z,
        oi_delta_pct=oi_delta_pct,
        oi_spike_z=oi_spike_z,
    )
