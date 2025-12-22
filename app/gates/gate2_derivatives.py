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
    funding_z = ctx.funding_z
    oi_delta_pct = ctx.oi_delta_pct
    oi_spike_z = ctx.oi_spike_z

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

    if crowded_ratio or extreme_funding or oi_spike:
        reason = "crowded_squeeze"
        if oi_spike:
            reason = "oi_spike"
        elif extreme_funding:
            reason = "funding_extreme"
        elif crowded_ratio:
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
