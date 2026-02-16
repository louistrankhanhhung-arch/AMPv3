from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict

from app.data.models import MarketSnapshot
from app.gates.gate1_htf import Gate1Result
from app.gates.gate2_derivatives import Gate2Result
from app.smc.structure_1h import analyze_structure_1h, Structure1HResult
from app.smc.zones import find_fvg_15m, Zone


@dataclass(frozen=True)
class Gate3Result:
    passed: bool
    reason: str
    structure: Structure1HResult
    zone: Optional[Zone]
    rr_tp2: Optional[float]
    entry: Optional[float]
    sl: Optional[float]
    tp2: Optional[float]
    notes: Dict[str, str]


def _rr(entry: float, sl: float, tp: float) -> Optional[float]:
    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    return abs(tp - entry) / risk


def gate3_structure_confirmation_v0(
    snapshot: MarketSnapshot,
    g1: Gate1Result,
    g2: Gate2Result,
    min_rr_tp2: float = 2.5,
) -> Gate3Result:
    """
    Gate3 v0: SMC structure + zone + RR sanity.

    Fail-closed rules:
    - Gate1 must pass
    - Gate2 must pass AND not alert_only
    - 1H structure must show BOS or CHoCH (close-confirm)
    - Must have a usable 15m FVG zone
    - RR to TP2 >= min_rr_tp2 (TP2 approximated via last swing in 1H)
    """
    structure = analyze_structure_1h(snapshot.candles_1h, close_confirm=True)

    # If Gate1 fails -> no trade
    if not g1.passed:
        return Gate3Result(
            passed=False,
            reason="gate1_fail",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"hint": "shadow_mode_ok"},
        )

    # Gate2 not ready / alert-only -> do not produce trade-eligible pass
    if (not g2.passed) or getattr(g2, "alert_only", False):
        return Gate3Result(
            passed=False,
            reason="gate2_not_trade_eligible",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"g2_regime": str(getattr(g2, "regime", None)), "g2_reason": str(getattr(g2, "reason", None))},
        )

    # Structure must show displacement break (BOS/CHoCH)
    if not (structure.bos or structure.choch):
        return Gate3Result(
            passed=False,
            reason=f"struct_no_break_{structure.reason}",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"trend": structure.trend},
        )

    zones = find_fvg_15m(snapshot.candles_15m, lookback=120)
    # Minimal zone filter: must exist and not deeply filled
    zone = next((z for z in zones if z.fill_pct <= 0.66), None)
    if zone is None:
        return Gate3Result(
            passed=False,
            reason="no_valid_zone_15m",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"zone_count": str(len(zones))},
        )

    # Entry/SL v0: use mid of zone and 1x zone height as buffer
    entry = (zone.top + zone.bottom) / 2.0
    z_h = max(1e-12, (zone.top - zone.bottom))

    # TP2 v0 approximation:
    # - If structure is up/bos: TP2 = last swing high
    # - If structure is down/bos: TP2 = last swing low
    # - If CHoCH: still target opposite swing as TP2 placeholder (you can refine)
    tp2: Optional[float] = None
    sl: Optional[float] = None

    if structure.trend in ("up", "range") and (structure.bos or structure.choch):
        # bullish intent => stop below zone
        sl = zone.bottom - 0.25 * z_h
        tp2 = structure.last_swing_high.price if structure.last_swing_high else None
    elif structure.trend == "down" and (structure.bos or structure.choch):
        # bearish intent => stop above zone
        sl = zone.top + 0.25 * z_h
        tp2 = structure.last_swing_low.price if structure.last_swing_low else None
    else:
        sl = zone.bottom - 0.25 * z_h
        tp2 = structure.last_swing_high.price if structure.last_swing_high else None

    if tp2 is None or sl is None:
        return Gate3Result(
            passed=False,
            reason="tp2_or_sl_missing",
            structure=structure,
            zone=zone,
            rr_tp2=None,
            entry=entry,
            sl=sl,
            tp2=tp2,
            notes={},
        )

    rr_tp2 = _rr(entry, sl, tp2)
    if rr_tp2 is None or rr_tp2 < min_rr_tp2:
        return Gate3Result(
            passed=False,
            reason="rr_too_low",
            structure=structure,
            zone=zone,
            rr_tp2=rr_tp2,
            entry=entry,
            sl=sl,
            tp2=tp2,
            notes={"min_rr_tp2": str(min_rr_tp2)},
        )

    # PASS
    return Gate3Result(
        passed=True,
        reason="pass",
        structure=structure,
        zone=zone,
        rr_tp2=rr_tp2,
        entry=entry,
        sl=sl,
        tp2=tp2,
        notes={
            "trend": structure.trend,
            "break_level": str(structure.break_level),
            "zone_kind": zone.kind,
            "zone_fill": f"{zone.fill_pct:.2f}",
        },
    )
