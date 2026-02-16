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
    intent: Optional[str] = None  # "LONG" | "SHORT" | None


def _rr(entry: float, sl: float, tp: float) -> Optional[float]:
    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    return abs(tp - entry) / risk

def _atr(candles, n: int = 14) -> Optional[float]:
    if candles is None or len(candles) < n + 2:
        return None
    trs: List[float] = []
    for i in range(-n, 0):
        c = candles[i]
        p = candles[i - 1]
        tr = max(c.h - c.l, abs(c.h - p.c), abs(c.l - p.c))
        trs.append(float(tr))
    if not trs:
        return None
    return sum(trs) / len(trs)


def _pick_intent(g1: Gate1Result) -> Optional[str]:
    """
    Practical directional intent:
    - If HTF bias is up and price is in discount -> LONG
    - If HTF bias is down and price is in premium -> SHORT
    - Mid location => no intent (fail-closed), unless you explicitly want mid-range setups later
    """
    bias = getattr(g1, "bias", None)
    loc = getattr(g1, "loc", None)
    if loc == "mid":
        return None
    if bias == "up" and loc == "discount":
        return "LONG"
    if bias == "down" and loc == "premium":
        return "SHORT"
    # Range bias: keep fail-closed in v0.1 (avoid chop)
    return None


def _has_displacement(candles_1h, atr_mult: float = 0.8) -> bool:
    """
    Displacement proxy v0.1:
    last candle body >= atr_mult * ATR(14)
    """
    a = _atr(candles_1h, 14)
    if a is None or a <= 0:
        return False
    last = candles_1h[-1]
    body = abs(float(last.c) - float(last.o))
    return body >= atr_mult * a


def _pick_zone(zones: List[Zone], intent: str) -> Optional[Zone]:
    """
    Prefer directional FVG + not deeply filled.
    """
    if not zones:
        return None
    want = "FVG_BULL" if intent == "LONG" else "FVG_BEAR"
    # Filter: not deeply filled, must have some thickness
    filt = []
    for z in zones:
        height = abs(float(z.top) - float(z.bottom))
        if height <= 0:
            continue
        if z.fill_pct > 0.55:
            continue
        if z.kind != want:
            continue
        filt.append(z)
    if not filt:
        return None
    # zones already sorted by score/recency in zones.py; keep top
    return filt[0]


def _tp2_from_gate1(g1: Gate1Result, intent: str) -> Optional[float]:
    liq = getattr(g1, "liq", None)
    if liq is None:
        return None
    if intent == "LONG":
        return getattr(liq, "liq_above", None)
    return getattr(liq, "liq_below", None)


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
            intent=None,
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
            intent=None,
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
            intent=None,
        )

    # Practical intent from HTF bias/location (fail-closed for mid-range / range regimes)
    intent = _pick_intent(g1)
    if intent is None:
        return Gate3Result(
            passed=False,
            reason="no_clear_intent_htf",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"bias": str(getattr(g1, "bias", None)), "loc": str(getattr(g1, "loc", None))},
            intent=None,
        )

    # Require displacement on the breaking 1H candle to reduce false breaks
    if not _has_displacement(snapshot.candles_1h, atr_mult=0.8):
        return Gate3Result(
            passed=False,
            reason="no_displacement_1h",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"struct": structure.reason},
            intent=intent,
        )

    zones = find_fvg_15m(snapshot.candles_15m, lookback=120)
    # Directional zone filter: must match intent and not deeply filled
    zone = _pick_zone(zones, intent=intent)
    if zone is None:
        return Gate3Result(
            passed=False,
            reason="no_valid_zone_15m_directional",
            structure=structure,
            zone=None,
            rr_tp2=None,
            entry=None,
            sl=None,
            tp2=None,
            notes={"zone_count": str(len(zones)), "intent": intent},
            intent=intent,
        )

    # Entry: use zone edge closer to current price (more realistic than pure mid)
    mark = float(getattr(snapshot, "mark", getattr(snapshot, "mark_price", 0.0)) or 0.0)
    top = float(zone.top)
    bot = float(zone.bottom)
    if top < bot:
        top, bot = bot, top
    # For LONG: prefer entry near top of bullish FVG (mitigation), for SHORT: near bottom
    entry = top if intent == "LONG" else bot

    # SL buffer using ATR (more practical than zone height)
    a15 = _atr(snapshot.candles_15m, 14) or 0.0
    a1h = _atr(snapshot.candles_1h, 14) or 0.0
    a = max(a15, 0.5 * a1h)  # blended volatility proxy
    buf = max(1e-12, 0.35 * a)  # tune later

    # TP2 v0 approximation:
    # - If structure is up/bos: TP2 = last swing high
    # - If structure is down/bos: TP2 = last swing low
    # - If CHoCH: still target opposite swing as TP2 placeholder (you can refine)
    tp2: Optional[float] = None
    sl: Optional[float] = None

    if intent == "LONG":
        sl = bot - buf
        tp2 = _tp2_from_gate1(g1, intent="LONG")
        if tp2 is None:
            tp2 = structure.last_swing_high.price if structure.last_swing_high else None
    else:
        sl = top + buf
        tp2 = _tp2_from_gate1(g1, intent="SHORT")
        if tp2 is None:
            tp2 = structure.last_swing_low.price if structure.last_swing_low else None

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
            intent=intent,
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
            notes={"min_rr_tp2": str(min_rr_tp2), "intent": intent},
            intent=intent,
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
            "intent": intent,
        },
        intent=intent,
    )
