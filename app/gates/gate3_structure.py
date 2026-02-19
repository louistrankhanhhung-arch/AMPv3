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
    tp2_candidate: Optional[float]
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

def _confirm_mode(g2: Gate2Result) -> str:
    """
    Option 2A: Gate3 is a confirmation gate with mode by derivatives regime.
      - squeeze: g2.regime == crowded_squeeze (flush/reversal triggers)
      - trend: default (healthy_trend etc.)
    """
    regime = str(getattr(g2, "regime", "") or "").lower()
    if regime == "crowded_squeeze":
        return "squeeze"
    return "trend"

def _displacement_against_crowd_1h(candles_1h, ratio_skew: Optional[str], atr_mult: float = 0.8) -> bool:
    """
    Displacement on last 1H candle, with direction against the crowded side.
      - crowd LONG  -> bearish displacement (close < open)
      - crowd SHORT -> bullish displacement (close > open)
    """
    if not candles_1h or len(candles_1h) < 20:
        return False
    a = _atr(candles_1h, 14)
    if a is None or a <= 0:
        return False
    last = candles_1h[-1]
    body = abs(float(last.c) - float(last.o))
    if body < atr_mult * a:
        return False
    skew = (ratio_skew or "").upper()
    if skew == "LONG":
        return float(last.c) < float(last.o)
    if skew == "SHORT":
        return float(last.c) > float(last.o)
    return False

def _sweep_external_1h(candles_1h, level: Optional[float], side: str) -> bool:
    """
    External liquidity sweep heuristic on last closed 1H candle.
      side="ABOVE": wick above level then close back below
      side="BELOW": wick below level then close back above
    """
    if not candles_1h or level is None:
        return False
    lvl = float(level)
    last = candles_1h[-1]
    if side.upper() == "ABOVE":
        return float(last.h) > lvl and float(last.c) < lvl
    if side.upper() == "BELOW":
        return float(last.l) < lvl and float(last.c) > lvl
    return False

def _fractal_swings_generic(candles, left: int = 2, right: int = 2):
    """
    Generic fractal swings for any timeframe candles:
    - swing high if candle.h is max within [i-left, i+right]
    - swing low if candle.l is min within [i-left, i+right]
    Returns list of (idx, kind, price).
    """
    n = len(candles) if candles else 0
    if n < left + right + 8:
        return []
    swings = []
    for i in range(left, n - right):
        window = candles[i - left : i + right + 1]
        hi = candles[i].h
        lo = candles[i].l
        if hi == max(c.h for c in window):
            swings.append((i, "H", float(hi)))
        if lo == min(c.l for c in window):
            swings.append((i, "L", float(lo)))
    return swings


def _micro_confirm_15m(
    candles_15m,
    intent: str,
    lookback: int = 48,            # 12h (48 x 15m)
    min_break_atr_mult: float = 0.10,  # CHoCH break must exceed level by >= 0.10 * ATR(14)
) -> tuple[bool, str]:
    """
    Micro-confirm v0 (15m):
    LONG:
      1) Sweep: low pierces last swing low, but close reclaims above it.
      2) CHoCH: after sweep, close breaks above last swing high (with small ATR buffer).
    SHORT: symmetric.
    """
    if not candles_15m or len(candles_15m) < 80:
        return False, "insufficient_15m_candles"

    c = candles_15m[-lookback:] if len(candles_15m) > lookback else candles_15m
    atr15 = _atr(c, 14)
    buf = (atr15 * min_break_atr_mult) if (atr15 is not None and atr15 > 0) else 0.0

    swings = _fractal_swings_generic(c, left=2, right=2)
    highs = [(i, p) for (i, k, p) in swings if k == "H"]
    lows = [(i, p) for (i, k, p) in swings if k == "L"]
    if len(highs) < 2 or len(lows) < 2:
        return False, "insufficient_15m_swings"

    last_high_i, last_high = highs[-1]
    last_low_i, last_low = lows[-1]

    # Sweep scan: find first sweep event in the recent window, then validate CHoCH after it.
    sweep_idx = None
    if intent == "LONG":
        # Need a recent swing low (use last_low) and reclaim close
        for i in range(max(0, last_low_i), len(c)):
            if float(c[i].l) < last_low and float(c[i].c) > (last_low + buf):
                sweep_idx = i
                break
        if sweep_idx is None:
            return False, "no_sweep_15m"
        # CHoCH up: close breaks above last swing high after sweep
        # Recompute "relevant" swing high as the latest high BEFORE sweep
        prev_highs = [(i, p) for (i, p) in highs if i < sweep_idx]
        if not prev_highs:
            return False, "no_prev_swing_high"
        ref_high_i, ref_high = prev_highs[-1]
        for j in range(sweep_idx + 1, len(c)):
            if float(c[j].c) > (ref_high + buf):
                return True, "micro_sweep_choch_up"
        return False, "no_choch_15m"

    # SHORT
    for i in range(max(0, last_high_i), len(c)):
        if float(c[i].h) > last_high and float(c[i].c) < (last_high - buf):
            sweep_idx = i
            break
    if sweep_idx is None:
        return False, "no_sweep_15m"
    prev_lows = [(i, p) for (i, p) in lows if i < sweep_idx]
    if not prev_lows:
        return False, "no_prev_swing_low"
    ref_low_i, ref_low = prev_lows[-1]
    for j in range(sweep_idx + 1, len(c)):
        if float(c[j].c) < (ref_low - buf):
            return True, "micro_sweep_choch_down"
    return False, "no_choch_15m"

def _micro_confirm_pullback_break_15m(
    candles_15m,
    intent: str,
    zone: Zone,
    lookback: int = 64,               # 16h
    min_break_atr_mult: float = 0.10, # internal break buffer
    max_zone_fill_pct: float = 0.55,  # don't accept deep mitigation
    require_accept_closes: int = 2,   # NEW: consecutive closes for acceptance
    accept_lookahead: int = 16,       # NEW: how far after touch to look for acceptance (~4h)
    strong_disp: bool = False,        # NEW: hybrid relax acceptance if strong displacement
) -> tuple[bool, str]:
    """
    Micro-confirm mode 2 (continuation):
      Pullback into directional zone -> then break internal structure (15m).

    LONG:
      1) Price trades into zone (touch) without deep fill.
      2) After touch, close breaks above most recent swing high (internal).
    SHORT: symmetric.

    Notes:
      - This does NOT require sweep.
      - Uses fractal swings as internal proxy (v0).
    """
    if not candles_15m or len(candles_15m) < 120:
        return False, "insufficient_15m_candles"

    # Reject zones already too filled (continuation prefers cleaner zones)
    if float(getattr(zone, "fill_pct", 1.0)) > max_zone_fill_pct:
        return False, "zone_too_filled_for_continuation"

    c = candles_15m[-lookback:] if len(candles_15m) > lookback else candles_15m
    atr15 = _atr(c, 14)
    buf = (atr15 * min_break_atr_mult) if (atr15 is not None and atr15 > 0) else 0.0

    top = float(zone.top)
    bot = float(zone.bottom)
    if top < bot:
        top, bot = bot, top
    mid = (top + bot) / 2.0

    # Detect first touch into zone (mitigation)
    touch_idx = None
    if intent == "LONG":
        # touch if low enters the zone area
        for i in range(0, len(c)):
            if float(c[i].l) <= top and float(c[i].l) >= bot:
                touch_idx = i
                break
    else:
        for i in range(0, len(c)):
            if float(c[i].h) >= bot and float(c[i].h) <= top:
                touch_idx = i
                break

    if touch_idx is None:
        return False, "no_pullback_into_zone"

    # NEW: 2 closes acceptance after touch (avoid wick-only "touch giả")
    # LONG: require consecutive closes >= zone_mid
    # SHORT: require consecutive closes <= zone_mid
    acc_needed = int(require_accept_closes) if require_accept_closes is not None else 0
    # HYBRID: if strong displacement on 1H, accept faster (1 close)
    if strong_disp and acc_needed > 1:
        acc_needed = 1
    if acc_needed > 0:
        acc = 0
        acc_start = None
        end = min(len(c), touch_idx + 1 + int(accept_lookahead))
        for i in range(touch_idx + 1, end):
            cl = float(c[i].c)
            ok = (cl >= mid) if intent == "LONG" else (cl <= mid)
            if ok:
                acc += 1
                if acc_start is None:
                    acc_start = i
                if acc >= acc_needed:
                    # acceptance achieved; internal break should happen AFTER this point
                    touch_idx = i
                    break
            else:
                acc = 0
                acc_start = None
        if acc < acc_needed:
            return False, f"no_acceptance_{acc_needed}_closes"

    # Internal break after touch: use latest swing levels before touch as reference
    swings = _fractal_swings_generic(c, left=2, right=2)
    highs = [(i, p) for (i, k, p) in swings if k == "H"]
    lows = [(i, p) for (i, k, p) in swings if k == "L"]
    if len(highs) < 2 or len(lows) < 2:
        return False, "insufficient_15m_swings"

    prev_highs = [(i, p) for (i, p) in highs if i < touch_idx]
    prev_lows = [(i, p) for (i, p) in lows if i < touch_idx]
    if not prev_highs or not prev_lows:
        return False, "no_reference_swings"

    ref_high = prev_highs[-1][1]
    ref_low = prev_lows[-1][1]

    if intent == "LONG":
        # break internal high after touch
        for j in range(touch_idx + 1, len(c)):
            if float(c[j].c) > (ref_high + buf):
                return True, "micro_pullback_break_up"
        return False, "no_internal_break_up"
    else:
        # break internal low after touch
        for j in range(touch_idx + 1, len(c)):
            if float(c[j].c) < (ref_low - buf):
                return True, "micro_pullback_break_down"
        return False, "no_internal_break_down"


def _pick_micro_mode(g2: Gate2Result) -> str:
    """
    Decide micro-confirm mode based on derivatives directional hint.
    Expecting g2.directional_bias_hint like:
      - "...continuation..." or "...reversal..." or "no_trade"
    """
    hint = str(getattr(g2, "directional_bias_hint", "") or "").lower()
    if "continu" in hint:
        return "mode2"
    if "revers" in hint:
        return "mode1"
    return "mode1"

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

def _strong_displacement_1h(candles_1h, strong_mult: float = 1.2) -> bool:
    """
    Strong displacement proxy:
      last candle body >= strong_mult * ATR(14)
    Used to relax acceptance requirement (hybrid mode).
    """
    a = _atr(candles_1h, 14)
    if a is None or a <= 0:
        return False
    last = candles_1h[-1]
    body = abs(float(last.c) - float(last.o))
    return body >= strong_mult * a

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
        # LiquidityTargets uses `above/below`. Keep backward-compat if old field names exist.
        return getattr(liq, "above", None) or getattr(liq, "liq_above", None)
    return getattr(liq, "below", None) or getattr(liq, "liq_below", None)

def _liq_levels_from_gate1(g1: Gate1Result) -> tuple[Optional[float], Optional[float]]:
    """
    Return (liq_above, liq_below) from Gate1 in a backward-compatible way.
    """
    liq = getattr(g1, "liq", None)
    if liq is None:
        return None, None
    above = getattr(liq, "above", None) or getattr(liq, "liq_above", None)
    below = getattr(liq, "below", None) or getattr(liq, "liq_below", None)
    return above, below

def gate3_structure_confirmation_v0(
    snapshot: MarketSnapshot,
    g1: Gate1Result,
    g2: Gate2Result,
    min_rr_tp2: float = 2.5,
) -> Gate3Result:
    """
    Gate3 v0 (Option 2A): Confirmation gate (still no planning).

    Fail-closed rules:
    - Gate1 must pass
    - Gate2 must pass AND not alert_only
    - Mode-specific confirmation:
        * trend: require BOS/CHoCH + displacement (classic)
        * squeeze: require (CHoCH + displacement) OR (external sweep + displacement against crowd)
    - Must have a usable 15m FVG zone
    - (RR filter is moved to signals/planner.py; Gate3 only returns candidates)
    """
    structure = analyze_structure_1h(snapshot.candles_1h, close_confirm=True)

    # If Gate1 fails -> no trade
    if not g1.passed:
        return Gate3Result(
            passed=False,
            reason="gate1_fail",
            structure=structure,
            zone=None,
            tp2_candidate=None,
            notes={"hint": "shadow_mode_ok", "mode": "n/a"},
            intent=None,
        )

    # Gate2 not ready / alert-only -> do not produce trade-eligible pass
    if (not g2.passed) or getattr(g2, "alert_only", False):
        return Gate3Result(
            passed=False,
            reason="gate2_not_trade_eligible",
            structure=structure,
            zone=None,
            tp2_candidate=None,
            notes={
                "g2_regime": str(getattr(g2, "regime", None)),
                "g2_reason": str(getattr(g2, "reason", None)),
                "mode": "n/a",
            },
            intent=None,
        )

    mode = _confirm_mode(g2)

    # Practical intent from HTF bias/location (fail-closed for mid-range / range regimes)
    intent = _pick_intent(g1)
    if intent is None:
        return Gate3Result(
            passed=False,
            reason="no_clear_intent_htf",
            structure=structure,
            zone=None,
            tp2_candidate=None,
            notes={"bias": str(getattr(g1, "bias", None)), "loc": str(getattr(g1, "loc", None))},
            intent=None,
        )

    # --- Mode-specific confirmation (Gate3 = confirmation gate) ---
    trigger = "n/a"
    if mode == "trend":
        # Structure must show BOS/CHoCH (close-confirm)
        if not (structure.bos or structure.choch):
            return Gate3Result(
                passed=False,
                reason=f"struct_no_break_{structure.reason}",
                structure=structure,
                zone=None,
                tp2_candidate=None,
                notes={"trend": structure.trend, "mode": mode},
                intent=intent,
            )
        # Require displacement to reduce false breaks
        if not _has_displacement(snapshot.candles_1h, atr_mult=0.8):
            return Gate3Result(
                passed=False,
                reason="no_displacement_1h",
                structure=structure,
                zone=None,
                tp2_candidate=None,
                notes={"struct": structure.reason, "mode": mode},
                intent=intent,
            )
        trigger = "bos_or_choch+disp"
    else:
        # squeeze mode: accept flush/reversal triggers
        ratio_skew = str(getattr(g2, "ratio_skew", "") or "").upper()
        liq_above, liq_below = _liq_levels_from_gate1(g1)

        sweep_ok = False
        if ratio_skew == "LONG":
            sweep_ok = _sweep_external_1h(snapshot.candles_1h, liq_above, side="ABOVE")
        elif ratio_skew == "SHORT":
            sweep_ok = _sweep_external_1h(snapshot.candles_1h, liq_below, side="BELOW")

        disp_against = _displacement_against_crowd_1h(snapshot.candles_1h, ratio_skew, atr_mult=0.8)
        disp_any = _has_displacement(snapshot.candles_1h, atr_mult=0.8)

        # Accept:
        #  - CHoCH + displacement (classic reversal), OR
        #  - external sweep + displacement against crowd (flush impulse)
        if structure.choch and disp_any:
            trigger = "choch+disp"
        elif sweep_ok and disp_against:
            trigger = "sweep_external+disp_against_crowd"
        else:
            return Gate3Result(
                passed=False,
                reason="squeeze_no_trigger",
                structure=structure,
                zone=None,
                tp2_candidate=None,
                notes={
                    "mode": mode,
                    "ratio_skew": ratio_skew or "NONE",
                    "sweep_ok": str(bool(sweep_ok)),
                    "disp_against": str(bool(disp_against)),
                    "struct": str(structure.reason),
                },
                intent=intent,
            )

    # HYBRID flag: strong displacement => relax acceptance rule in mode2
    strong_disp = _strong_displacement_1h(snapshot.candles_1h, strong_mult=1.2)

    zones = find_fvg_15m(snapshot.candles_15m, lookback=120)
    # Directional zone filter: must match intent and not deeply filled
    zone = _pick_zone(zones, intent=intent)
    if zone is None:
        return Gate3Result(
            passed=False,
            reason="no_valid_zone_15m_directional",
            structure=structure,
            zone=None,
            tp2_candidate=None,
            notes={"zone_count": str(len(zones)), "intent": intent, "mode": mode, "trigger": trigger},
            intent=intent,
        )

    # Micro-confirm (15m): choose mode based on derivatives directional hint
    # squeeze mode forces reversal-friendly micro confirm (mode1)
    micro_mode = "mode1" if mode == "squeeze" else _pick_micro_mode(g2)
    if micro_mode == "mode2":
        micro_ok, micro_reason = _micro_confirm_pullback_break_15m(
            snapshot.candles_15m,
            intent=intent,
            zone=zone,
            lookback=64,
            min_break_atr_mult=0.10,
            strong_disp=strong_disp,
        )
    else:
        micro_ok, micro_reason = _micro_confirm_15m(
            snapshot.candles_15m,
            intent=intent,
            lookback=48,
            # slightly stricter in squeeze mode to reduce chop spam
            min_break_atr_mult=0.12 if mode == "squeeze" else 0.10,
        )
    if not micro_ok:
        return Gate3Result(
            passed=False,
            reason="micro_confirm_fail",
            structure=structure,
            zone=zone,
            tp2_candidate=None,
            notes={
                "micro_reason": micro_reason,
                "micro_mode": micro_mode,
                "intent": intent,
                "strong_disp": str(strong_disp),
                "mode": mode,
                "trigger": trigger,
            },
            intent=intent,
        )

    # Candidate context only (planner will compute entry/SL/TP ladder/RR)
    mark = float(getattr(snapshot, "mark", getattr(snapshot, "mark_price", 0.0)) or 0.0)
    top = float(zone.top)
    bot = float(zone.bottom)
    if top < bot:
        top, bot = bot, top
    tp2_candidate: Optional[float] = None
    if intent == "LONG":
        tp2_candidate = _tp2_from_gate1(g1, intent="LONG")
        if tp2_candidate is None:
            tp2_candidate = structure.last_swing_high.price if structure.last_swing_high else None
    else:
        tp2_candidate = _tp2_from_gate1(g1, intent="SHORT")
        if tp2_candidate is None:
            tp2_candidate = structure.last_swing_low.price if structure.last_swing_low else None

    # PASS
    return Gate3Result(
        passed=True,
        reason="pass",
        structure=structure,
        zone=zone,
        tp2_candidate=tp2_candidate,
        notes={
            "trend": structure.trend,
            "break_level": str(structure.break_level),
            "zone_kind": zone.kind,
            "zone_fill": f"{zone.fill_pct:.2f}",
            "intent": intent,
            "micro": micro_reason,
            "micro_mode": micro_mode,
            "strong_disp": str(strong_disp),
            "mark": str(mark),
            "zone_top": str(top),
            "zone_bot": str(bot),
            "min_rr_tp2_moved_to_planner": str(min_rr_tp2),
            "mode": mode,
            "trigger": trigger,
            "g2_regime": str(getattr(g2, "regime", None)),
            "g2_reason": str(getattr(g2, "reason", None)),
            "ratio_skew": str(getattr(g2, "ratio_skew", None)),
        },
        intent=intent,
    )
