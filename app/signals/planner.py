from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple

from app.data.models import MarketSnapshot, Candle
from app.gates.gate1_htf import Gate1Result
from app.gates.gate2_derivatives import Gate2Result
from app.gates.gate3_structure import Gate3Result
from app.smc.zones import Zone


# -------------------------
# Data models (minimal + useful)
# -------------------------

@dataclass(frozen=True)
class TPLevel:
    name: str          # "TP1".."TP5"
    price: float
    reason: str


@dataclass(frozen=True)
class TradePlan:
    symbol: str
    exchange: Optional[str]
    intent: str                  # "LONG" | "SHORT"

    # Core levels
    entry1: float                # primary entry (usually zone mid)
    entry2: Optional[float]      # optional scale-in entry (edge of zone)
    sl: float
    sl_reason: str

    # Targets
    tps: List[TPLevel]           # TP1..TP5 (may be <5 depending on available liq)

    # Metrics
    rr_tp2: Optional[float]      # RR to TP2 using entry1
    rr_tp2_entry2: Optional[float]  # RR to TP2 using entry2 if present
    risk_per_unit: float         # |entry1 - sl|

    # Execution hints (for state machine)
    leeway_price: float
    leeway_reason: str

    # Metadata for logging/journal/scoring
    meta: Dict[str, Any]


# -------------------------
# Helpers
# -------------------------

def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _mark(snapshot: MarketSnapshot) -> Optional[float]:
    for k in ("mark_price", "mark", "last_price"):
        v = _safe_float(getattr(snapshot, k, None))
        if v is not None and v > 0:
            return v
    # fallback: last close 15m
    if snapshot.candles_15m:
        return float(snapshot.candles_15m[-1].c)
    return None


def _atr(candles: List[Candle], n: int = 14) -> Optional[float]:
    if not candles or len(candles) < n + 2:
        return None
    trs: List[float] = []
    for i in range(-n, 0):
        c = candles[i]
        p = candles[i - 1]
        tr = max(c.h - c.l, abs(c.h - p.c), abs(c.l - p.c))
        trs.append(float(tr))
    return (sum(trs) / len(trs)) if trs else None


def _rr(entry: float, sl: float, tp: float) -> Optional[float]:
    r = abs(entry - sl)
    if r <= 1e-12:
        return None
    return abs(tp - entry) / r


def _norm_zone(zone: Zone) -> Tuple[float, float, float]:
    top = float(zone.top)
    bot = float(zone.bottom)
    if top < bot:
        top, bot = bot, top
    mid = (top + bot) / 2.0
    return top, bot, mid


def _coin_group(symbol: str) -> str:
    """
    Keep consistent with Gate1 spread tiers.
    """
    sym = (symbol or "").upper()
    CORE = {"BTCUSDT", "ETHUSDT"}
    MAJORS = {"BNBUSDT", "SOLUSDT"}
    LOW_PRICE_ALTS = {"ARBUSDT", "NEARUSDT"}
    if sym in CORE:
        return "core"
    if sym in MAJORS:
        return "major"
    if sym in LOW_PRICE_ALTS:
        return "alt_low_price"
    return "alt"


def _leeway_from_atr(snapshot: MarketSnapshot) -> Tuple[float, str]:
    """
    Execution leeway for triggering EXEC signal:
      - core: tighter
      - alts: wider
    Uses ATR(15m) as main scale; fallback to mark*bp.
    """
    m = _mark(snapshot) or 0.0
    g = _coin_group(snapshot.symbol)
    atr15 = _atr(snapshot.candles_15m, 14)

    # multipliers tuned for "distance_to_entry <= leeway" logic
    if g == "core":
        mult = 0.10
        fb_bps = 3.0
    elif g == "major":
        mult = 0.14
        fb_bps = 5.0
    elif g == "alt_low_price":
        mult = 0.22
        fb_bps = 12.0
    else:
        mult = 0.18
        fb_bps = 10.0

    if isinstance(atr15, (int, float)) and atr15 > 0:
        return float(atr15) * float(mult), f"atr15_mult_{mult:.2f}"
    # fallback: mark * bps
    return (m * (fb_bps / 10000.0)) if m > 0 else 0.0, f"fallback_{fb_bps:.1f}bps"


def _next_liq_levels(levels: List[float], ref: float, intent: str, k: int = 3) -> List[float]:
    """
    Pick next k liquidity levels beyond a reference price.
    """
    out: List[float] = []
    if not levels:
        return out
    lv = [float(x) for x in levels if isinstance(x, (int, float))]
    lv = sorted(set(lv))
    if intent == "LONG":
        for x in lv:
            if x > ref:
                out.append(x)
            if len(out) >= k:
                break
    else:
        for x in reversed(lv):
            if x < ref:
                out.append(x)
            if len(out) >= k:
                break
    return out


# -------------------------
# Main planner
# -------------------------

def build_plan_v0(
    snapshot: MarketSnapshot,
    g1: Gate1Result,
    g2: Gate2Result,
    g3: Gate3Result,
    *,
    min_rr_tp2: float = 2.5,         # your A-mode baseline
    sl_pad_zone_mult: float = 0.15,  # padding beyond zone height
    sl_pad_atr_mult: float = 0.25,   # min padding based on ATR15
) -> Optional[TradePlan]:
    """
    Build TradePlan from Gate3 candidates:
      - Entry1 = zone mid
      - Entry2 = deeper edge (better RR, higher miss probability)
      - SL = beyond opposite edge + max(zone_pad, atr_pad)
      - TP ladder: TP1 internal swing, TP2 candidate, TP3..TP5 external liquidity ladder
      - Computes RR(TP2) with Entry1/Entry2

    Fail-closed:
      - Requires g3.passed, intent, zone, tp2_candidate
      - Requires RR(TP2) >= min_rr_tp2 on Entry1 OR Entry2 (prefer Entry1 in A-mode)
    """
    if not getattr(g3, "passed", False):
        return None
    intent = str(getattr(g3, "intent", "") or "")
    if intent not in ("LONG", "SHORT"):
        return None
    zone = getattr(g3, "zone", None)
    if zone is None:
        return None
    tp2 = _safe_float(getattr(g3, "tp2_candidate", None))
    if tp2 is None:
        return None

    top, bot, mid = _norm_zone(zone)
    m = _mark(snapshot)
    if m is None or m <= 0:
        return None

    # --- Entries ---
    entry1 = float(mid)
    # Entry2: more aggressive scaling at "better price"
    entry2 = float(bot) if intent == "LONG" else float(top)

    # --- SL ---
    zone_h = abs(top - bot)
    atr15 = _atr(snapshot.candles_15m, 14)
    pad_zone = zone_h * float(sl_pad_zone_mult)
    pad_atr = (float(atr15) * float(sl_pad_atr_mult)) if (atr15 is not None and atr15 > 0) else 0.0
    pad = max(pad_zone, pad_atr, 1e-12)
    if intent == "LONG":
        sl = float(bot) - pad
        sl_reason = f"below_zone_bot_pad=max(zone*{sl_pad_zone_mult:.2f},atr15*{sl_pad_atr_mult:.2f})"
    else:
        sl = float(top) + pad
        sl_reason = f"above_zone_top_pad=max(zone*{sl_pad_zone_mult:.2f},atr15*{sl_pad_atr_mult:.2f})"

    risk_per_unit = abs(entry1 - sl)
    if risk_per_unit <= 1e-12:
        return None

    # --- TP1 from 1H internal liquidity (structure swings) ---
    struct = getattr(g3, "structure", None)
    tp1: Optional[float] = None
    tp1_reason = "na"
    if struct is not None:
        # For LONG prefer last swing high above entry; for SHORT prefer last swing low below entry
        if intent == "LONG":
            swh = getattr(struct, "last_swing_high", None)
            if swh is not None and float(getattr(swh, "price", 0.0)) > entry1:
                tp1 = float(getattr(swh, "price"))
                tp1_reason = "1h_last_swing_high"
        else:
            swl = getattr(struct, "last_swing_low", None)
            if swl is not None and float(getattr(swl, "price", 0.0)) < entry1:
                tp1 = float(getattr(swl, "price"))
                tp1_reason = "1h_last_swing_low"

        # If not found, fallback to break_level if meaningful
        if tp1 is None:
            brk = _safe_float(getattr(struct, "break_level", None))
            if brk is not None:
                if (intent == "LONG" and brk > entry1) or (intent == "SHORT" and brk < entry1):
                    tp1 = float(brk)
                    tp1_reason = "1h_break_level"

    # Last fallback: TP1 = 1R (gives management logic something deterministic)
    if tp1 is None:
        tp1 = (entry1 + risk_per_unit) if intent == "LONG" else (entry1 - risk_per_unit)
        tp1_reason = "fallback_1R"

    # --- Ensure tp ordering sanity ---
    # For LONG: tp1 < tp2 ideally; for SHORT: tp1 > tp2 ideally.
    # If not, adjust tp1 to 1R to keep ladder monotonic.
    if intent == "LONG" and not (tp1 < tp2):
        tp1 = entry1 + risk_per_unit
        tp1_reason = "adjust_tp1_to_1R_for_monotonic"
    if intent == "SHORT" and not (tp1 > tp2):
        tp1 = entry1 - risk_per_unit
        tp1_reason = "adjust_tp1_to_1R_for_monotonic"

    # --- TP3..TP5 from 4H liquidity ladder (Gate1 LiquidityTargets) ---
    liq = getattr(g1, "liq", None)
    swing_highs = list(getattr(liq, "swing_highs", [])) if liq is not None else []
    swing_lows = list(getattr(liq, "swing_lows", [])) if liq is not None else []

    # If tp2 is already Gate1 'above/below', then ladder uses next swings beyond tp2.
    ladder_src = swing_highs if intent == "LONG" else swing_lows
    next_lv = _next_liq_levels(ladder_src, ref=float(tp2), intent=intent, k=3)

    tps: List[TPLevel] = [
        TPLevel("TP1", float(tp1), tp1_reason),
        TPLevel("TP2", float(tp2), "gate1_liq_or_struct_fallback"),
    ]
    for i, lvl in enumerate(next_lv, start=3):
        tps.append(TPLevel(f"TP{i}", float(lvl), "4h_swing_liq_ladder"))

    # If still fewer than 5 targets, extend with simple R-multiples (last resort)
    # (Keeps state machine management consistent even without many swings.)
    while len(tps) < 5:
        k = len(tps)  # 2->make TP3, etc.
        mult = 2.0 + (k - 2) * 1.0  # 2R, 3R, 4R...
        px = (entry1 + mult * risk_per_unit) if intent == "LONG" else (entry1 - mult * risk_per_unit)
        tps.append(TPLevel(f"TP{k+1}", float(px), f"fallback_{mult:.1f}R"))

    # --- RR computations (real RR) ---
    rr_tp2 = _rr(entry1, sl, float(tp2))
    rr_tp2_entry2 = _rr(entry2, sl, float(tp2)) if entry2 is not None else None

    # --- A-mode RR guard ---
    # Prefer entry1 RR, but allow entry2 to qualify (means you may miss trades if price doesn't fill deep).
    rr_ok_entry1 = (rr_tp2 is not None and rr_tp2 >= float(min_rr_tp2))
    rr_ok_entry2 = (rr_tp2_entry2 is not None and rr_tp2_entry2 >= float(min_rr_tp2))
    if not (rr_ok_entry1 or rr_ok_entry2):
        return None

    # --- leeway ---
    leeway_price, leeway_reason = _leeway_from_atr(snapshot)

    # --- meta ---
    meta: Dict[str, Any] = {
        "g2_regime": getattr(g2, "regime", None),
        "g2_hint": getattr(g2, "directional_bias_hint", None),
        "g2_conf": getattr(g2, "confidence", None),
        "zone_kind": getattr(zone, "kind", None),
        "zone_fill_pct": float(getattr(zone, "fill_pct", 1.0)),
        "zone_score": float(getattr(zone, "score", 0.0)),
        "zone_top": top,
        "zone_bot": bot,
        "zone_mid": mid,
        "mark": float(m),
        "atr15": float(atr15) if isinstance(atr15, (int, float)) else None,
        "coin_group": _coin_group(snapshot.symbol),
        "min_rr_tp2": float(min_rr_tp2),
        "rr_ok_entry1": rr_ok_entry1,
        "rr_ok_entry2": rr_ok_entry2,
    }

    return TradePlan(
        symbol=snapshot.symbol,
        exchange=getattr(snapshot.deriv_1h, "meta", {}).get("exchange") if getattr(snapshot, "deriv_1h", None) else None,
        intent=intent,
        entry1=float(entry1),
        entry2=float(entry2) if entry2 is not None else None,
        sl=float(sl),
        sl_reason=sl_reason,
        tps=tps[:5],
        rr_tp2=rr_tp2,
        rr_tp2_entry2=rr_tp2_entry2,
        risk_per_unit=float(risk_per_unit),
        leeway_price=float(leeway_price),
        leeway_reason=leeway_reason,
        meta=meta,
    )

** End Patch
