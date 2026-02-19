from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

from app.data.models import MarketSnapshot
from app.gates.gate1_htf import Gate1Result
from app.gates.gate2_derivatives import Gate2Result
from app.gates.gate3_structure import Gate3Result
from app.signals.planner import TradePlan


@dataclass(frozen=True)
class ScoreResult:
    passed: bool                 # eligible for downstream state machine / notify
    tier: str                    # "A" | "B" | "C" | "SKIP"
    risk_mult: float             # 1.0 (A), 0.5 (B), 0.0 (C/SKIP)
    score_0_100: int
    rr_tp2: float
    reasons: List[str]
    checks: Dict[str, Any]


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def score_signal_v1(
    snapshot: MarketSnapshot,
    g1: Gate1Result,
    g2: Gate2Result,
    g3: Gate3Result,
    *,
    plan: TradePlan,  # REQUIRED: no plan => no scoring (no RR proxy)
    only_trade_tiers: Tuple[str, ...] = ("A", "B"),
    a_score_min: int = 80,
    b_score_min: int = 60,
    a_rr_min: float = 3.0,
    b_rr_min: float = 2.0,
) -> ScoreResult:
    """
    Scoring v1 (strict):
      - Requires plan (RR is REAL). No RR proxy.
      - Fail-closed if any gate fails or Gate2 is alert_only.
      - Outputs tier + risk_mult + score + reasons/checks for journaling.
    """
    reasons: List[str] = []
    checks: Dict[str, Any] = {}

    # --- Hard eligibility (fail-closed) ---
    if not getattr(g1, "passed", False):
        return ScoreResult(False, "SKIP", 0.0, 0, 0.0, ["gate1_fail"], {"g1_reason": getattr(g1, "reason", None)})
    if (not getattr(g2, "passed", False)) or bool(getattr(g2, "alert_only", False)):
        return ScoreResult(
            False,
            "SKIP",
            0.0,
            0,
            0.0,
            ["gate2_not_trade_eligible"],
            {
                "g2_reason": getattr(g2, "reason", None),
                "g2_regime": getattr(g2, "regime", None),
                "alert_only": getattr(g2, "alert_only", False),
            },
        )
    if not getattr(g3, "passed", False):
        return ScoreResult(False, "SKIP", 0.0, 0, 0.0, ["gate3_fail"], {"g3_reason": getattr(g3, "reason", None)})

    # --- Plan requirements (no proxy) ---
    if plan is None:
        return ScoreResult(False, "SKIP", 0.0, 0, 0.0, ["plan_required_missing"], {})
    if not plan.rr_tp2 or plan.rr_tp2 <= 0:
        return ScoreResult(False, "SKIP", 0.0, 0, 0.0, ["plan_rr_missing"], {"rr_tp2": plan.rr_tp2})

    rr_tp2 = float(plan.rr_tp2)
    checks["rr_tp2"] = rr_tp2

    # --- Score components (0..100) ---
    score = 50  # base once gates pass + plan exists

    # 1) HTF location quality
    htf = getattr(g1, "htf", None)
    loc = getattr(htf, "location", None) if htf else None
    pos = getattr(htf, "pos_pct", None) if htf else None
    checks["htf_loc"] = loc
    checks["htf_pos_pct"] = pos
    if loc in ("discount", "premium"):
        score += 12
    else:
        score -= 8
        reasons.append("htf_location_not_extreme")

    # 2) Derivatives regime alignment
    regime = str(getattr(g2, "regime", "") or "")
    conf = str(getattr(g2, "confidence", "") or "")
    hint = str(getattr(g2, "directional_bias_hint", "") or "")
    checks["g2_regime"] = regime
    checks["g2_confidence"] = conf
    checks["g2_hint"] = hint
    if regime == "healthy_trend":
        score += 10
    elif regime == "crowded_squeeze":
        score += 4
        reasons.append("crowded_squeeze_regime")
    else:
        score -= 12
        reasons.append("derivatives_neutralish")

    if conf == "HIGH":
        score += 4
    elif conf == "LOW":
        score -= 4

    # 3) Structure quality
    struct = getattr(g3, "structure", None)
    bos = bool(getattr(struct, "bos", False))
    choch = bool(getattr(struct, "choch", False))
    checks["bos"] = bos
    checks["choch"] = choch
    if bos and not choch:
        score += 8
    elif choch and not bos:
        score += 10
    elif bos and choch:
        score += 6
        reasons.append("bos_and_choch_both_true")
    else:
        score -= 20
        reasons.append("no_break_flag")

    # 4) Zone quality (use Gate3 zone + plan meta if present)
    zone = getattr(g3, "zone", None)
    fill = getattr(zone, "fill_pct", None) if zone else None
    checks["zone_fill_pct"] = fill
    if fill is not None:
        if float(fill) <= 0.25:
            score += 10
        elif float(fill) <= 0.55:
            score += 5
        else:
            score -= 10
            reasons.append("zone_too_filled")

    # 5) RR to TP2 (REAL)
    if rr_tp2 >= a_rr_min:
        score += 12
    elif rr_tp2 >= b_rr_min:
        score += 6
    elif rr_tp2 >= 1.5:
        score -= 4
        reasons.append("rr_borderline")
    else:
        score -= 15
        reasons.append("rr_too_low")

    score_f = _clamp(float(score), 0.0, 100.0)
    score_i = int(round(score_f))
    checks["final_score"] = score_i

    # --- Tier decision ---
    tier = "C"
    risk_mult = 0.0
    if rr_tp2 >= a_rr_min and score_i >= a_score_min:
        tier = "A"
        risk_mult = 1.0
    elif rr_tp2 >= b_rr_min and score_i >= b_score_min:
        tier = "B"
        risk_mult = 0.5
    else:
        tier = "C"
        risk_mult = 0.0

    # Respect trade set
    passed = tier in set(only_trade_tiers)
    if not passed:
        reasons.append(f"tier_{tier}_not_in_trade_set")
        tier = "SKIP"
        risk_mult = 0.0

    checks["final_tier"] = tier
    return ScoreResult(passed, tier, risk_mult, score_i, rr_tp2, reasons, checks)

*** End Patch
