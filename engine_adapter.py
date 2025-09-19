
"""
engine_adapter.py
- Thin wrapper so main.py can call decide(...) without decision_engine.py.
- Uses tiny_core_side_state to compute decision and formats a legacy-compatible dict.
"""
from typing import Dict, Any, List, Optional, Iterable
from tiny_core_side_state import SideCfg, run_side_state_core
import os

def _last_closed_bar(df):
    """
    Return the last *closed* bar for streaming safety:
    use df.iloc[-2] if available, else the last row.
    """
    try:
        n = len(df)
        if n >= 2:
            return df.iloc[-2]
        elif n == 1:
            return df.iloc[-1]
    except Exception:
        pass
    return None

def _atr_from_features_tf(features_by_tf: Dict[str, Any], tf: str = "4H") -> float:
    """Use ATR at the last *closed* bar to avoid partial-candle drift."""
    try:
        df = (features_by_tf or {}).get(tf, {}).get("df")
        if df is not None and len(df) > 0:
            last = _last_closed_bar(df)
            if last is not None:
                # read ATR value at the same index as the closed bar
                return float(df.loc[last.name, "atr14"])
    except Exception:
        pass
    return 0.0

def _soft_levels_by_tf(features_by_tf: Dict[str, Any], tf: str = "4H") -> Dict[str, float]:
    """
    Lấy các mức mềm ở nến đã đóng gần nhất của TF chỉ định: BB upper/mid/lower, EMA20/50, Close.
    """
    out = {}
    try:
        df = (features_by_tf or {}).get(tf, {}).get("df")
        if df is not None and len(df) > 0:
            last = _last_closed_bar(df)
            if last is None:
                return out
            for k in ("bb_upper","bb_mid","bb_lower","ema20","ema50","close"):
                if k in last and last[k] == last[k]:  # not NaN
                    out[k] = float(last[k])
    except Exception:
        pass
    return out

def _rsi_from_features_tf(features_by_tf: Dict[str, Any], tf: str = "1H") -> Optional[float]:
    """
    Lấy RSI (mặc định cột rsi14) tại nến *đã đóng* gần nhất của TF cho trước.
    """
    try:
        df = (features_by_tf or {}).get(tf, {}).get("df")
        if df is not None and len(df) > 0:
            last = _last_closed_bar(df)
            if last is not None:
                for col in ("rsi14", "rsi", "RSI"):
                    if col in df.columns:
                        return float(df.loc[last.name, col])
    except Exception:
        pass
    return None

def _guard_near_bb_low_4h_and_rsi1h_extreme(side: Optional[str], entry: Optional[float], feats: Dict[str, Any]) -> Dict[str, Any]:
    """
    WAIT guard khi:
      - entry đang 'ôm' BB-lower 4H (<= 0.30 * ATR_4H)
      - VÀ RSI(1H) đang cực trị (<=20 hoặc >=80)
    Áp dụng cả long/short. Tránh ENTER kể cả khi state=trend_break.
    """
    try:
        if side not in ("long","short") or entry is None:
            return {"block": False, "why": ""}
        atr4 = _atr_from_features_tf(feats, "4H")
        if atr4 <= 0:
            return {"block": False, "why": ""}
        lv = _soft_levels_by_tf(feats, "4H")
        bb_l = lv.get("bb_lower")
        if bb_l is None:
            return {"block": False, "why": ""}
        rsi1 = _rsi_from_features_tf(feats, "1H")
        if rsi1 is None:
            return {"block": False, "why": ""}
        thr = 0.30 * atr4
        near_bb_low = (entry >= bb_l) and (abs(entry - bb_l) <= thr)
        rsi_extreme = (rsi1 <= 20.0) or (rsi1 >= 80.0)
        if near_bb_low and rsi_extreme:
            return {"block": True, "why": f"4H_bb_low±{thr:.4f} & RSI1H={rsi1:.1f}"}
    except Exception:
        pass
    return {"block": False, "why": ""}

def _near_soft_level_guard_multi(
    side: Optional[str],
    entry: Optional[float],
    feats: Dict[str, Any],
    tfs: Iterable[str] = ("4H",),
) -> Dict[str, Any]:
    """
    Trả về {"block": bool, "why": str} nếu entry quá gần BB/EMA (1H/4H…) theo hướng giao dịch.
    Quy tắc (mặc định):
      - BB upper/lower: <= 0.30*ATR
      - EMA20/EMA50/BB mid: <= 0.25*ATR
    """
    if side not in ("long","short") or entry is None:
        return {"block": False, "why": ""}
    reasons: List[str] = []
    for tf in tfs:
        atr = _atr_from_features_tf(feats, tf)
        if atr <= 0:
            continue
        lv = _soft_levels_by_tf(feats, tf)
        if not lv:
            continue
        bb_u, bb_m, bb_l = lv.get("bb_upper"), lv.get("bb_mid"), lv.get("bb_lower")
        e20, e50 = lv.get("ema20"), lv.get("ema50")
        thr_band   = 0.30 * atr
        thr_center = 0.25 * atr
        def _dist(a, b):
            try: return abs(float(a) - float(b))
            except Exception: return float("inf")
        if side == "long":
            if bb_u is not None and entry <= bb_u and _dist(entry, bb_u) <= thr_band:
                reasons.append(f"{tf}:near_BB_upper(<= {thr_band:.4f})")
            for nm, lvl in (("EMA20", e20), ("EMA50", e50), ("BB_mid", bb_m)):
                if lvl is not None and entry >= lvl and _dist(entry, lvl) <= thr_center:
                    reasons.append(f"{tf}:near_{nm}(<= {thr_center:.4f})")
        else:
            if bb_l is not None and entry >= bb_l and _dist(entry, bb_l) <= thr_band:
                reasons.append(f"{tf}:near_BB_lower(<= {thr_band:.4f})")
            for nm, lvl in (("EMA20", e20), ("EMA50", e50), ("BB_mid", bb_m)):
                if lvl is not None and entry <= lvl and _dist(entry, lvl) <= thr_center:
                    reasons.append(f"{tf}:near_{nm}(<= {thr_center:.4f})")
    if reasons:
        return {"block": True, "why": ";".join(reasons)}
    return {"block": False, "why": ""}

def _rr(entry: Optional[float], sl: Optional[float], tp: Optional[float], side: Optional[str]) -> Optional[float]:
    if entry is None or sl is None or tp is None or side is None:
        return None
    if side == "long":
        risk = entry - sl
        reward = tp - entry
    else:
        risk = sl - entry
        reward = entry - tp
    if risk <= 0:
        return None
    return reward / risk

def _leverage_hint(side: Optional[str], entry: Optional[float], sl: Optional[float]) -> Optional[float]:
    """
    Leverage tối ưu để rủi ro thực ~ risk_pct (ENV RISK_PCT, mặc định 5%).
    """
    try:
        if side not in ("long","short") or entry is None or sl is None or entry <= 0:
            return None
        risk_raw = abs((entry - sl) / entry)
        if risk_raw <= 0:
            return None
        risk_pct = float(os.getenv("RISK_PCT", "0.05"))
        lev = risk_pct / risk_raw
        lev_min = float(os.getenv("LEVERAGE_MIN", "1.0"))
        lev_max = float(os.getenv("LEVERAGE_MAX", "5.0"))
        return float(max(lev_min, min(lev, lev_max)))
    except Exception:
        return None

def decide(symbol: str, timeframe: str, features_by_tf: Dict[str, Dict[str, Any]], evidence_bundle: Dict[str, Any]) -> Dict[str, Any]:
    # evidence_bundle expected to include 'evidence' object; pass through as eb-like
    eb = evidence_bundle.get("evidence") or evidence_bundle  # tolerate both shapes
    cfg = SideCfg()
    dec = run_side_state_core(features_by_tf, eb, cfg)

    # Build plan (legacy fields)
    tps = dec.setup.tps or []
    tp1 = tps[0] if len(tps) > 0 else None
    tp2 = tps[1] if len(tps) > 1 else None
    tp3 = tps[2] if len(tps) > 2 else None

    # RR calculations
    rr1 = _rr(dec.setup.entry, dec.setup.sl, tp1, dec.side)
    rr2 = _rr(dec.setup.entry, dec.setup.sl, tp2, dec.side)
    rr3 = _rr(dec.setup.entry, dec.setup.sl, tp3, dec.side)

    # -------- SOFT PROXIMITY GUARD (BB/EMA) --------
    prox = _near_soft_level_guard_multi(dec.side, dec.setup.entry, features_by_tf)
    if prox.get("block"):
        # Ép về WAIT + thêm lý do "soft_proximity"
        dec.decision = "WAIT"
        reasons = list(dec.reasons or [])
        reasons.append("soft_proximity")
        dec.reasons = reasons
        # Không đổi setup; chỉ cấm vào kèo lúc này

    # -------- BB-low(4H) + RSI(1H) extreme guard --------
    bb_rsi_guard = _guard_near_bb_low_4h_and_rsi1h_extreme(dec.side, dec.setup.entry, features_by_tf)
    if bb_rsi_guard.get("block"):
        dec.decision = "WAIT"
        reasons = list(dec.reasons or [])
        reasons.append("guard:near_4h_bb_low_and_rsi1h_os")
        dec.reasons = reasons

    # -------- RR2/RR3 floor check -> WAIT + entry2 hint --------
    rr2_floor = float(os.getenv("RR2_FLOOR", "1.30"))
    rr3_floor = float(os.getenv("RR3_FLOOR", "1.80"))

    def _suggest_entry2_for_floor(side: str, sl: float, tp: float, floor: float, cur_entry: float) -> Optional[float]:
        try:
            if side == "long":
                # (tp - e2) / (e2 - sl) >= floor  =>  e2 <= (tp + floor*sl) / (1 + floor)
                e2 = (tp + floor * sl) / (1.0 + floor)
                return float(e2) if e2 > 0 else None
            elif side == "short":
                # (e2 - tp) / (sl - e2) >= floor  =>  e2 >= (floor*sl + tp) / (1 + floor)
                e2 = (floor * sl + tp) / (1.0 + floor)
                return float(e2) if e2 > 0 else None
        except Exception:
            return None
        return None

    rr_floor_hit = False
    suggest_from = None  # ("TP2"/"TP3", tp_value, floor_value)
    if tp2 is not None and rr2 is not None and rr2 < rr2_floor:
        rr_floor_hit = True
        suggest_from = ("TP2", tp2, rr2_floor)
    if tp3 is not None and rr3 is not None and rr3 < rr3_floor:
        rr_floor_hit = True
        # nếu cả 2 dưới sàn, ưu tiên ràng buộc nghiêm hơn (TP3)
        suggest_from = ("TP3", tp3, rr3_floor)

    if rr_floor_hit and dec.side in ("long","short") and dec.setup.entry is not None and dec.setup.sl is not None:
        dec.decision = "WAIT"
        reasons = list(dec.reasons or [])
        if "rr_floor" not in reasons:
            reasons.append("rr_floor")
        dec.reasons = reasons
        _tpname, _tpval, _floor = suggest_from
        e2 = _suggest_entry2_for_floor(dec.side, dec.setup.sl, float(_tpval), float(_floor), dec.setup.entry)
        if e2 is not None:
            # long: entry2 thấp hơn; short: entry2 cao hơn
            dec.setup.entry2 = float(e2)

    # ---------- price formatting helpers ----------
    def _infer_dp(symbol: str, price: Optional[float], features_by_tf: Dict[str, Any], evidence_bundle: Dict[str, Any]) -> int:
        """
        Ưu tiên:
        1) meta.price_dp / meta.tick_size -> dp
        2) Heuristic theo giá (crypto)
        3) VN stock (không có '/') -> 0 lẻ
        """
        # 1) từ features/meta nếu có
        try:
            meta = (features_by_tf or {}).get("1H", {}).get("meta", {}) or {}
            dp = meta.get("price_dp")
            if isinstance(dp, int) and 0 <= dp <= 8:
                return dp
            tick = meta.get("tick_size") or evidence_bundle.get("meta", {}).get("tick_size")
            if tick:
                s = f"{tick}"
                if "." in s:
                    return min(8, max(0, len(s.split(".")[1].rstrip("0"))))
                # tick là số nguyên -> 0 lẻ
                return 0
        except Exception:
            pass
        # 2) Heuristic theo giá (crypto)
        if "/" in symbol:
            p = float(price or evidence_bundle.get("last_price") or 0.0)
            if p >= 1000: return 1
            if p >= 100:  return 2
            if p >= 1:    return 3
            if p >= 0.1:  return 4
            if p >= 0.01: return 5
            return 6
        # 3) VN stock (mã không có '/'): 0 lẻ (VND)
        return 0

    def _fmt(x: Optional[float], dp: int) -> Optional[str]:
        if x is None:
            return None
        try:
            return f"{float(x):.{dp}f}"
        except Exception:
            return f"{x}"

    # size hint (leverage) theo công thức risk_pct / risk_raw
    size_hint = _leverage_hint(dec.side, dec.setup.entry, dec.setup.sl)

    # ---------- end helpers ----------

    plan = {
        "direction": dec.side.upper() if dec.side else None,
        "entry": dec.setup.entry,
        "entry2": None,               # kept for compatibility; tiny core emits single entry
        "sl": dec.setup.sl,
        "tp": tp1,                    # fallback single TP
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr1,                    # primary RR
        "rr2": rr2,
        "rr3": rr3,
        "risk_size_hint": size_hint,  # <— leverage đề xuất
    }

    # ---- ensure locals before logging ----
    decision = dec.decision or "WAIT"
    state = dec.state
    confidence = 0.0
    try:
        if isinstance(dec.meta, dict):
            confidence = float(dec.meta.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0

    # ---- logging with exchange-like decimals ----
    dp = _infer_dp(symbol, dec.setup.entry, features_by_tf, evidence_bundle)
    f_entry = _fmt(dec.setup.entry, dp)
    f_sl    = _fmt(dec.setup.sl, dp)
    f_tp1   = _fmt(tp1, dp) if tp1 is not None else None
    f_tp2   = _fmt(tp2, dp) if tp2 is not None else None
    f_tp3   = _fmt(tp3, dp) if tp3 is not None else None

    # legacy log line(s) — keep for backward-compat printing
    legacy_lines = []
    legacy_lines.append(
        " ".join(
            [
                f"[{symbol}]",
                f"DECISION={decision}",
                f"| STATE={state or '-'}",
                f"| DIR={plan['direction'] or '-'}",
                f"| entry={f_entry}",
                f"sl={f_sl}",
                f"TP1={f_tp1}" if f_tp1 is not None else "TP1=None",
                f"TP2={f_tp2}" if f_tp2 is not None else "TP2=None",
                f"TP3={f_tp3}" if f_tp3 is not None else "TP3=None",
                f"RR1={f'{rr1:.1f}' if rr1 is not None else 'None'}",
                f"RR2={f'{rr2:.1f}' if rr2 is not None else 'None'}",
                f"RR3={f'{rr3:.1f}' if rr3 is not None else 'None'}",
                (
                    (lambda _v: f"LEV={__import__('math').floor(float(_v)):.1f}x")(size_hint)
                    if isinstance(size_hint,(int,float)) else ""
                ),
            ]
        )
    )

    # headline (one-liner) — show all three TPs
    _tp_parts_hl = [
        f"TP1={f_tp1}" if f_tp1 is not None else "TP1=None",
        f"TP2={f_tp2}" if f_tp2 is not None else "TP2=None",
        f"TP3={f_tp3}" if f_tp3 is not None else "TP3=None",
    ]
    _tp_text_hl = " ".join(_tp_parts_hl)
    headline = f"[{symbol}] {decision} | {state or '-'} {plan['direction'] or '-'} | E={f_entry} SL={f_sl} {_tp_text_hl}"

    # Telegram signal (nếu ENTER): format theo dp
    telegram_signal = None
    if decision == "ENTER" and plan["direction"] and dec.setup.sl is not None and (dec.setup.entry is not None or tp1 is not None):
        strategy = (state or "").replace("_", " ").title()
        entry_lines = []
        if dec.setup.entry is not None:
            entry_lines.append(f"Entry: {f_entry}")
        if f_tp1 is not None:
            entry_lines.append(f"TP1: {f_tp1}")
        if f_tp2 is not None:
            entry_lines.append(f"TP2: {f_tp2}")
        if f_tp3 is not None:
            entry_lines.append(f"TP3: {f_tp3}")
        telegram_signal = "\n".join(
            [
                f"#{symbol.replace('/', '')} {plan['direction']}",
                f"State: {state or '-'} | Strategy: {strategy}",
                *entry_lines,
                f"SL: {f_sl}",
                f"RR1: {rr1:.1f}" if rr1 is not None else "",
            ]
        ).strip()
    # Chuẩn hoá logs cho main.py:
    # - Giữ legacy text trong logs["TEXT"] (list)
    # - Cung cấp cấu trúc cho WAIT/ENTER để main.py lấy missing/reasons
    logs: Dict[str, Any] = {
        "TEXT": legacy_lines,
        "ENTER": {"state_meta": dec.meta} if decision == "ENTER" else {},
        "WAIT": (
            {
                "missing": list(dec.reasons or []),
                "reasons": list(dec.reasons or []),
                "state_meta": dec.meta,
            }
            if decision != "ENTER"
            else {}
        ),
        "AVOID": {},
    }
    notes: List[str] = []
    if dec.state == "none_state":
        notes.append("No clear retest/break context — WAIT")
    if "far_from_entry" in dec.reasons:
        notes.append("Proximity guard: too far from entry")
    if "rr_too_low" in dec.reasons:
        notes.append("RR min not satisfied")
    if "soft_proximity" in dec.reasons:
        notes.append(f"Soft proximity (BB/EMA): {prox.get('why','')}")

    out = {
        "symbol": symbol,
        "timeframe": timeframe,
        "asof": evidence_bundle.get("asof"),
        "state": state,
        "confidence": round(confidence, 3),
        "decision": decision,
        "plan": plan,
        "logs": logs,
        "reasons": list(dec.reasons or []),  # tiện lợi, phòng khi caller cần
        "notes": notes,
        "headline": headline,
        "telegram_signal": telegram_signal,
    }
    return out
