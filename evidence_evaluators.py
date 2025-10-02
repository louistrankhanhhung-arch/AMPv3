"""
Evidence Evaluators v2 (no trade decision)
-----------------------------------------
Adds pullback/throwback/BB-expansion/volume-explosive evidences and wires them
into a richer evidence bundle. Intended to be used by decision_engine_v2.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import os, json, logging
import math

# ===============================================
# Reversal signal evaluator (supports 2 signatures)
# ===============================================
def _last_closed_row(df: pd.DataFrame | None) -> pd.Series | None:
    try:
        if df is None or len(df) == 0:
            return None
        return df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
    except Exception:
        return None

def _rev_eval_from_df(side_up: str, df4: pd.DataFrame | None, df1: pd.DataFrame | None) -> tuple[bool, str]:
    """
    Core reversal logic moved from main.py.
    Returns (is_reversal, why).
    side_up ∈ {'LONG','SHORT'}
    """
    try:
        last4 = _last_closed_row(df4) if df4 is not None else None
        prev4 = df4.iloc[-3] if (df4 is not None and len(df4) >= 3) else None
        last1 = _last_closed_row(df1) if df1 is not None else None
        if last4 is None:
            return False, ""

        # Lấy các mức/indicator 4H
        c4  = float(last4.get("close", last4["close"]))
        e20 = float(last4["ema20"]) if "ema20" in last4 else float("nan")
        e50 = float(last4["ema50"]) if "ema50" in last4 else float("nan")
        bmid= float(last4["bb_mid"]) if "bb_mid" in last4 else float("nan")
        atr = float(last4["atr14"])  if "atr14" in last4 else 0.0
        # RSI xác nhận từ 1H (nếu có)
        rsi1 = None
        if last1 is not None:
            for col in ("rsi14","rsi","RSI"):
                if col in last1.index:
                    rsi1 = float(last1[col]); break
            # Lấy RSI 1H của bar trước để kiểm tra tín hiệu cross
            prev1 = None
            try:
                if df1 is not None and len(df1) >= 3:
                    prev1 = df1.iloc[-3]
            except Exception:
                prev1 = None
            rsi1_prev = None
            if prev1 is not None:
                for col in ("rsi14","rsi","RSI"):
                    if col in prev1.index:
                        rsi1_prev = float(prev1[col]); break

        conds: list[str] = []
        strong: list[str] = []

        if side_up == "LONG":
            # Yếu tố “mềm” (cần >=2)
            if (e20 == e20) and (e50 == e50) and (c4 < (e50 - 0.10*atr)) and (e20 < e50):
                conds.append("close<ema50-0.1ATR & ema20<ema50(4H)")
            if (bmid == bmid) and atr > 0 and (c4 < (bmid - 0.35*atr)):
                conds.append("close < BBmid - 0.35*ATR(4H)")
            if rsi1 is not None and rsi1 <= 35.0:
                conds.append("RSI(1H)≤35")
            rsi_cross = (rsi1 is not None) and (rsi1_prev is not None) and (rsi1_prev > 50.0) and (rsi1 < 45.0)
            if rsi_cross:
                conds.append("RSI(1H) bear cross 50→45")
            # Mẫu nến mạnh (1 điều kiện đủ)
            if prev4 is not None:
                o4 = float(last4.get("open", last4["close"]))
                op = float(prev4.get("open", prev4["close"]))
                cp = float(prev4.get("close", prev4["close"]))
                # Bearish engulfing 4H
                if (c4 < o4) and (o4 >= cp) and (c4 <= op):
                    strong.append("bearish_engulfing(4H)")
        else:  # SHORT
            if (e20 == e20) and (e50 == e50) and (c4 > (e50 + 0.10*atr)) and (e20 > e50):
                conds.append("close>ema50+0.1ATR & ema20>ema50(4H)")
            if (bmid == bmid) and atr > 0 and (c4 > (bmid + 0.35*atr)):
                conds.append("close > BBmid + 0.35*ATR(4H)")
            if rsi1 is not None and rsi1 >= 65.0:
                conds.append("RSI(1H)≥65")
            rsi_cross = (rsi1 is not None) and (rsi1_prev is not None) and (rsi1_prev < 50.0) and (rsi1 > 55.0)
            if rsi_cross:
                conds.append("RSI(1H) bull cross 50→55")
            if prev4 is not None:
                o4 = float(last4.get("open", last4["close"]))
                op = float(prev4.get("open", prev4["close"]))
                cp = float(prev4.get("close", prev4["close"]))
                # Bullish engulfing 4H
                if (c4 > o4) and (o4 <= cp) and (c4 >= op):
                    strong.append("bullish_engulfing(4H)")

        # Cần nến mạnh + ≥1 điều kiện mềm, hoặc ≥4 điều kiện mềm (và bắt buộc có RSI-cross)
        if strong and conds:
            return True, f"{strong[0]} + {conds[0]}"
        if (len(conds) >= 4) and rsi_cross:
            return True, " & ".join(conds[:3])
        return False, ""
    except Exception:
        return False, ""

def _reversal_signal(*args):
    """
    Two-call styles:
      1) For engine guard (bool):        _reversal_signal(bundle: dict, side: str) -> bool
      2) For progress-close (tuple):     _reversal_signal(side: str, df4, df1) -> (bool, str)
    """
    # Style 1: (bundle, side) -> bool
    if len(args) == 2 and isinstance(args[0], dict):
        bundle, side = args
        side_up = str(side).upper()
        # Try to extract df from bundle if present; if unavailable, be safe (no close)
        try:
            fbtf = (bundle.get("features_by_tf") or {})
            df4 = ((fbtf.get("4H") or {}).get("df")) or None
            df1 = ((fbtf.get("1H") or {}).get("df")) or None
        except Exception:
            df4 = df1 = None
        ok, _ = _rev_eval_from_df(side_up, df4, df1)
        return bool(ok)
    # Style 2: (side, df4, df1) -> (bool, str)
    if len(args) == 3:
        side_up, df4, df1 = args
        return _rev_eval_from_df(str(side_up).upper(), df4, df1)
    # Fallback
    return False

# --------------------------
# MINI RE-TEST (4H, self-contained)
# --------------------------
def ev_mini_retest_4h(
    df: pd.DataFrame,
    lookback_bars: int = 3,
    atr_frac: float = 0.25,
) -> Dict[str, Any]:
    """
    Mini-retest cho khung 4H:
    - Dùng 2 nến đã đóng gần nhất (tránh whipsaw intrabar).
    - 'Retest nhỏ' nếu biên retest nằm trong atr_frac * ATR của nến trước.
    - Xác định hướng bằng close so với high/low nến trước.
    Trả về: {"ok": bool, "dir": "long"|"short"|None, "detail": {...}}
    """
    out: Dict[str, Any] = {"ok": False, "dir": None, "long": False, "short": False, "detail": {}}
    try:
        if df is None or len(df) < max(lookback_bars + 2, 3):
            return out

        # Dùng 2 nến đã đóng gần nhất
        last = df.iloc[-2]
        prev = df.iloc[-3]

        hi_last = float(last.get("high", last["close"]))
        lo_last = float(last.get("low",  last["close"]))
        cl_last = float(last.get("close", last["close"]))

        hi_prev = float(prev.get("high", prev["close"]))
        lo_prev = float(prev.get("low",  prev["close"]))
        cl_prev = float(prev.get("close", prev["close"]))

        # ATR: lấy từ cột 'atr' nếu có; nếu không, xấp xỉ bằng (high-low) trung bình 3 nến gần nhất
        if "atr" in df.columns:
            atr_val = float(df.iloc[-3:]["atr"].dropna().iloc[-1])
        else:
            _rng = (df.iloc[-3:]["high"] - df.iloc[-3:]["low"]).abs()
            atr_val = float(_rng.mean())
        if not math.isfinite(atr_val) or atr_val <= 0:
            return out

        tol = atr_frac * atr_val

        # Retest LONG nhỏ: nến hiện tại (last) lùi kiểm định đỉnh cũ (hi_prev) trong phạm vi tol,
        # và đóng cửa vẫn giữ xu hướng (cl_last > cl_prev).
        long_retest = (abs(lo_last - hi_prev) <= tol) and (cl_last > cl_prev)

        # Retest SHORT nhỏ: nến hiện tại (last) lùi kiểm định đáy cũ (lo_prev) trong phạm vi tol,
        # và đóng cửa vẫn giữ xu hướng giảm (cl_last < cl_prev).
        short_retest = (abs(hi_last - lo_prev) <= tol) and (cl_last < cl_prev)

        if long_retest and not short_retest:
            out["ok"] = True
            out["dir"] = "long"
            out["long"] = True
            out["short"] = False
        elif short_retest and not long_retest:
            out["ok"] = True
            out["dir"] = "short"
            out["long"] = False
            out["short"] = True

        out["detail"] = {
            "hi_prev": hi_prev, "lo_prev": lo_prev, "hi_last": hi_last, "lo_last": lo_last,
            "cl_prev": cl_prev, "cl_last": cl_last, "atr": atr_val, "tol": tol,
            "long_retest": bool(long_retest), "short_retest": bool(short_retest),
        }
    except Exception as e:
        out["detail"] = {"error": str(e)}
    return out

# --------------------------------------------------------------------------------------
# 1) Types & Config (per-TF thresholds with sensible defaults)
# --------------------------------------------------------------------------------------

@dataclass
class TFThresholds:
    break_buffer_atr: float = 0.2
    vol_ratio_thr: float = 1.5
    vol_z_thr: float = 1.0
    rsi_long: float = 55.0
    rsi_short: float = 45.0
    bbw_lookback: int = 50
    zigzag_pct: float = 2.0
    ema_spread_small_atr: float = 0.3
    hvn_guard_atr: float = 0.7

@dataclass
class Config:
    per_tf: Dict[str, TFThresholds] = field(default_factory=lambda: {
        "1H": TFThresholds(break_buffer_atr=0.35, vol_ratio_thr=1.3, vol_z_thr=0.7, rsi_long=55, rsi_short=45, bbw_lookback=50, zigzag_pct=2.0, ema_spread_small_atr=0.35, hvn_guard_atr=0.7),
        "4H": TFThresholds(break_buffer_atr=0.20, vol_ratio_thr=1.25, vol_z_thr=0.6, rsi_long=55, rsi_short=45, bbw_lookback=50, zigzag_pct=2.0, ema_spread_small_atr=0.3,  hvn_guard_atr=0.8),
        "1D": TFThresholds(break_buffer_atr=0.15, vol_ratio_thr=1.2, vol_z_thr=0.5, rsi_long=55, rsi_short=45, bbw_lookback=50, zigzag_pct=2.0, ema_spread_small_atr=0.25, hvn_guard_atr=1.0),
    })

PRIMARY_TF = "4H"
CONFIRM_TF = "4H"
CONTEXT_TF = "1D"

# --------------------------------------------------------------------------------------
# 2) Utilities
# --------------------------------------------------------------------------------------

def _get_last_closed_bar(df: pd.DataFrame) -> pd.Series:
    if len(df) >= 2:
        return df.iloc[-2]
    return df.iloc[-1] if len(df) else pd.Series(dtype=float)

# --------------------------
# ATR-adaptive helpers
# --------------------------
 
def _atr_regime(df: pd.DataFrame, lookback: int = 180) -> Dict[str, Any]:
    """
    Determine low/normal/high volatility regime using NATR percentiles.
    Returns: {'regime': str, 'natr': float, 'p33': float, 'p67': float}
    """
    out = {"regime": "normal", "natr": np.nan, "p33": np.nan, "p67": np.nan}
    if df is None or not len(df) or 'atr14' not in df or 'close' not in df:
        return out
    n = min(lookback, len(df))
    sub = df.iloc[-n:]
    natr = sub['atr14'] / sub['close'].replace(0, np.nan)
    p33 = np.nanpercentile(natr, 33)
    p67 = np.nanpercentile(natr, 67)
    now = float(natr.iloc[-1])
    reg = "normal"
    if now <= p33:
        reg = "low"
    elif now >= p67:
        reg = "high"
    return {"regime": reg, "natr": now, "p33": float(p33), "p67": float(p67)}

def _clone(obj):
    # light clone for dataclass-like configs
    return type(obj)(**{k: getattr(obj, k) for k in obj.__annotations__.keys()})

def _adapt_cfg(cfg_tf: TFThresholds, regime: str) -> TFThresholds:
    """
    Scale key thresholds per regime. We keep 'normal' ~1.0×, soften at 'low',
    and harden at 'high' to reduce whipsaw.
    """
    c = _clone(cfg_tf)
    if regime == "low":
        c.break_buffer_atr *= 0.85
        c.hvn_guard_atr     *= 0.90
        c.ema_spread_small_atr *= 1.10
        c.rsi_long = float(c.rsi_long) - 2.0
        c.rsi_short = float(c.rsi_short) + 2.0
        c.vol_ratio_thr = float(c.vol_ratio_thr) - 0.10
        c.vol_z_thr     = float(c.vol_z_thr)     - 0.10
    elif regime == "high":
        c.break_buffer_atr *= 1.25
        c.hvn_guard_atr     *= 1.20
        c.ema_spread_small_atr *= 0.90
        c.rsi_long = float(c.rsi_long) + 2.0
        c.rsi_short = float(c.rsi_short) - 2.0
        c.vol_ratio_thr = float(c.vol_ratio_thr) + 0.20
        c.vol_z_thr     = float(c.vol_z_thr)     + 0.20
    # normal: keep as is
    return c

def _slow_market_guards(bbw_now: float, bbw_med: float,
                        vol_now: float, vol_med: float,
                        regime: str,
                        elapsed_frac: float | None = None) -> Dict[str, Any]:
    """
    Two lightweight guards for 'slow/illiquid' sessions.
      - vol_of_vol: bbw_now / bbw_med
      - liq_ratio : vol_now / vol_med
      elapsed_frac: tỉ lệ thời gian đã trôi của cây nến 1H hiện tại (0..1)
                  dùng để time-scale ngưỡng liquidity_floor.
    """
    bbw_med = float(bbw_med or 0.0)
    vol_med = float(vol_med or 0.0)
    vov = (float(bbw_now) / bbw_med) if bbw_med > 0 else np.nan
    liq = (float(vol_now) / vol_med) if vol_med > 0 else np.nan
    is_slow = (regime == "low") and (vov < 0.8 if np.isfinite(vov) else False)
    base_thr = 0.7 if regime == 'low' else (0.5 if regime == 'high' else 0.55)
    # time-weight: thr(t) = base * (elapsed/bar)^0.7
    try:
        ef = 1.0 if elapsed_frac is None else float(elapsed_frac)
        ef = max(0.0, min(1.0, ef))
    except Exception:
        ef = 1.0
    liq_thr = float(base_thr) * (ef ** 0.7)

    liq_floor = (liq < liq_thr) if np.isfinite(liq) else False
    return {
        "regime": regime,
        "vol_of_vol": float(vov) if np.isfinite(vov) else None,
        "liquidity_ratio": float(liq) if np.isfinite(liq) else None,
        "is_slow": bool(is_slow),
        "liquidity_floor": bool(liq_floor),
        "liq_thr": float(liq_thr),
        "elapsed_frac": float(ef),
    }

def _last_swing(swings: Dict[str, Any], kind: str) -> Optional[float]:
    if not swings or 'swings' not in swings:
        return None
    for s in reversed(swings['swings']):
        if s.get('type') == kind:
            return float(s['price'])
    return None


def _ema_spread_atr(df: pd.DataFrame) -> float:
    e20 = float(df['ema20'].iloc[-1])
    e50 = float(df['ema50'].iloc[-1])
    atr = float(df['atr14'].iloc[-1]) if 'atr14' in df.columns else 0.0
    if atr <= 0:
        return 0.0
    return abs(e20 - e50) / atr

# --------------------------------------------------------------------------------------
# 3) Core evidences (existing)
# --------------------------------------------------------------------------------------

def ev_price_breakout(df: pd.DataFrame, swings: Dict[str, Any], atr: float, cfg: TFThresholds) -> Dict[str, Any]:
    hh = _last_swing(swings, 'HH')
    close = float(df['close'].iloc[-1])
    buffer = cfg.break_buffer_atr * atr
    if hh is None:
        return {"ok": False, "score": 0.0, "why": "no HH reference", "missing": ["hh"], "ref": None}
    core = close > (hh + buffer)
    last = _get_last_closed_bar(df)
    ft = bool(last['close'] > (hh + buffer))
    hold = bool(last['low'] > (hh + 0.1 * atr))
    score = (0.6 if core else 0.0) + (0.2 if ft else 0.0) + (0.2 if hold else 0.0)
    return {"ok": bool(core), "score": round(score,3), "why": ",".join([w for w in ["core" if core else "", "follow" if ft else "", "hold" if hold else ""] if w]), "missing": ([] if core else ["price"]), "ref": {"hh": hh, "buffer": buffer}}


def ev_price_breakdown(df: pd.DataFrame, swings: Dict[str, Any], atr: float, cfg: TFThresholds) -> Dict[str, Any]:
    ll = _last_swing(swings, 'LL')
    close = float(df['close'].iloc[-1])
    buffer = cfg.break_buffer_atr * atr
    if ll is None:
        return {"ok": False, "score": 0.0, "why": "no LL reference", "missing": ["ll"], "ref": None}
    core = close < (ll - buffer)
    last = _get_last_closed_bar(df)
    ft = bool(last['close'] < (ll - buffer))
    hold = bool(last['high'] < (ll - 0.1 * atr))
    score = (0.6 if core else 0.0) + (0.2 if ft else 0.0) + (0.2 if hold else 0.0)
    return {"ok": bool(core), "score": round(score,3), "why": ",".join([w for w in ["core" if core else "", "follow" if ft else "", "hold" if hold else ""] if w]), "missing": ([] if core else ["price"]), "ref": {"ll": ll, "buffer": buffer}}


def ev_price_reclaim(df: pd.DataFrame, level: float, atr: float, cfg: TFThresholds, side: str = 'long') -> Dict[str, Any]:
    """
    Reclaim hợp lệ khi:
      - Có cross rõ ràng so với level ± buffer (dùng close của 2 nến liên tiếp)
      - Và nến hiện tại "hold" tối thiểu (low/high không đâm lại quá sâu)
    """
    if not np.isfinite(level) or df is None or len(df) < 2:
        return {"ok": False, "score": 0.0, "why": "invalid_input", "missing": ["level" if not np.isfinite(level) else None], "ref": None}

    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last['close']); prev_close = float(prev['close'])
    low = float(last['low']); high = float(last['high'])
    atr = float(atr or 0.0)
    buf = float(cfg.break_buffer_atr) * atr

    if side == 'long':
        crossed = (prev_close <= level - buf) and (close > level + buf)
        hold_ok = (low > level - 0.1 * atr)
    else:
        crossed = (prev_close >= level + buf) and (close < level - buf)
        hold_ok = (high < level + 0.1 * atr)

    ok = bool(crossed and hold_ok)
    score = (0.8 if crossed else 0.0) + (0.2 if hold_ok else 0.0)
    return {"ok": ok, "score": round(score, 3), "why": f"cross@±{cfg.break_buffer_atr:.2f}ATR|hold={hold_ok}", "ref": {"level": float(level), "buffer": buf, "side": side}}

# --- NEW: reclaim auto, không dùng priors/điểm ---
def ev_price_reclaim_auto(df, *, level: float, atr: float, cfg) -> dict:
    """
    Xác nhận reclaim nếu có cross rõ ràng quanh level ± buffer (ATR),
    và nến hiện tại 'hold' không thủng lại quá sâu.
    Trả về side 'long' (cross lên) hoặc 'short' (cross xuống).
    """
    import numpy as np
    if df is None or len(df) < 2 or not np.isfinite(level):
        return {"ok": False, "why": "invalid_input"}

    last, prev = df.iloc[-1], df.iloc[-2]
    close, prev_close = float(last["close"]), float(prev["close"])
    low  = float(last["low"])  if "low"  in last else close
    high = float(last["high"]) if "high" in last else close
    atr  = float(atr or 0.0)
    buf  = float(getattr(cfg, "break_buffer_atr", 0.2)) * atr  # ví dụ 0.2 ATR

    long_cross  = (prev_close <= level - buf) and (close > level + buf)
    short_cross = (prev_close >= level + buf) and (close < level - buf)

    if long_cross:
        hold_ok = (low > level - 0.1 * (atr or 0.0))
        return {"ok": bool(hold_ok), "ref": {"level": float(level), "side": "long"}, "why": f"cross_up|hold={hold_ok}"}
    if short_cross:
        hold_ok = (high < level + 0.1 * (atr or 0.0))
        return {"ok": bool(hold_ok), "ref": {"level": float(level), "side": "short"}, "why": f"cross_down|hold={hold_ok}"}

    return {"ok": False, "why": "no_cross"}

# --- SR unification helpers ---
def pick_ref_level(levels, price, soft_levels=None):
    """
    Chọn mốc SR thống nhất (ref_level) một cách bảo thủ:
      1) tp của bands_up/down gần giá nhất
      2) sr_up/sr_down gần giá nhất
      3) (tuỳ chọn) soft_up/soft_down['level'] gần giá nhất
      -> KHÔNG fallback về price nếu không có mốc.
    """
    import numpy as np
    levels = levels or {}
    soft_levels = soft_levels or {}

    cands = []

    # bands_up/down: lấy 'tp' nếu tồn tại
    for seq in (levels.get("bands_up") or []), (levels.get("bands_down") or []):
        for o in (seq or []):
            try:
                v = float(o.get("tp"))
                if np.isfinite(v):
                    cands.append(v)
            except Exception:
                pass

    # sr_up/down: các giá trị số
    for key in ("sr_up", "sr_down"):
        for v in (levels.get(key) or []):
            try:
                v = float(v)
                if np.isfinite(v):
                    cands.append(v)
            except Exception:
                pass

    if cands:
        return min(cands, key=lambda v: abs(v - float(price))) if np.isfinite(float(price)) else None

    # soft levels (nếu muốn)
    soft = []
    for key in ("soft_up", "soft_down"):
        for o in (soft_levels.get(key) or []):
            try:
                v = float(o.get("level"))
                if np.isfinite(v):
                    soft.append(v)
            except Exception:
                pass
    if soft:
        return min(soft, key=lambda v: abs(v - float(price))) if np.isfinite(float(price)) else None

    return None  # không bịa ref_level

def ev_sideways(df: pd.DataFrame, bbw_last: float, bbw_med: float, atr: float, cfg: TFThresholds) -> Dict[str, Any]:
    ema_spread = _ema_spread_atr(df)
    squeeze = bool(bbw_last < bbw_med)
    rng_ok = (df['high'].tail(20).max() - df['low'].tail(20).min()) / max(atr, 1e-9) <= 3.0
    ok = squeeze and (ema_spread <= cfg.ema_spread_small_atr) and rng_ok
    score = (0.4 if squeeze else 0.0) + (0.4 if ema_spread <= cfg.ema_spread_small_atr else 0.0) + (0.2 if rng_ok else 0.0)
    return {"ok": bool(ok), "score": round(score,3), "why": "|".join([w for w in ["squeeze" if squeeze else "", "ema_spread_small" if ema_spread <= cfg.ema_spread_small_atr else "", "narrow_range" if rng_ok else ""] if w]), "missing": [] if ok else ["sideways_conditions"]}


def ev_volume(vol: Dict[str, Any], cfg: TFThresholds) -> Dict[str, Any]:
    vr = float(vol.get('vol_ratio', 1.0)); vz = float(vol.get('vol_z20', 0.0))
    ok = (vr >= cfg.vol_ratio_thr) or (vz >= cfg.vol_z_thr)
    strong = (vr >= max(2.0, cfg.vol_ratio_thr + 0.3)) or (vz >= max(2.0, cfg.vol_z_thr + 0.5))
    grade = 'strong' if strong else ('ok' if ok else 'weak')
    score = 1.0 if strong else (0.7 if ok else 0.0)
    return {"ok": bool(ok), "score": round(score,3), "why": ",".join([w for w in [f"vr>={cfg.vol_ratio_thr}" if vr >= cfg.vol_ratio_thr else "", f"vz>={cfg.vol_z_thr}" if vz >= cfg.vol_z_thr else ""] if w]), "missing": [] if ok else ["volume"], "vol_ratio": vr, "vol_z20": vz, "grade": grade}


def ev_momentum(mom: Dict[str, Any], cfg: TFThresholds, side: str = 'long') -> Dict[str, Any]:
    rsi = float(mom.get('rsi', 50.0)); div = mom.get('divergence', 'none')
    ok = (rsi >= cfg.rsi_long) if side == 'long' else (rsi <= cfg.rsi_short)
    score = 0.8 if ok else 0.2
    if (side == 'long' and div == 'bearish') or (side == 'short' and div == 'bullish'):
        score -= 0.2
    return {"ok": bool(ok), "score": round(max(0.0, score),3), "why": f"rsi={rsi:.1f}|div={div}", "missing": [] if ok else ["rsi"], "rsi": rsi, "divergence": div}


def ev_trend_alignment(trend_now: Dict[str, Any], trend_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    now = trend_now.get('state'); ctx = trend_ctx.get('state') if trend_ctx else None
    ema_ok = (now in ('up','down')); aligned = (ctx is None) or (now == ctx)
    return {"ok": bool(ema_ok and aligned), "score": round((0.7 if ema_ok else 0.0) + (0.3 if aligned else 0.0),3), "why": f"now={now}|ctx={ctx}", "missing": [] if ema_ok and aligned else ["trend"]}


def ev_candles(candles: Dict[str, Any], side: Optional[str] = None) -> Dict[str, Any]:
    ok = False; why = []
    if side == 'long':
        ok = bool(candles.get('bullish_engulf') or candles.get('bullish_pin'))
        if candles.get('bullish_engulf'): why.append('bullish_engulf')
        if candles.get('bullish_pin'): why.append('bullish_pin')
    elif side == 'short':
        ok = bool(candles.get('bearish_engulf') or candles.get('bearish_pin'))
        if candles.get('bearish_engulf'): why.append('bearish_engulf')
        if candles.get('bearish_pin'):    why.append('bearish_pin')
    else:
        ok = any(bool(v) for v in candles.values()); why = [k for k,v in candles.items() if v]
    return {"ok": bool(ok), "score": 0.6 if ok else 0.0, "why": ",".join(why), "missing": [] if ok else ["candle"]}


def ev_liquidity(price: float, atr: float, vp_zones: List[Dict[str, Any]], cfg: TFThresholds, side: Optional[str] = None) -> Dict[str, Any]:
    guard = cfg.hvn_guard_atr * max(atr, 1e-9)
    near_heavy = False; nearest = None
    def mid(z):
        return (float(z['price_range'][0]) + float(z['price_range'][1]))/2.0
    for z in vp_zones or []:
        m = mid(z)
        if side == 'long' and m >= price and (m - price) <= guard: near_heavy=True; nearest=m; break
        if side == 'short' and m <= price and (price - m) <= guard: near_heavy=True; nearest=m; break
    return {"ok": (not near_heavy), "score": 1.0 if not near_heavy else 0.2, "why": "" if not near_heavy else f"heavy_zone_within_{cfg.hvn_guard_atr}*ATR", "near_heavy_zone": bool(near_heavy), "nearest_zone_mid": nearest}

# --------------------------------------------------------------------------------------
# 4) New evidences: BB expanding, explosive volume, throwback, pullback
# --------------------------------------------------------------------------------------

def ev_bb_expanding(bbw_last: float, bbw_med: float) -> Dict[str, Any]:
    ok = bool(bbw_last > bbw_med)
    return {"ok": ok, "score": 0.7 if ok else 0.0, "why": "bbw_last>bbw_med" if ok else "bbw_last<=bbw_med", "bbw_last": float(bbw_last), "bbw_med": float(bbw_med)}


def ev_volume_explosive(vol: Dict[str, Any]) -> Dict[str, Any]:
    vr = float(vol.get('vol_ratio', 1.0)); vz = float(vol.get('vol_z20', 0.0))
    ok = (vr >= 2.0) or (vz >= 2.0)
    strong = (vr >= 3.0) or (vz >= 3.0)
    grade = 'strong' if strong else ('ok' if ok else 'weak')
    score = 1.0 if strong else (0.8 if ok else 0.0)
    why = []
    if vr >= 2.0: why.append('vol_ratio>=2')
    if vz >= 2.0: why.append('vol_z>=2')
    return {"ok": ok, "score": round(score,3), "why": ",".join(why), "grade": grade, "vol_ratio": vr, "vol_z20": vz}


def _last_hh_ll_from_swings(swings: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    hh = None; ll = None
    for s in reversed(swings.get('swings', [])):
        if hh is None and s.get('type') == 'HH': hh = float(s['price'])
        if ll is None and s.get('type') == 'LL': ll = float(s['price'])
        if hh is not None and ll is not None: break
    return hh, ll


def ev_throwback_ready(df: pd.DataFrame, swings: Dict[str, Any], atr: float, side: Optional[str], pad_range: Tuple[float,float]=(0.02,0.10)) -> Dict[str, Any]:
    if atr <= 0 or side not in ('long','short'):
        return {"ok": False, "why": "side_or_atr_invalid"}
    # Adapt pad range by ATR regime (override only when default is used)
    try:
        reg = _atr_regime(df).get("regime", "normal")
    except Exception:
        reg = "normal"
    base_pad = pad_range
    if pad_range == (0.02, 0.10) or pad_range is None:
        base_pad = (0.02, 0.10) if reg in ("low", "normal") else (0.03, 0.12)
    hh, ll = _last_hh_ll_from_swings(swings)
    if side == 'long' and hh is not None:
        lo = float(hh + base_pad[0]*atr); hi = float(hh + base_pad[1]*atr)
        return {"ok": True, "why": "throwback_zone_ready", "ref": hh, "zone": [lo, hi]}
    if side == 'short' and ll is not None:
        lo = float(ll - base_pad[1]*atr); hi = float(ll - base_pad[0]*atr)
        return {"ok": True, "why": "throwback_zone_ready", "ref": ll, "zone": [lo, hi]}
    return {"ok": False, "why": "no_ref_level"}

def ev_throwback_valid(df, swings, atr, side, candles=None, lookback=3):
    base = ev_throwback_ready(df, swings, atr, side)
    if not base.get('ok'):
        return {"ok": False, "why": "throwback_zone_unavailable"}
    lo, hi = base.get('zone', [None, None])
    if lo is None or hi is None:
        return {"ok": False, "why": "throwback_zone_invalid"}

    # chạm vùng trong N nến gần nhất
    closes = df['close'].values[-max(lookback, 1):]
    touched = any((lo <= float(c) <= hi) for c in closes)

    # nến xác nhận theo hướng
    cc = ev_candles(candles or {}, side)
    confirm = bool(cc.get('ok', False))

    inside = False
    try:
        last = df.iloc[-1]; prev = df.iloc[-2]
        inside = bool((last['high'] <= prev['high']) and (last['low'] >= prev['low']))
    except Exception:
        inside = False
    mid = float((lo + hi) / 2.0)
    try:
        atr_now = float(df['atr14'].iloc[-1])
    except Exception:
        atr_now = 0.0
    dist_ok = (abs(float(df['close'].iloc[-1]) - mid) <= 0.6 * max(atr_now, 1e-9))
    ok = bool((touched and confirm) or (inside and dist_ok))

    return {
        "ok": ok,
        "why": "throwback_valid" if ok else "throwback_not_ok",
        "ref": base.get('ref'),
        "zone": base.get('zone'), "mid": (float((base.get("zone")[0] + base.get("zone")[1]) / 2.0) if base.get("zone") else None),
        "confirm_candle": confirm
    }


def ev_pullback_valid(df: pd.DataFrame, swings: Dict[str, Any], atr: float, mom: Dict[str, Any], vol: Dict[str, Any], candles: Dict[str, Any], side: Optional[str]) -> Dict[str, Any]:
    if atr <= 0 or side not in ('long','short'):
        return {"ok": False, "why": "side_or_atr_invalid"}
    hh, ll = _last_hh_ll_from_swings(swings)
    rsi = float(mom.get('rsi', 50.0))
    # Adapt retracement window & zone width by ATR regime
    try:
        reg = _atr_regime(df).get("regime", "normal")
    except Exception:
        reg = "normal"
    if reg == "low":
        retr_lo, retr_hi, zone_k = 0.236, 0.382, 0.05
    elif reg == "high":
        retr_lo, retr_hi, zone_k = 0.382, 0.618, 0.06
    else:
        retr_lo, retr_hi, zone_k = 0.382, 0.50, 0.05
    try:
        v5 = float(df['volume'].tail(5).mean()); v10 = float(df['volume'].tail(10).mean());
        vs20 = float(df['vol_sma20'].iloc[-1]) if 'vol_sma20' in df.columns else v10
        contracting = (v5 < v10) and (v10 < vs20)
    except Exception:
        contracting = True
    bull = bool(df.get('lower_wick_pct', pd.Series([0])).iloc[-1] >= 50 if 'lower_wick_pct' in df.columns else False) or False
    bear = bool(df.get('upper_wick_pct', pd.Series([0])).iloc[-1] >= 50 if 'upper_wick_pct' in df.columns else False) or False

    if side == 'long' and hh is not None and ll is not None and hh > ll:
        rng = max(1e-9, hh - ll)
        current = float(df['close'].iloc[-1])
        retr = float((hh - current) / rng)
        ok = (retr_lo <= retr <= retr_hi) and (rsi > 50) and contracting and (bull or True)
        # inside-bar tolerance near mid
        try:
            last = df.iloc[-1]; prev = df.iloc[-2]
            inside = bool((last["high"] <= prev["high"]) and (last["low"] >= prev["low"]))
        except Exception:
            inside = False
        ema20 = float(df['ema20'].iloc[-1]) if 'ema20' in df.columns else float('nan')
        bb_mid = float(df['bb_mid'].iloc[-1]) if 'bb_mid' in df.columns else float('nan')
        if not ok and inside:
            center = ema20 if np.isfinite(ema20) else bb_mid
            if np.isfinite(center) and abs(current - center) <= ((0.6 if _atr_regime(df).get('regime','normal') in ('low','normal') else 1.0) * atr):
                ok = True
        center = ema20 if np.isfinite(ema20) else bb_mid
        zone = [float(center - zone_k*atr), float(center + zone_k*atr)] if np.isfinite(center) else None
        fzone = [float(bb_mid - zone_k*atr), float(bb_mid + zone_k*atr)] if np.isfinite(bb_mid) else None
        return {"ok": bool(ok), "why": "pullback_ok" if ok else "pullback_not_ok", "retrace_pct": round(retr,3), "rsi_ok": bool(rsi>50), "vol_contracting": bool(contracting), "confirm_candle": bool(bull), "zone": zone, "fallback_zone": fzone, "mid": (float((zone[0]+zone[1])/2.0) if zone else (float((fzone[0]+fzone[1])/2.0) if fzone else None))}
    if side == 'short' and hh is not None and ll is not None and hh > ll:
        rng = max(1e-9, hh - ll)
        current = float(df['close'].iloc[-1])
        retr = float((current - ll) / rng)
        ok = (retr_lo <= retr <= retr_hi) and (rsi < 50) and contracting and (bear or True)
        # inside-bar tolerance near mid
        try:
            last = df.iloc[-1]; prev = df.iloc[-2]
            inside = bool((last["high"] <= prev["high"]) and (last["low"] >= prev["low"]))
        except Exception:
            inside = False
        ema20 = float(df['ema20'].iloc[-1]) if 'ema20' in df.columns else float('nan')
        bb_mid = float(df['bb_mid'].iloc[-1]) if 'bb_mid' in df.columns else float('nan')
        if not ok and inside:
            center = ema20 if np.isfinite(ema20) else bb_mid
            if np.isfinite(center) and abs(current - center) <= ((0.6 if _atr_regime(df).get('regime','normal') in ('low','normal') else 1.0) * atr):
                ok = True
        center = ema20 if np.isfinite(ema20) else bb_mid
        zone = [float(center - zone_k*atr), float(center + zone_k*atr)] if np.isfinite(center) else None
        fzone = [float(bb_mid - zone_k*atr), float(bb_mid + zone_k*atr)] if np.isfinite(bb_mid) else None
        return {"ok": bool(ok), "why": "pullback_ok" if ok else "pullback_not_ok", "retrace_pct": round(retr,3), "rsi_ok": bool(rsi<50), "vol_contracting": bool(contracting), "confirm_candle": bool(bear), "zone": zone, "fallback_zone": fzone, "mid": (float((zone[0]+zone[1])/2.0) if zone else (float((fzone[0]+fzone[1])/2.0) if fzone else None))}
    return {"ok": False, "why": "insufficient_swings"}

def _ensure_mid(ev):
    try:
        z = ev.get('zone')
        if z and isinstance(z, (list, tuple)) and len(z) == 2:
            ev['mid'] = float(z[0] + z[1]) / 2.0
    except Exception:
        pass
    return ev

def _ensure_mid_ev(ev: dict | None) -> dict | None:
    """
    If an evidence dict has a 'zone' = [lo, hi] but no 'mid', add mid = (lo+hi)/2.
    Safe to call with None.
    """
    if not ev or not isinstance(ev, dict):
        return ev
    try:
        z = ev.get("zone")
        if (
            z
            and isinstance(z, (list, tuple))
            and len(z) == 2
            and isinstance(z[0], (int, float))
            and isinstance(z[1], (int, float))
            and "mid" not in ev
        ):
            ev["mid"] = float(z[0] + z[1]) / 2.0
    except Exception:
        # không làm gián đoạn pipeline nếu zone lỗi
        pass
    return ev

# --------------------------------------------------------------------------------------
# 4b) Additional evidences for EARLY recognition
# --------------------------------------------------------------------------------------

def ev_mean_reversion(df: pd.DataFrame) -> Dict[str, Any]:
    """
    BB% + RSI extremes, nhưng vô hiệu khi trend mạnh:
      - ADX >= 22 hoặc |EMA50 slope| >= 0.15 * ATR
    """
    try:
        pct_bb = float(df['bb_percent'].iloc[-1])
        rsi = float(df['rsi14'].iloc[-1])
    except Exception:
        return {"ok": False, "why": "missing_bb_or_rsi"}

    atr = float(df['atr14'].iloc[-1]) if 'atr14' in df.columns else 0.0
    adx = float(df['adx14'].iloc[-1]) if 'adx14' in df.columns else None
    slope = None
    if 'ema50' in df.columns and len(df['ema50']) >= 4:
        slope = float(df['ema50'].diff().tail(3).mean())

    long_ok = (pct_bb <= 5.0) and (rsi <= 25.0)
    short_ok = (pct_bb >= 95.0) and (rsi >= 75.0)

    trend_strong = False
    if adx is not None:
        trend_strong |= adx >= 22.0
    if slope is not None and atr > 0:
        trend_strong |= abs(slope) >= 0.15 * atr

    if trend_strong:
        return {"ok": False, "why": f"trend_strong(adx={adx}, slope={slope}, atr={atr})"}

    if long_ok:
        return {"ok": True, "score": 0.8, "why": f"bb%={pct_bb:.1f}|rsi={rsi:.1f}", "side": "long", "ref": {"atr": atr}}
    if short_ok:
        return {"ok": True, "score": 0.8, "why": f"bb%={pct_bb:.1f}|rsi={rsi:.1f}", "side": "short", "ref": {"atr": atr}}
    return {"ok": False, "why": f"bb%={pct_bb:.1f}|rsi={rsi:.1f}"}


def ev_false_breakout(df: pd.DataFrame, swings: Dict[str, Any], atr: float, cfg: TFThresholds) -> Dict[str, Any]:
    """Breakout fake: price pokes above HH but closes back inside; weak follow-through volume."""
    hh = _last_swing(swings, 'HH')
    if hh is None or atr <= 0:
        return {"ok": False, "why": "no_HH_or_atr"}
    # use previous fully closed bar as "poke", last-1
    if len(df) < 3:
        return {"ok": False, "why": "insufficient_bars"}
    poke = df.iloc[-3]; last = _get_last_closed_bar(df)
    broke = bool(poke['high'] > (hh + cfg.break_buffer_atr * atr))
    failed = bool(last['close'] <= hh and last['high'] > hh)
    # weak vol on break; or reversal vol grows
    vol_break = float(poke.get('volume', 0.0))
    vs20 = float(df['vol_sma20'].iloc[-1]) if 'vol_sma20' in df.columns else max(1.0, df['volume'].tail(20).mean())
    weak = vol_break < vs20
    ok = broke and failed and weak
    return {"ok": bool(ok), "score": 0.8 if ok else 0.0, "why": "poke>HH_then_close_inside|weak_vol" if ok else "no_fakeout", "side": "short", "ref": {"hh": hh}}


def ev_false_breakdown(df: pd.DataFrame, swings: Dict[str, Any], atr: float, cfg: TFThresholds) -> Dict[str, Any]:
    """Breakdown fake: price pokes below LL but closes back inside; weak follow-through volume."""
    ll = _last_swing(swings, 'LL')
    if ll is None or atr <= 0:
        return {"ok": False, "why": "no_LL_or_atr"}
    if len(df) < 3:
        return {"ok": False, "why": "insufficient_bars"}
    poke = df.iloc[-3]; last = _get_last_closed_bar(df)
    broke = bool(poke['low'] < (ll - cfg.break_buffer_atr * atr))
    failed = bool(last['close'] >= ll and last['low'] < ll)
    vol_break = float(poke.get('volume', 0.0))
    vs20 = float(df['vol_sma20'].iloc[-1]) if 'vol_sma20' in df.columns else max(1.0, df['volume'].tail(20).mean())
    weak = vol_break < vs20
    ok = broke and failed and weak
    return {"ok": bool(ok), "score": 0.8 if ok else 0.0, "why": "poke<LL_then_close_inside|weak_vol" if ok else "no_fakeout", "side": "long", "ref": {"ll": ll}}


def ev_trend_follow_ready(df: pd.DataFrame, momentum: Dict[str, Any], trend: Dict[str, Any], side: str) -> Dict[str, Any]:
    """
    Direct trend-follow readiness using: EMA20 vs EMA50, BB%, RSI.
    side ∈ {'long','short'}
    """
    try:
        e20 = float(df['ema20'].iloc[-1]); e50 = float(df['ema50'].iloc[-1])
        pct_bb = float(df['bb_percent'].iloc[-1]) if 'bb_percent' in df.columns else 50.0
        rsi = float(momentum.get('rsi', 50.0))
        st = trend.get('state')
    except Exception:
        return {"ok": False, "why": "missing_inputs", "side": side}
    if side == 'long':
        ok = (st == 'up') and (e20 > e50) and (pct_bb >= 70.0) and (rsi >= 55.0)
    else:
        ok = (st == 'down') and (e20 < e50) and (pct_bb <= 30.0) and (rsi <= 45.0)
    return {"ok": bool(ok), "score": 0.8 if ok else 0.0, "why": f"trend={st}|ema20{'> ' if e20>e50 else '<='}ema50|bb%={pct_bb:.1f}|rsi={rsi:.1f}", "side": side}


def ev_rejection(df: pd.DataFrame, swings: Dict[str, Any], atr: float) -> Dict[str, Any]:
    """Strong wick rejection near HH/LL with wick ratio ≥ 60% of bar range."""
    if atr <= 0 or df is None or len(df) < 2:
        return {"ok": False, "why": "invalid_input"}
    last = _get_last_closed_bar(df)
    hh = _last_swing(swings, 'HH'); ll = _last_swing(swings, 'LL')
    prox = 0.2 * atr
    out = {"ok": False, "why": "no_rejection"}
    # upper rejection near HH
    if hh is not None and abs(float(last['high']) - hh) <= prox:
        rng = max(1e-9, float(last['high']) - float(last['low']))
        upper = float(last['high']) - float(last['close'])
        if (upper / rng) >= 0.6:
            return {"ok": True, "score": 0.8, "why": "upper_wick_reject@HH", "side": "short", "ref": {"hh": hh}}
    # lower rejection near LL
    if ll is not None and abs(float(last['low']) - ll) <= prox:
        rng = max(1e-9, float(last['high']) - float(last['low']))
        lower = float(last['close']) - float(last['low'])
        if (lower / rng) >= 0.6:
            return {"ok": True, "score": 0.8, "why": "lower_wick_reject@LL", "side": "long", "ref": {"ll": ll}}
    return out


def ev_divergence_updown(momentum: Dict[str, Any]) -> Dict[str, Any]:
    """Map momentum.divergence → bullish/bearish divergence with side."""
    div = momentum.get('divergence', 'none')
    if div == 'bullish':
        return {"ok": True, "score": 0.7, "why": "bullish_divergence", "side": "long"}
    if div == 'bearish':
        return {"ok": True, "score": 0.7, "why": "bearish_divergence", "side": "short"}
    return {"ok": False, "why": "no_divergence"}


def ev_compression_ready(bbw_last: float, bbw_med: float, atr_last: float) -> Dict[str, Any]:
    """Compression (squeeze) pre-break: BBW below median + ATR not rising."""
    squeeze = bool(bbw_last <= bbw_med)
    low_atr = bool(atr_last <= max(1e-9, atr_last))  # treat as low unless rising (placeholder)
    ok = squeeze and low_atr
    return {"ok": ok, "score": 0.6 if ok else 0.0, "why": "squeeze" if ok else "no_squeeze"}


def ev_volatility_breakout(vol: Dict[str, Any], bbw_last: float, bbw_med: float, atr_last: float,
                           atr_series: Optional[pd.Series] = None) -> Dict[str, Any]:
    """
    ATR-rising check uses EMA3 > EMA8 on ATR series if provided,
    otherwise falls back to atr_last > 0 (non-zero volatility).
    """
    atr_rising = False
    if isinstance(atr_series, pd.Series) and len(atr_series) >= 10:
        ema3 = atr_series.ewm(span=3, adjust=False).mean().iloc[-1]
        ema8 = atr_series.ewm(span=8, adjust=False).mean().iloc[-1]
        atr_rising = bool(ema3 > ema8)
    else:
        atr_rising = (float(atr_last or 0.0) > 0.0)
    vr = float(vol.get('vol_ratio', 1.0)); vz = float(vol.get('vol_z20', 0.0))
    bb_expand = bool(bbw_last > bbw_med)
    explosive = (vr >= 2.0) or (vz >= 2.0)
    ok = bb_expand and explosive and atr_rising
    score = 1.0 if ok and ((vr >= 3.0) or (vz >= 3.0)) else (0.8 if ok else 0.0)
    why = []
    if bb_expand: why.append("bb_expand")
    if explosive: why.append("vol_explosive")
    if atr_rising: why.append("atr_rising")
    return {"ok": ok, "score": round(score,3), "why": "|".join(why) if why else "weak"}

def build_evidence_bundle(symbol: str, features_by_tf: Dict[str, Dict[str, Any]], cfg: Config) -> Dict[str, Any]:
    f1 = features_by_tf.get('1H', {})
    f4 = features_by_tf.get('4H', {})
    fD = features_by_tf.get('1D', {})

    df1: pd.DataFrame = f1.get('df')
    df4: pd.DataFrame = f4.get('df') if f4 else None

    atr1 = float(f1.get('volatility', {}).get('atr', 0.0) or 0.0)
    atr4 = float(f4.get('volatility', {}).get('atr', 0.0) or 0.0) if f4 else 0.0
    # BBW cho 1H (trigger) và 4H (execution)
    bbw1 = f1.get('volatility', {}).get('bbw_last', 0.0)
    bbw1_med = f1.get('volatility', {}).get('bbw_med', 0.0)
    bbw4 = f4.get('volatility', {}).get('bbw_last', 0.0) if f4 else 0.0
    bbw4_med = f4.get('volatility', {}).get('bbw_med', 0.0) if f4 else 0.0

    # === ATR Regime & adaptive thresholds (theo 4H execution) ===
    regime_info = _atr_regime(df4 if df4 is not None else pd.DataFrame())
    reg = regime_info.get("regime", "normal")
    cfg_1h = _adapt_cfg(cfg.per_tf['1H'], reg) if '1H' in cfg.per_tf else None
    cfg_4h = _adapt_cfg(cfg.per_tf['4H'], reg) if '4H' in cfg.per_tf else None
    cfg_1d = _adapt_cfg(cfg.per_tf['1D'], reg) if '1D' in cfg.per_tf else None

    # Price action base (1H)
    ev_pb = ev_price_breakout(df1, f1.get('swings', {}), atr1, cfg_1h) if df1 is not None else {"ok": False}
    ev_pdn = ev_price_breakdown(df1, f1.get('swings', {}), atr1, cfg_1h) if df1 is not None else {"ok": False}
    ev_mr  = ev_mean_reversion(df1)
    ev_div = ev_divergence_updown(f1.get('momentum', {}))
    ev_rjt = ev_rejection(df1, f1.get('swings', {}), atr1)
    # --- False breakouts/breakdowns (đảo phá ngưỡng) ---
    ev_fb_out = ev_false_breakout(df1, f1.get('swings', {}), atr1, cfg_1h) if df1 is not None else {"ok": False}
    ev_fb_dn  = ev_false_breakdown(df1, f1.get('swings', {}), atr1, cfg_1h) if df1 is not None else {"ok": False}

    # --- Trend-follow readiness (hai phía) ---
    ev_tf_long  = ev_trend_follow_ready(df1, f1.get('momentum', {}), f1.get('trend', {}), side='long')  if df1 is not None else {"ok": False}
    ev_tf_short = ev_trend_follow_ready(df1, f1.get('momentum', {}), f1.get('trend', {}), side='short') if df1 is not None else {"ok": False}

    
    # --- SR reference (ref_level) thống nhất cho reclaim ---
    f1h = features_by_tf.get('1H', {}) or {}
    levels1h = f1h.get('levels', {}) or {}
    soft1h = f1h.get('soft_levels', {}) or {}
    if df1 is not None and len(df1):
        _px1h = float(df1['close'].iloc[-1])
    else:
        _px1h = float('nan')
    ref_level = pick_ref_level(levels1h, _px1h, soft_levels=soft1h) if np.isfinite(_px1h) else None

    # hint hướng từ breakout/breakdown (nếu có)
    side_hint = 'long' if ev_pb.get('ok') else ('short' if ev_pdn.get('ok') else None)

    # Reclaim (bias-free): chỉ khi có ref_level rõ ràng — buffer dùng ATR 4H
    ev_prc = {"ok": False, "why": "no_ref_level"}
    if (df1 is not None) and (ref_level is not None):
        ev_prc = ev_price_reclaim_auto(df1, level=ref_level, atr=atr4 or atr1, cfg=cfg_4h or cfg_1h)

    # Sideways (đánh giá theo 4H execution)
    ev_sdw = ev_sideways(df4 if df4 is not None else df1, bbw4 or bbw1, bbw4_med or bbw1_med, atr4 or atr1, cfg_4h or cfg_1h) if (df4 is not None or df1 is not None) else {"ok": False}

    # Volume & Momentum
    ev_vol_1h = ev_volume(f1.get('volume', {}), cfg_1h)
    ev_vol_4h = ev_volume(f4.get('volume', {}), cfg_4h) if f4 else {"ok": False}
    vol_ok = ev_vol_1h['ok'] or ev_vol_4h.get('ok', False)

    side_hint = 'long' if ev_pb.get('ok') else ('short' if ev_pdn.get('ok') else None)
    ev_mom_1h = ev_momentum(f1.get('momentum', {}), cfg_1h, side=side_hint or 'long')
    ev_tr = ev_trend_alignment(f1.get('trend', {}), f4.get('trend', {}) if f4 else None)

    # Candles & Liquidity
    ev_cdl = ev_candles(f1.get('candles', {}), side=side_hint)
    price_now = float(df1['close'].iloc[-1]) if df1 is not None else float('nan')
    vp = f4.get('vp_zones', []) if f4 else []
    if not vp and fD: vp = fD.get('vp_zones', [])
    ev_liq_ = ev_liquidity(price_now, atr4 or atr1, vp, cfg_4h or cfg.per_tf['4H'], side=side_hint)

    # New evidences
    ev_bb = ev_bb_expanding(bbw1, bbw1_med)
    # Retest zones sized theo ATR 4H để tránh quá chặt
    ev_tb = ev_throwback_valid(df1, f1.get('swings', {}), atr4 or atr1, side_hint, f1.get('candles', {})) if df1 is not None else {"ok": False}
    ev_pbk = (ev_pullback_valid(
        df1,
        f1.get('swings', {}) or {},
        atr4 or atr1,
        f1.get('momentum', {}) or {},
        f1.get('volume', {}) or {},
        f1.get('candles', {}) or {},
        side_hint
    ) if (df1 is not None and side_hint in ('long','short')) else {"ok": False})
 
    # Slow-market guards (Volatility-of-Vol & Liquidity floor)
    # Tính tỉ lệ thời gian đã trôi của nến 1H hiện tại (0..1)
    elapsed_frac = 1.0
    try:
        if df1 is not None and len(df1.index) > 0:
            last_close = pd.to_datetime(df1.index[-1])
            # 'last_close' là thời điểm đóng của nến trước; nến hiện tại bắt đầu ngay tại mốc đó
            now_utc = pd.Timestamp.utcnow()
            if getattr(last_close, "tzinfo", None) is not None:
                now_utc = now_utc.tz_localize("UTC").astimezone(last_close.tz)
            elapsed_sec = (now_utc - last_close).total_seconds()
            # 1H bar ⇒ mẫu số 3600s
            elapsed_frac = max(0.0, min(1.0, elapsed_sec / 3600.0))
    except Exception:
        elapsed_frac = 1.0
        
    # --- Volume at 1H boundary: ưu tiên dùng volume nến ĐÃ ĐÓNG trong vài phút đầu giờ ---
    vol_now_raw = float(f1.get('volume', {}).get('now', 0.0) or f1.get('volume', {}).get('v', 0.0) or 0.0)
    vol_med     = float(f1.get('volume', {}).get('median', 0.0) or 0.0)
    # Nếu feature đã cung cấp prev_closed thì dùng trực tiếp; nếu không thì truy xuất từ df1
    prev_closed_vol = f1.get('volume', {}).get('prev_closed', None)
    if prev_closed_vol is None:
        try:
            prev_closed_vol = float(_get_last_closed_bar(df1)['volume']) if (df1 is not None and len(df1) > 0) else None
        except Exception:
            prev_closed_vol = None
    # Grace window đầu nến (mặc định 8 phút, có thể override qua ENV)
    try:
        grace_min = float(os.getenv("VOLUME_GUARD_GRACE_MIN", "8"))
        grace_frac = max(0.0, min(1.0, grace_min / 60.0))
    except Exception:
        grace_frac = 0.1333  # ~8 phút
    vol_now_eff = vol_now_raw
    if (elapsed_frac <= grace_frac) and (prev_closed_vol is not None):
        # Dùng max để tránh làm giảm khối lượng nếu now đã lớn
        vol_now_eff = max(vol_now_raw, float(prev_closed_vol))

    adaptive_meta = _slow_market_guards(
        bbw1, bbw1_med,
        vol_now_eff, vol_med,
        reg,
        elapsed_frac=elapsed_frac
    )
    # Bổ sung thêm trường phục vụ log/trace
    adaptive_meta.update({
        "vol_now_eff": float(vol_now_eff),
        "prev_closed_vol": (float(prev_closed_vol) if prev_closed_vol is not None else None),
    })
 
    # Optional: volatility breakout with proper ATR slope (series vẫn theo 1H trigger)
    atr_series = df1['atr14'] if (df1 is not None and 'atr14' in df1) else None
    ev_volb = ev_volatility_breakout(f1.get('volume', {}), bbw1, bbw1_med, atr1, atr_series=atr_series)

    # ---- enrich 'volume' to expose numeric fields at top-level for logging ----
    vol_now_safe = float(vol_now_eff) if np.isfinite(vol_now_eff) else None
    vol_med_safe = float(vol_med) if np.isfinite(vol_med) else None
    vol_ratio_1h = float(ev_vol_1h.get('vol_ratio')) if isinstance(ev_vol_1h, dict) and ev_vol_1h.get('vol_ratio') is not None else None
    vol_z20_1h   = float(ev_vol_1h.get('vol_z20'))   if isinstance(ev_vol_1h, dict) and ev_vol_1h.get('vol_z20')   is not None else None
    vol_grade_1h = (ev_vol_1h.get('grade') if isinstance(ev_vol_1h, dict) else None) or ""

    # ---- mini-retest flags (4H) ----
    mini = ev_mini_retest_4h(df4, lookback_bars=3, atr_frac=0.25) if isinstance(df4, pd.DataFrame) else {"ok": False}

    evidences = {
        'price_breakout': ev_pb,
        'price_breakdown': ev_pdn,
        'price_reclaim': ev_prc,
        'sideways': ev_sdw,
        # expose primary/confirm plus summary fields for main logger
        'volume': {
            'primary': ev_vol_1h,
            'confirm': ev_vol_4h,
            'ok': bool(vol_ok),
            'vol_now': vol_now_safe,
            'vol_now_eff': vol_now_safe,
            'vol_med': vol_med_safe,
            'vol_ratio': vol_ratio_1h,
            'vol_z20': vol_z20_1h,
            'grade': vol_grade_1h,
        },
        'momentum': {'primary': ev_mom_1h},
        'trend_alignment': ev_tr,
        'candles': ev_cdl,
        'liquidity': ev_liq_,
        'bb_expanding': ev_bb,
        'throwback': ev_tb,
        'pullback': ev_pbk,
        'mean_reversion': ev_mr,
        'divergence': ev_div,
        'rejection': ev_rjt,
        'volatility_breakout': ev_volb,
        'false_breakout': ev_fb_out,
        'false_breakdown': ev_fb_dn,
        'trend_follow_ready': {'long': ev_tf_long, 'short': ev_tf_short},
        'adaptive': adaptive_meta,  # regime + slow-market guards (meta, not scored),
        'mini_retest': {
            'ok': bool(mini.get('ok', False)),
            'long': bool(mini.get('long', False)),
            'short': bool(mini.get('short', False)),
            'why': mini.get('why'),
        },
    }

    # --- Normalize zones and add 'mid' for retest-style evidences ---
    def _normalize_zone_mid(name: str):
        ev = evidences.get(name)
        if not isinstance(ev, dict):
            return
        # Promote fallback_zone -> zone if zone missing/invalid
        z = ev.get("zone")
        fz = ev.get("fallback_zone")
        if (not z or not isinstance(z, (list, tuple)) or len(z) != 2) and isinstance(fz, (list, tuple)) and len(fz) == 2:
            try:
                ev["zone"] = [float(fz[0]), float(fz[1])]
            except Exception:
                pass
        evidences[name] = _ensure_mid_ev(ev)

    _normalize_zone_mid("pullback")
    _normalize_zone_mid("throwback")

    out = {
        'symbol': symbol,
        'asof': str(df1.index[-1]) if df1 is not None else None,
        'timeframes': ['1H','4H','1D'],
        'evidence': evidences,  # không chấm state/confidence/why ở layer này
    }
    return out

# Guidance
TF_GUIDANCE: Dict[str, Dict[str, List[str]]] = {
    'price_breakout':  {'required': ['1H'], 'optional': ['4H']},
    'price_breakdown': {'required': ['1H'], 'optional': ['4H']},
    'price_reclaim':   {'required': ['1H'], 'optional': ['4H']},
    'sideways':        {'required': ['4H'], 'optional': ['1H','1D']},
    'volume':          {'required': ['1H'], 'optional': ['4H']},
    'momentum':        {'required': ['1H'], 'optional': ['4H','1D']},
    'trend_alignment': {'required': ['1H','4H'], 'optional': ['1D']},
    'candles':         {'required': ['1H'], 'optional': ['4H']},
    'liquidity':       {'required': ['4H'], 'optional': ['1D']},
    'bb_expanding':    {'required': ['1H'], 'optional': []},
    'throwback':       {'required': ['1H'], 'optional': ['4H']},
    'pullback':        {'required': ['1H'], 'optional': ['4H']},
}
