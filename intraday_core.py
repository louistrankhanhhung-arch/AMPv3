"""
Intraday Core (15m execution, 1H context, 4H guard)
---------------------------------------------------
A minimal, battle-tested signal decision module designed for fast intraday
trading in crypto. This module is intentionally independent from the current
bundle/decision engine; it only needs OHLCV DataFrames and a few light
metrics. It returns a legacy-compatible plan dict.

Pipeline:
  Evidence -> Checklist -> detect form (A/B/C/D) -> build setup -> guards

Forms (both LONG/SHORT):
  A. Flush → Reclaim → Mini-Retest (LTF reversal)
  B. Pullback HL (gentle continuation)
  C. Mean-reversion to strong MA (MA99/200 1H/1D) + reaction candle
  D. Breakout continuation (close > recent swing, vol>MA20)

Execution TF: 15m ("15m" key)
Context TF:   1H ("1H")
Guard TF:     4H ("4H")

Expected DF columns (enriched elsewhere): open, high, low, close, volume,
ema20, ema50, rsi14, atr14, bb_upper, bb_mid, bb_lower, vol_sma20, vol_ratio.
If some are missing (ema7, ema99, ema200), we compute them on the fly.

Inputs
------
decide_intraday(bundle: dict, cfg: IntradayCfg) -> dict

bundle = {
  "symbol": "BNB/USDT",
  "dfs": {"15m": df15, "1H": df1, "4H": df4},
  "market": {  # optional (guards)
      "BTC": {"1H": df_btc_1h, "4H": df_btc_4h},
      "ETH": {"1H": df_eth_1h, "4H": df_eth_4h}
  },
  "liquidity": {"spread": 0.0008, "vol_usd": 2_000_000},  # optional
}

Return (plan dict):
{
  "decision": "ENTER"|"WAIT"|"AVOID",
  "STATE": "flush_reclaim"|"pullback_hl"|"mean_rev"|"break_cont",
  "DIRECTION": "LONG"|"SHORT",
  "symbol": str,
  "entry": float,
  "sl": float,
  "tp1": float, "tp2": float, "tp3": float, "tp4": float, "tp5": float,
  "rr1": float, "rr2": float, "rr3": float, "rr4": float, "rr5": float,
  "risk_size_hint": float,   # leverage hint honoring max 2% base risk
  "notes": [str,...],        # short reasons (for UI)
  "guard": {"blocked": bool, "why": str},
  "params": {                # for runner (optional)
      "time_exit_5m_bars": 3,
      "time_exit_min_R": 0.3,
      "giveback_R": 0.3,
      "be_after_tp1": true,
      "sl_to_tp1_after_tp2": true,
      "sl_to_tp2_after_tp3": true
  }
}

Author: IMP/AMP intraday core
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple
import math
import numpy as np
import pandas as pd

# --- light EMA helpers (avoid tight coupling) -------------------------------
def _ema(series: pd.Series, span: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.ewm(span=span, adjust=False).mean()

# --- safe dataframe access --------------------------------------------------
def _last_closed(df: pd.DataFrame) -> Optional[pd.Series]:
    try:
        if df is None or len(df) == 0:
            return None
        return df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
    except Exception:
        return None

@dataclass
class IntradayCfg:
    # Market-wide simple guard
    allow_when_btc_dumping: bool = False   # if False, block when BTC 1H/4H both red & momentum down
    btc_dump_rsi: float = 45.0             # BTC 1H RSI below => treat as risk-off unless strong setup A
    eth_confirm_weight: float = 0.4        # ETH adds confirmation if aligned with BTC

    # Liquidity filter
    min_vol_usd: float = 500_000           # avoid illiquid
    max_spread: float = 0.0025             # 0.25%

    # Regime (NATR proxy: atr/close)
    max_natr_for_normal: float = 0.06      # if above, only allow setup A (flush-reclaim)

    # Execution params
    rr_ladder: Tuple[float, float, float, float, float] = (0.9, 1.6, 2.4, 3.2, 4.0)
    time_exit_5m_bars: int = 3
    time_exit_min_R: float = 0.3
    giveback_R: float = 0.3

    # Risk cap: SL distance should imply <= 2% base risk at advice leverage
    max_sl_pct_of_capital: float = 0.02
    default_lev: float = 3.0

# ----------------------- utilities -----------------------------------------

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "ema7" not in out.columns:
        out["ema7"] = _ema(out["close"], 7)
    if "ema99" not in out.columns:
        out["ema99"] = _ema(out["close"], 99)
    if "ema200" not in out.columns:
        out["ema200"] = _ema(out["close"], 200)
    if "vol_sma20" not in out.columns:
        out["vol_sma20"] = pd.to_numeric(out["volume"], errors="coerce").rolling(20).mean()
    if "vol_ratio" not in out.columns:
        out["vol_ratio"] = out["volume"] / out["vol_sma20"]
    if "atr14" not in out.columns:
        # ATR proxy using high-low range if TR not available (acceptable on 15m)
        rng = (out["high"] - out["low"]).abs()
        out["atr14"] = rng.rolling(14).mean()
    return out


def _natr(df: pd.DataFrame) -> float:
    try:
        last = _last_closed(df)
        if last is None:
            return 0.0
        atr = float(last.get("atr14", 0.0) or 0.0)
        close = float(last.get("close", 0.0) or 0.0)
        return float(atr / close) if close else 0.0
    except Exception:
        return 0.0


def _aligned(a: float, b: float, eps: float = 0.0) -> bool:
    return (a - 50.0) * (b - 50.0) >= -eps


def _rsi(df: pd.DataFrame) -> Optional[float]:
    try:
        s = _last_closed(df)
        if s is None:
            return None
        for c in ("rsi14", "RSI", "rsi"):
            if c in s.index:
                return float(s[c])
    except Exception:
        return None
    return None

# ----------------------- setup detectors -----------------------------------

def _detect_A_flush_reclaim(df15: pd.DataFrame, df1h: pd.DataFrame) -> Tuple[bool, str, Optional[str]]:
    """Detect LTF flush → reclaim → mini-retest.
    Heuristics:
      - sweep last swing low/high within 2*atr15, close back above/below (reclaim)
      - immediate BOS: current close > last minor swing high (for long) or < swing low (for short)
      - volume spike on the reclaim bar: vol_ratio >= 1.5
    Returns: (ok, state_name, direction)
    """
    df15 = _ensure_cols(df15)
    last = _last_closed(df15)
    prev = df15.iloc[-3] if len(df15) >= 3 else last
    if last is None:
        return False, "", None

    atr15 = float(last.get("atr14", 0.0) or 0.0)
    hi_last, lo_last, cl_last, op_last = [float(last.get(k, last["close"])) for k in ("high","low","close","open")]
    hi_prev, lo_prev, cl_prev = [float(prev.get(k, prev["close"])) for k in ("high","low","close")]

    # Sweep & reclaim
    swept_down = (lo_last < lo_prev - 0.1 * atr15) and (cl_last > lo_prev)
    swept_up   = (hi_last > hi_prev + 0.1 * atr15) and (cl_last < hi_prev)

    vol_ratio = float(last.get("vol_ratio", 0.0) or 0.0)
    vol_ok = vol_ratio >= 1.5

    # BOS proxy: close beyond previous close/extreme in direction of reclaim
    bos_long = cl_last > max(cl_prev, hi_prev)
    bos_short = cl_last < min(cl_prev, lo_prev)

    if vol_ok and swept_down and bos_long:
        return True, "flush_reclaim", "LONG"
    if vol_ok and swept_up and bos_short:
        return True, "flush_reclaim", "SHORT"
    return False, "", None


def _detect_B_pullback_hl(df15: pd.DataFrame, df1h: pd.DataFrame) -> Tuple[bool, str, Optional[str], Optional[float]]:
    """Detect gentle continuation pullback HL.
    Conditions:
      - 1H had an impulse: ema20_1h > ema50_1h for long (or < for short)
      - price pulls back towards EMA7/20 1H
      - falling volume during pullback (last vol_ratio <= 1.0)
    Returns: (ok, state_name, direction, ref_ema_for_entry)
    """
    df1h = _ensure_cols(df1h)
    last1 = _last_closed(df1h)
    if last1 is None:
        return False, "", None, None
    e20, e50 = float(last1["ema20"]), float(last1["ema50"])
    dir_long = e20 > e50
    dir_short = e20 < e50

    # proximity to EMA7/20 1H
    ema7_1h = float(last1.get("ema7", e20))
    ema20_1h = e20
    close_1h = float(last1["close"]) if "close" in last1 else None

    vol_ok = float(last1.get("vol_ratio", 1.0) or 1.0) <= 1.05

    if dir_long and vol_ok and close_1h >= ema20_1h * 0.98:
        return True, "pullback_hl", "LONG", ema20_1h
    if dir_short and vol_ok and close_1h <= ema20_1h * 1.02:
        return True, "pullback_hl", "SHORT", ema20_1h
    return False, "", None, None


def _detect_C_mean_rev(df15: pd.DataFrame, df1h: pd.DataFrame) -> Tuple[bool, str, Optional[str], Optional[str]]:
    """Mean-reversion toward strong MA99/200 on 1H.
    Requires a reaction candle (pin/hammer-like): lower wick for long, upper wick for short.
    Returns: (ok, state_name, direction, which_ma) with which_ma in {"ema99","ema200"}.
    """
    df1h = _ensure_cols(df1h)
    last = _last_closed(df1h)
    if last is None:
        return False, "", None, None
    ema99 = float(last.get("ema99", np.nan))
    ema200 = float(last.get("ema200", np.nan))
    close = float(last.get("close", np.nan))
    open_ = float(last.get("open", np.nan))
    high = float(last.get("high", np.nan))
    low = float(last.get("low", np.nan))

    # wick heuristics
    body = abs(close - open_)
    lower_wick = max(0.0, min(close, open_) - low)
    upper_wick = max(0.0, high - max(close, open_))

    # near MA within 0.8 * ATR(1H)
    atr1h = float(last.get("atr14", 0.0) or 0.0)
    tol = 0.8 * atr1h if atr1h else 0.0

    near99 = math.isfinite(ema99) and abs(close - ema99) <= tol
    near200 = math.isfinite(ema200) and abs(close - ema200) <= tol

    if near99 or near200:
        # reaction
        if lower_wick > 0.6 * body:
            return True, "mean_rev", "LONG", "ema99" if near99 else "ema200"
        if upper_wick > 0.6 * body:
            return True, "mean_rev", "SHORT", "ema99" if near99 else "ema200"
    return False, "", None, None


def _detect_D_break_cont(df15: pd.DataFrame, df1h: pd.DataFrame) -> Tuple[bool, str, Optional[str]]:
    """Breakout continuation (close > swing high + vol>MA20). For short: symmetric.
    """
    df15 = _ensure_cols(df15)
    last = _last_closed(df15)
    if last is None:
        return False, "", None
    vol_ok = float(last.get("vol_ratio", 0.0) or 0.0) >= 1.2

    # simple swing proxy: use previous 10-bar highs/lows
    win = 10
    try:
        hiN = float(pd.to_numeric(df15["high"], errors="coerce").tail(win).max())
        loN = float(pd.to_numeric(df15["low"], errors="coerce").tail(win).min())
    except Exception:
        return False, "", None

    cl = float(last.get("close", 0.0) or 0.0)
    if vol_ok and cl > hiN:
        return True, "break_cont", "LONG"
    if vol_ok and cl < loN:
        return True, "break_cont", "SHORT"
    return False, "", None

# ----------------------- guards --------------------------------------------

def _guard_market(bundle: dict, direction: Optional[str], cfg: IntradayCfg) -> Tuple[bool, str]:
    mk = bundle.get("market") or {}
    if not mk or cfg.allow_when_btc_dumping:
        return False, ""
    try:
        btc1 = mk.get("BTC", {}).get("1H")
        btc4 = mk.get("BTC", {}).get("4H")
        if btc1 is None or btc4 is None:
            return False, ""
        r1 = _rsi(btc1) or 50.0
        r4 = _rsi(btc4) or 50.0
        # block if both are weak and direction is LONG (risk-on)
        if direction == "LONG" and (r1 < cfg.btc_dump_rsi and r4 < 50.0):
            return True, f"BTC weak (RSI1H={r1:.1f}, RSI4H={r4:.1f})"
    except Exception:
        pass
    return False, ""


def _guard_liquidity(bundle: dict, cfg: IntradayCfg) -> Tuple[bool, str]:
    liq = bundle.get("liquidity") or {}
    vol_usd = float(liq.get("vol_usd") or 0.0)
    spread = float(liq.get("spread") or 0.0)
    if vol_usd and vol_usd < cfg.min_vol_usd:
        return True, f"low liquidity ({vol_usd:.0f}usd)"
    if spread and spread > cfg.max_spread:
        return True, f"wide spread ({spread*100:.2f}%)"
    return False, ""


def _guard_regime(df15: pd.DataFrame, form: str, cfg: IntradayCfg) -> Tuple[bool, str]:
    natr = _natr(df15)
    if natr > cfg.max_natr_for_normal and form != "flush_reclaim":
        return True, f"high NATR {natr*100:.1f}% → only allow flush-reclaim"
    return False, ""

# ----------------------- entry/SL/TP builder -------------------------------

def _ladder(side: str, entry: float, sl: float, rr: Tuple[float,...]) -> Tuple[Tuple[float,...], Tuple[float,...]]:
    tps = []
    rrs = []
    risk = (entry - sl) if side == "LONG" else (sl - entry)
    risk = max(risk, 1e-9)
    for r in rr:
        if side == "LONG":
            tps.append(entry + r * risk)
        else:
            tps.append(entry - r * risk)
        rrs.append(r)
    return tuple(tps), tuple(rrs)


def _adv_lev(entry: float, sl: float, cfg: IntradayCfg) -> float:
    """Advice leverage to cap SL loss ≤ 2% base capital.
    If (|entry-sl|/entry)*lev <= 2% ⇒ lev <= 0.02 * entry / |entry-sl|
    """
    try:
        gap_pct = abs(entry - sl) / entry
        if gap_pct <= 0:
            return cfg.default_lev
        max_lev = cfg.max_sl_pct_of_capital / gap_pct
        return float(max(1.0, min(cfg.default_lev, max_lev)))
    except Exception:
        return cfg.default_lev

# ----------------------- main API ------------------------------------------

def decide_intraday(bundle: Dict[str, Any], cfg: Optional[IntradayCfg] = None) -> Dict[str, Any]:
    cfg = cfg or IntradayCfg()
    sym = str(bundle.get("symbol") or "?")
    dfs = bundle.get("dfs") or {}
    df15 = _ensure_cols(dfs.get("15m")) if dfs.get("15m") is not None else None
    df1h = _ensure_cols(dfs.get("1H")) if dfs.get("1H") is not None else None
    df4h = _ensure_cols(dfs.get("4H")) if dfs.get("4H") is not None else None

    if df15 is None or df1h is None:
        return {"decision": "AVOID", "symbol": sym, "why": "missing 15m/1H data"}

    notes = []

    # 1) detect forms (priority: A > D > B > C)
    ok, state, direction = False, "", None

    a_ok, a_state, a_dir = _detect_A_flush_reclaim(df15, df1h)
    if a_ok:
        ok, state, direction = True, a_state, a_dir
        notes.append("A: flush→reclaim BOS + vol spike")
    else:
        d_ok, d_state, d_dir = _detect_D_break_cont(df15, df1h)
        if d_ok:
            ok, state, direction = True, d_state, d_dir
            notes.append("D: breakout continuation + vol")
        else:
            b_ok, b_state, b_dir, b_ref = _detect_B_pullback_hl(df15, df1h)
            if b_ok:
                ok, state, direction = True, b_state, b_dir
                notes.append("B: pullback HL to EMA1H")
            else:
                c_ok, c_state, c_dir, which_ma = _detect_C_mean_rev(df15, df1h)
                if c_ok:
                    ok, state, direction = True, c_state, c_dir
                    notes.append(f"C: mean-revert to {which_ma}")

    if not ok or direction is None:
        return {"decision": "WAIT", "symbol": sym, "why": "no form matched"}

    # 2) guards
    # 2a) market guard
    g_block, g_why = _guard_market(bundle, direction, cfg)
    if g_block:
        return {"decision": "WAIT", "symbol": sym, "why": g_why}

    # 2b) liquidity guard
    l_block, l_why = _guard_liquidity(bundle, cfg)
    if l_block:
        return {"decision": "AVOID", "symbol": sym, "why": l_why}

    # 2c) regime (NATR) guard
    r_block, r_why = _guard_regime(df15, state, cfg)
    if r_block:
        return {"decision": "WAIT", "symbol": sym, "why": r_why}

    # 3) build setup (entry/SL)
    last15 = _last_closed(df15)
    if last15 is None:
        return {"decision": "AVOID", "symbol": sym, "why": "no 15m bar"}
    cl15 = float(last15["close"])
    hi15 = float(last15.get("high", cl15))
    lo15 = float(last15.get("low", cl15))
    atr15 = float(last15.get("atr14", 0.0) or 0.0)

    # entries for each form (simplified)
    if state == "flush_reclaim":
        entry = cl15  # market at reclaim confirmation; caller may convert to limit at mini-retest band
        if direction == "LONG":
            sl = min(lo15, cl15 - 0.8 * atr15)
        else:
            sl = max(hi15, cl15 + 0.8 * atr15)
    elif state == "pullback_hl":
        e20_1h = float(_last_closed(df1h).get("ema20", cl15))
        entry = e20_1h  # limit into EMA20 1H
        if direction == "LONG":
            sl = entry - 0.8 * (float(_last_closed(df1h).get("atr14", atr15)) or atr15)
        else:
            sl = entry + 0.8 * (float(_last_closed(df1h).get("atr14", atr15)) or atr15)
    elif state == "mean_rev":
        last1 = _last_closed(df1h)
        ema_ref = float(last1.get("ema99" if direction == "LONG" else "ema99", last1.get("ema200", cl15)))
        entry = ema_ref
        if direction == "LONG":
            sl = min(lo15, entry - 1.0 * atr15)
        else:
            sl = max(hi15, entry + 1.0 * atr15)
    else:  # break_cont
        entry = cl15
        if direction == "LONG":
            sl = min(lo15, cl15 - 1.0 * atr15)
        else:
            sl = max(hi15, cl15 + 1.0 * atr15)

    # sanitize
    if not (math.isfinite(entry) and math.isfinite(sl)):
        return {"decision": "WAIT", "symbol": sym, "why": "invalid entry/sl"}

    # 4) TP ladder & leverage hint
    tps, rrs = _ladder(direction, entry, sl, cfg.rr_ladder)
    lev = _adv_lev(entry, sl, cfg)

    plan = {
        "decision": "ENTER",
        "STATE": state,
        "DIRECTION": direction,
        "symbol": sym,
        "entry": float(entry),
        "sl": float(sl),
        "tp1": float(tps[0]), "tp2": float(tps[1]), "tp3": float(tps[2]),
        "tp4": float(tps[3]), "tp5": float(tps[4]),
        "rr1": float(rrs[0]), "rr2": float(rrs[1]), "rr3": float(rrs[2]), "rr4": float(rrs[3]), "rr5": float(rrs[4]),
        "risk_size_hint": float(lev),
        "notes": notes,
        "guard": {"blocked": False, "why": ""},
        "params": {
            "time_exit_5m_bars": cfg.time_exit_5m_bars,
            "time_exit_min_R": cfg.time_exit_min_R,
            "giveback_R": cfg.giveback_R,
            "be_after_tp1": True,
            "sl_to_tp1_after_tp2": True,
            "sl_to_tp2_after_tp3": True,
        },
    }
    return plan


# Convenience alias used by external engine
class IntradayCore:
    def __init__(self, cfg: Optional[IntradayCfg] = None):
        self.cfg = cfg or IntradayCfg()
    def decide(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        return decide_intraday(bundle, self.cfg)
