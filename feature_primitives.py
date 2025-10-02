"""
feature_primitives.py
---------------------
Feature primitives tách rời để dễ debug & tái sử dụng.

• Cụm Xu Hướng (trend)
    - compute_swings(df, pct=2.0, max_keep=20, last_n_each=3)
    - compute_trend(df)
    - compute_candles(df)

• Cụm Động Lượng (momentum)
    - compute_volume_features(df)
    - compute_momentum(df)
    - compute_volatility(df, bbw_lookback=50)

• Cụm SR (support/resistance)
    - compute_levels(df, atr=None, tol_coef=0.5, extremes=12, lookback=300)
    - compute_soft_levels(df)

• Tổng hợp đa khung thời gian
    - compute_features_by_tf(dfs_by_tf: Dict[str, pd.DataFrame]) -> Dict

YÊU CẦU CỘT (đã enrich trước):
open, high, low, close, volume, ema20, ema50, rsi14, atr14,
bb_upper, bb_mid, bb_lower, vol_sma20, vol_ratio, vol_z20,
(body_pct, upper_wick_pct, lower_wick_pct) nếu muốn mẫu nến chuẩn.
"""
from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd
from indicators import calc_vp

# =============================
# Helpers chung
# =============================

def _last_closed_bar(df: pd.DataFrame) -> pd.Series:
    """Trả về nến đã đóng cuối cùng nếu có >=2 nến, ngược lại lấy nến cuối.
    Dùng cho pattern candle để an toàn trong streaming.
    """
    if df is None or len(df) == 0:
        raise ValueError("empty df")
    if len(df) >= 2:
        return df.iloc[-2]
    return df.iloc[-1]


# =============================
# CỤM XU HƯỚNG
# =============================

def _zigzag(series: pd.Series, pct: float = 2.0) -> List[Tuple[pd.Timestamp, float]]:
    """ZigZag đơn giản theo % (trên chuỗi close). Trả [(t, price), ...]."""
    pts: List[Tuple[pd.Timestamp, float]] = []
    if series is None or series.empty:
        return pts

    last_ext = float(series.iloc[0])
    last_t = series.index[0]
    direction = 0  # 1 up, -1 down, 0 none

    for t, v in series.items():
        v = float(v)
        change_pct = (v - last_ext) / last_ext * 100 if last_ext != 0 else 0
        if direction >= 0 and change_pct >= pct:
            pts.append((last_t, last_ext))
            last_ext, last_t, direction = v, t, 1
        elif direction <= 0 and change_pct <= -pct:
            pts.append((last_t, last_ext))
            last_ext, last_t, direction = v, t, -1
        else:
            if (direction >= 0 and v > last_ext) or (direction <= 0 and v < last_ext):
                last_ext, last_t = v, t

    pts.append((last_t, last_ext))
    return pts


def compute_swings(
    df: pd.DataFrame,
    pct: float = 2.0,
    *,
    lookback: int = 250,
    max_keep: int = 20,
    last_n_each: int = 3,
) -> Dict[str, Any]:
    """Tạo danh sách HH/LL từ ZigZag.

    Args:
        pct: ngưỡng ZigZag theo %.
        lookback: số nến lấy để tính.
        max_keep: giới hạn tổng số swing lưu lại để nhẹ payload.
        last_n_each: số lượng HH/LL gần nhất muốn trích riêng (ví dụ 3HH-3LL như bạn đề xuất).
    Returns:
        {
          'swings': [{'type': 'HH'|'LL', 't': str, 'price': float}, ...],
          'last_HH': [float,...],
          'last_LL': [float,...]
        }
    """
    series = df['close'].tail(lookback)
    zz = _zigzag(series, pct=pct)
    out: List[Dict[str, Any]] = []
    for i in range(1, len(zz)):
        prev, curr = zz[i - 1][1], zz[i][1]
        t = zz[i][0]
        out.append({
            "type": "HH" if curr > prev else "LL",
            "t": str(t),
            "price": float(curr)
        })
    swings = out[-max_keep:]

    # Trích 3HH/3LL (mặc định)
    last_HH = [s['price'] for s in reversed(swings) if s['type'] == 'HH'][:last_n_each]
    last_LL = [s['price'] for s in reversed(swings) if s['type'] == 'LL'][:last_n_each]

    return {"swings": swings, "last_HH": last_HH, "last_LL": last_LL}


def compute_trend(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.sort_index()
    e20, e50 = float(df['ema20'].iloc[-1]), float(df['ema50'].iloc[-1])
    spread = e20 - e50
    if e20 > e50:
        state = "up"
    elif e20 < e50:
        state = "down"
    else:
        state = "side"
    ema50 = df['ema50'].tail(4)
    ema50_slope = float(ema50.diff().tail(3).mean())  # slope mượt 3 nến
    return {
        "state": state, "ema20": e20, "ema50": e50,
        "ema_spread": spread, "ema50_slope": ema50_slope
    }


def compute_candles(df: pd.DataFrame) -> Dict[str, bool]:
    last = _last_closed_bar(df)
    prev = df.iloc[-3] if len(df) >= 3 else last

    body = float(last.get('body_pct', 0.0))
    uw = float(last.get('upper_wick_pct', 0.0))
    lw = float(last.get('lower_wick_pct', 0.0))

    green = bool(last['close'] > last['open'])
    red = bool(last['close'] < last['open'])

    bullish_pin = (lw >= 50) and (body <= 30) and green
    bearish_pin = (uw >= 50) and (body <= 30) and red

    bull_engulf = (
        green
        and prev['close'] < prev['open']
        and last['close'] > prev['open']
        and last['open'] < prev['close']
    )
    bear_engulf = (
        red
        and prev['close'] > prev['open']
        and last['close'] < prev['open']
        and last['open'] > prev['close']
    )

    inside = (last['high'] <= prev['high']) and (last['low'] >= prev['low'])

    return {
        "bullish_pin": bool(bullish_pin),
        "bearish_pin": bool(bearish_pin),
        "bullish_engulf": bool(bull_engulf),
        "bearish_engulf": bool(bear_engulf),
        "inside_bar": bool(inside),
    }


# =============================
# CỤM ĐỘNG LƯỢNG
# =============================

def compute_volume_features(df: pd.DataFrame) -> Dict[str, Any]:
    vr = float(df['vol_ratio'].iloc[-1]) if 'vol_ratio' in df.columns else 1.0
    vz = float(df['vol_z20'].iloc[-1]) if 'vol_z20' in df.columns else 0.0

    v3 = float(df['volume'].tail(3).mean())
    v5 = float(df['volume'].tail(5).mean())
    v10 = float(df['volume'].tail(10).mean())
    v20 = float(df['vol_sma20'].iloc[-1]) if 'vol_sma20' in df.columns else v10

    contraction = (v5 < v10) and (v10 < v20)
    now = float(df['volume'].iloc[-1]) if len(df) else 0.0
    # Khối lượng nến đã đóng gần nhất (tránh now=0 ở đầu nến do stream)
    try:
        prev_closed = float(_last_closed_bar(df).get('volume', 0.0))
    except Exception:
        prev_closed = 0.0
    median = float(df['volume'].tail(20).median()) if len(df) else 0.0

    return {
        "vol_ratio": vr,
        "vol_z20": vz,
        "v3": v3, "v5": v5, "v10": v10, "v20": v20,
        "now": now, "prev_closed": prev_closed, "median": median,
        "contraction": bool(contraction),
        "break_vol_ok": bool((vr >= 1.5) or (vz >= 1.0)),
        "break_vol_strong": bool((vr >= 2.0) or (vz >= 2.0)),
    }


def compute_momentum(df: pd.DataFrame) -> Dict[str, Any]:
    price = df['close'].tail(30)
    rsi = df['rsi14'].tail(30) if 'rsi14' in df.columns else pd.Series([50])
    rsi_last = float(rsi.iloc[-1]) if len(rsi) else 50.0

    div = "none"
    if len(price) >= 3 and len(rsi) >= 3:
        if price.iloc[-1] >= price.max() - 1e-9 and rsi.iloc[-1] < rsi.max() - 1e-9:
            div = "bearish"
        elif price.iloc[-1] <= price.min() + 1e-9 and rsi.iloc[-1] > rsi.min() + 1e-9:
            div = "bullish"

    return {"rsi": rsi_last, "divergence": div}


def compute_volatility(df: pd.DataFrame, bbw_lookback: int = 50) -> Dict[str, Any]:
    atr = float(df['atr14'].iloc[-1]) if 'atr14' in df.columns else 0.0
    close_last = float(df['close'].iloc[-1]) if 'close' in df.columns and len(df) else float('nan')
    natr = (atr / close_last) if (close_last and close_last != 0) else float('nan')

    if 'bb_width_pct' in df.columns and pd.notna(df['bb_width_pct'].iloc[-1]):
        bbw_series = df['bb_width_pct'].copy()
    else:
        upper = df['bb_upper']; lower = df['bb_lower']; mid = df['bb_mid']
        base = mid.where(mid.abs() > 1e-12, other=df['close'])
        bbw_series = ((upper - lower) / base.abs()) * 100.0
        bbw_series = bbw_series.replace([np.inf, -np.inf], np.nan)

    bbw_med = float(bbw_series.tail(bbw_lookback).median(skipna=True)) if len(bbw_series) else 0.0
    bbw_last = float(bbw_series.iloc[-1]) if len(bbw_series) else 0.0
    squeeze = bool(bbw_last < bbw_med) if bbw_med > 0 else False

    natr = (atr / close_last) if (isinstance(close_last, (int,float)) and close_last) else float('nan')
    return {"atr": atr, "natr": natr, "bbw_last": bbw_last, "bbw_med": bbw_med, "squeeze": squeeze}


# =============================
# CỤM SR
# =============================

def compute_levels(
    df: pd.DataFrame,
    atr: Optional[float] = None,
    *,
    tol_coef: float = 0.5,
    extremes: int = 12,
    lookback: int = 300,
    vp_zones: Optional[List[Dict[str, Any]]] = None,
    weights: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """SR cứng từ local HL + extreme closes, cluster bởi tol = tol_coef*ATR.
    Bổ sung: touch count, dwell time, confluence psych level, volume-profile weight,
    và score/strength để xếp hạng band phục vụ ENTRY/TP/SL.

    Args:
        atr: ATR hiện tại; nếu None sẽ lấy từ df['atr14'].
        tol_coef: hệ số cluster theo ATR (mặc định 0.5).
        extremes: số lượng extremes (close) hai phía để lấy ứng viên.
        lookback: số nến dùng để tính SR.
        vp_zones: danh sách vùng volume profile (nếu có), mỗi phần tử dạng
                  {"price_range": (lo, hi), "volume_sum": float}.
        weights: trọng số để tính điểm band, keys: touch/dwell/psych/vp.

    Returns:
        {
          "sr_up": [...], "sr_down": [...],
          "bands_up": [{"band":[lo,hi], "tp":tp, "touches":int, "dwell":int,
                         "psych_conf":0..1, "vp_weight":0..1,
                         "score":0..1, "strength":"weak|medium|strong"}, ...],
          "bands_down": [...],
          "tol": float,
        }
    """
    import math

    # ---------- Helpers (local to function) ----------
    def level_stats(sub_df: pd.DataFrame, level: float, tol: float) -> Tuple[int, int]:
        hi, lo = sub_df['high'], sub_df['low']
        in_band = (lo <= level + tol) & (hi >= level - tol)
        touches = int(((~in_band.shift(1, fill_value=False)) & in_band).sum())
        close_band = sub_df['close'].between(level - 0.3*tol, level + 0.3*tol)
        dwell = int(close_band.sum())
        return touches, dwell

    def round_step(price: float) -> float:
        p = max(price, 1e-9)
        p10 = 10 ** math.floor(math.log10(p))
        for m in (1, 2, 5):
            step = m * p10
            if p / step < 20:
                return step
        return 10 * p10

    def psych_confluence(level: float, tol: float) -> float:
        step = round_step(level)
        nearest = round(level / step) * step
        dist = abs(level - nearest)
        return max(0.0, 1.0 - dist / max(tol, 1e-9))

    def overlap_ratio(a_lo: float, a_hi: float, b_lo: float, b_hi: float) -> float:
        inter = max(0.0, min(a_hi, b_hi) - max(a_lo, b_lo))
        base = max(1e-9, a_hi - a_lo)
        return inter / base

    def vp_band_weight(band: Tuple[float, float], zones: Optional[List[Dict[str, Any]]]) -> float:
        if not zones:
            return 0.0
        vmax = max((float(z.get('volume_sum', 0.0)) for z in zones), default=0.0) or 1.0
        lo, hi = band
        best = 0.0
        for z in zones:
            zlo, zhi = float(z['price_range'][0]), float(z['price_range'][1])
            r = overlap_ratio(lo, hi, zlo, zhi)
            v = float(z.get('volume_sum', 0.0)) / vmax
            best = max(best, r * v)  # overlap * normalized volume strength
        return max(0.0, min(1.0, best))

    def make_bands(levels: List[float], tol: float) -> List[List[float]]:
        bands: List[List[float]] = []
        for p in levels:
            if not bands or abs(p - bands[-1][-1]) > tol:
                bands.append([p])
            else:
                bands[-1].append(p)
        return bands

    def enrich_bands(bands: List[List[float]], sub_df: pd.DataFrame, tol: float) -> List[Dict[str, Any]]:
        raw: List[Dict[str, Any]] = []
        for grp in bands:
            lo, hi = min(grp), max(grp)
            tp = round((lo + hi) / 2.0, 2)
            touches, dwell = level_stats(sub_df, tp, tol)
            psych = psych_confluence(tp, tol)
            vpw = vp_band_weight((lo, hi), vp_zones)
            raw.append({
                "band": [lo, hi], "tp": tp,
                "touches": touches, "dwell": dwell,
                "psych_conf": round(psych, 3),
                "vp_weight": round(vpw, 3),
            })
        # Normalize touches/dwell to 0..1
        max_t = max((b['touches'] for b in raw), default=0) or 1
        max_d = max((b['dwell'] for b in raw), default=0) or 1
        for b in raw:
            b['norm_touch'] = b['touches'] / max_t
            b['norm_dwell'] = b['dwell'] / max_d
        # Scoring
        w = {"touch": 0.35, "dwell": 0.20, "psych": 0.25, "vp": 0.20}
        if weights:
            w.update({k: float(v) for k, v in weights.items() if k in w})
        for b in raw:
            score = (
                w['touch'] * b['norm_touch'] +
                w['dwell'] * b['norm_dwell'] +
                w['psych'] * b['psych_conf'] +
                w['vp'] * b['vp_weight']
            )
            b['score'] = round(float(score), 3)
            b['strength'] = 'strong' if b['score'] >= 0.7 else ('medium' if b['score'] >= 0.5 else 'weak')
            # cleanup norms from output (optional)
            del b['norm_touch']; del b['norm_dwell']
        # sort by score desc
        raw.sort(key=lambda x: x['score'], reverse=True)
        return raw

    # ---------- Core computation ----------
    sub = df.tail(lookback)
    px = float(sub['close'].iloc[-1])

    if atr is None:
        atr = float(sub['atr14'].iloc[-1]) if 'atr14' in sub.columns else 0.0
    tol = max(atr * tol_coef, 1e-6)

    highs = sub['high']; lows = sub['low']
    loc_high = highs[(highs.shift(1) < highs) & (highs.shift(-1) < highs)]
    loc_low = lows[(lows.shift(1) > lows) & (lows.shift(-1) > lows)]

    closes = sub['close']
    extreme_up = closes.nlargest(extremes).tolist()
    extreme_dn = closes.nsmallest(extremes).tolist()

    cands: List[float] = []
    cands += [float(x) for x in loc_high.dropna().tolist()]
    cands += [float(x) for x in loc_low.dropna().tolist()]
    cands += [float(x) for x in extreme_up if np.isfinite(x)]
    cands += [float(x) for x in extreme_dn if np.isfinite(x)]

    cands = sorted(set(round(x, 4) for x in cands))

    # one-pass cluster with tolerance
    merged: List[float] = []
    for p in cands:
        if not merged:
            merged.append(p)
            continue
        if abs(p - merged[-1]) <= tol:
            merged[-1] = (merged[-1] + p) / 2.0
        else:
            merged.append(p)

    sr_up = sorted({x for x in merged if x > px})
    sr_dn = sorted({x for x in merged if x < px})

    bands_up = make_bands(sr_up, tol)
    bands_dn = make_bands(sr_dn, tol)

    enriched_up = enrich_bands(bands_up, sub, tol)
    enriched_dn = enrich_bands(bands_dn, sub, tol)

    return {
        "sr_up": [round(x, 4) for x in sr_up],
        "sr_down": [round(x, 4) for x in sr_dn],
        "bands_up": enriched_up,
        "bands_down": enriched_dn,
        "tol": float(tol),
    }

    if atr is None:
        atr = float(sub['atr14'].iloc[-1]) if 'atr14' in sub.columns else 0.0
    tol = max(atr * tol_coef, 1e-6)

    highs = sub['high']
    lows = sub['low']
    loc_high = highs[(highs.shift(1) < highs) & (highs.shift(-1) < highs)]
    loc_low = lows[(lows.shift(1) > lows) & (lows.shift(-1) > lows)]

    closes = sub['close']
    extreme_up = closes.nlargest(extremes).tolist()
    extreme_dn = closes.nsmallest(extremes).tolist()

    cands: List[float] = []
    cands += [float(x) for x in loc_high.dropna().tolist()]
    cands += [float(x) for x in loc_low.dropna().tolist()]
    cands += [float(x) for x in extreme_up if np.isfinite(x)]
    cands += [float(x) for x in extreme_dn if np.isfinite(x)]

    cands = sorted(set(round(x, 4) for x in cands))

    # cluster 1D theo tol
    merged: List[float] = []
    for p in cands:
        if not merged:
            merged.append(p)
            continue
        if abs(p - merged[-1]) <= tol:
            merged[-1] = (merged[-1] + p) / 2.0
        else:
            merged.append(p)

    sr_up = sorted({x for x in merged if x > px})
    sr_dn = sorted({x for x in merged if x < px})

    # dựng bands (loose grouping)
    def _bands(levels: List[float]) -> List[Dict[str, Any]]:
        bands: List[List[float]] = []
        for p in levels:
            if not bands or abs(p - bands[-1][-1]) > tol:
                bands.append([p])
            else:
                bands[-1].append(p)
        out = []
        for grp in bands:
            lo, hi = min(grp), max(grp)
            tp = round((lo + hi) / 2.0, 2)
            out.append({"band": [lo, hi], "tp": tp})
        return out

    return {
        "sr_up": [round(x, 4) for x in sr_up],
        "sr_down": [round(x, 4) for x in sr_dn],
        "bands_up": _bands(sr_up),
        "bands_down": _bands(sr_dn),
        "tol": tol,
    }


def compute_soft_levels(df: pd.DataFrame) -> Dict[str, List[Dict[str, float]]]:
    last = df.iloc[-1]
    px = float(last['close'])
    candidates = {
        "BB.upper": float(last['bb_upper']),
        "BB.mid": float(last['bb_mid']),
        "BB.lower": float(last['bb_lower']),
        "EMA20": float(last['ema20']),
        "EMA50": float(last['ema50']),
        "SMA20": float(last.get('sma20', last['ema20'])),
        "SMA50": float(last.get('sma50', last['ema50'])),
    }
    up, dn = [], []
    for name, lvl in candidates.items():
        if not np.isfinite(lvl):
            continue
        if lvl > px:
            up.append((name, lvl))
        elif lvl < px:
            dn.append((name, lvl))
    up = [dict(name=n, level=l) for n, l in sorted(up, key=lambda x: x[1])]
    dn = [dict(name=n, level=l) for n, l in sorted(dn, key=lambda x: x[1], reverse=True)]
    return {"soft_up": up, "soft_down": dn}


# =============================
# ĐA KHUNG THỜI GIAN
# =============================

# cấu hình Volume Profile theo từng khung
TF_VP = {
    "1H": dict(window_bars=240, bins=40, top_k=10),   # ~10 ngày 1H
    "4H": dict(window_bars=240, bins=30, top_k=10),   # ~40 ngày 4H
    "1D": dict(window_bars=180, bins=24, top_k=8),    # ~6 tháng 1D
}

# trọng số xếp hạng band SR
LEVEL_WEIGHTS = {"touch": 0.35, "dwell": 0.20, "psych": 0.25, "vp": 0.20}

def compute_features_by_tf(dfs_by_tf: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Tính toàn bộ primitives cho nhiều TF. dfs_by_tf ví dụ: {'1H': df1h, '4H': df4h, '1D': df1d}
    Trả dict theo từng TF với cấu trúc đồng nhất.
    """
    out: Dict[str, Any] = {}
    for tf, df in dfs_by_tf.items():
        if df is None or len(df) == 0:
            out[tf] = {"error": "empty df"}
            continue
        df = df.sort_index()

        swings  = compute_swings(df)
        trend   = compute_trend(df)
        candles = compute_candles(df)
        vol     = compute_volume_features(df)
        mom     = compute_momentum(df)
        vola    = compute_volatility(df)

        # Volume Profile theo TF
        vp_cfg = TF_VP.get(tf.upper(), TF_VP["4H"])
        try:
            vp_zones = calc_vp(df, **vp_cfg)  # list[{price_range, price_mid, volume_sum}]
        except Exception:
            vp_zones = []

        # SR cứng + ranking (đưa vp_zones & weights vào)
        sr = compute_levels(
            df, atr=vola.get('atr', 0.0),
            vp_zones=vp_zones,
            weights=LEVEL_WEIGHTS,
        )
        soft = compute_soft_levels(df)

        out[tf] = {
            "swings": swings,
            "trend": trend,
            "candles": candles,
            "volume": vol,
            "momentum": mom,
            "volatility": vola,
            "levels": sr,
            "soft_levels": soft,
            "vp_zones": vp_zones,
        }
    return out
