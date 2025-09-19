from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, List
import math

# ===============================================
# Tiny-Core (Side-Aware State) — dict-safe evidence
# ===============================================

@dataclass
class SideCfg:
    """Config cho phân loại state có xét side (long/short)."""

    # Regime thresholds (tinh chỉnh theo thị trường bạn chạy)
    bbw_squeeze_thr: float = 0.06       # range-like khi BBW dưới ngưỡng
    adx_trend_thr: float = 20.0         # range-like khi ADX dưới ngưỡng
    break_buffer_atr: float = 0.3      # buffer tính theo ATR quanh mốc break

    # Guards cho BREAK theo biến động tương đối (NATR) – 4H/execution
    natr_break_min: float = 0.004      # ~0.4%
    natr_break_max: float = 0.060      # ~6% (quá cao -> dễ whipsaw/chasing)
    
    # Early breakout option: allow break without volume/exp when NATR is low
    early_breakout_ok: bool = True
    early_breakout_natr_max: float = 0.012  # <= ~1.2% coi là low-vol
    early_breakout_need_inside_or_minitest: bool = True  # yêu cầu inside/mini-retest cho early

    # Retest proximity thresholds by regime (dist to mid in ATR)
    dist_atr_thr_low: float = 0.60
    dist_atr_thr_normal: float = 0.75
    dist_atr_thr_high: float = 1.00

    # --- Continuation gate & mini-retest (mới) ---
    use_continuation_gate: bool = True
    continuation_need_inside_or_minitest: bool = True
    mini_retest_lookback_bars: int = 3
    mini_retest_atr_frac: float = 0.25

    # Tie handling
    tie_eps: float = 1e-6               # sai số tuyệt đối để coi như hoà
    side_margin: float = 0.5         # yêu cầu chênh tối thiểu để chọn side

    # Retest score gates
    retest_long_threshold: float = 0.75
    retest_short_threshold: float = 0.75

    # TP ladder mặc định cho tính RR (fallback khi thiếu band)
    rr_targets: Tuple[float, float, float] = (1.2, 2.0, 3.0)

    # Timeframes (trigger/execution thống nhất 4H)
    tf_primary: str = "4H"
    tf_confirm: str = "4H"

    # Proximity thresholds theo regime cho RETEST
    dist_atr_thr_low: float = 0.60
    dist_atr_thr_normal: float = 0.75
    dist_atr_thr_high: float = 1.00

    # --- SL regime adaptation ---
    sl_min_atr_low: float = 0.60
    sl_min_atr_normal: float = 0.80
    sl_min_atr_high: float = 1.20

    # EMA50 4H cushion for SL
    use_ema50_sl_cushion: bool = True
    ema50_sl_cushion_atr_frac: float = 0.15  # 0.10–0.20 ATR recommended

    # --- 4H confirm rules for breakout/continuation ---
    use_4h_confirm: bool = True
    rsi_long_thr_4h: float = 55.0
    rsi_short_thr_4h: float = 45.0
    # allow fast trigger without 4H when regime == 'high'
    skip_4h_when_high_vol: bool = True


# Kết quả setup/decision để tương thích engine_adapter.py
@dataclass
class Setup:
    entry: Optional[float] = None
    sl: Optional[float] = None
    tps: List[float] = field(default_factory=list)

@dataclass
class Decision:
    decision: str = "WAIT"                 # ENTER / WAIT / AVOID
    state: Optional[str] = None
    side: Optional[str] = None
    setup: Setup = field(default_factory=Setup)
    meta: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)

# Stub/alias kiểu dữ liệu
SI = Any


# -----------------------------
# Helpers (an toàn, tái sử dụng)
# -----------------------------
def _safe_get(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        try:
            return obj.get(name, default)  # dict-style
        except Exception:
            return default

def _get_ev(eb: Dict[str, Any], key: str) -> Dict[str, Any]:
    if eb is None:
        return {}
    # eb có thể là {'evidence': {...}} hoặc đã là dict evidence thẳng
    evs = eb.get("evidence", eb) if isinstance(eb, dict) else {}
    x = evs.get(key, {}) if isinstance(evs, dict) else {}
    return x or {}

def _price(df) -> Optional[float]:
    try:
        return float(df["close"].iloc[-1])
    except Exception:
        return None

def _rr(direction: Optional[str], entry: Optional[float], sl: Optional[float], tp: Optional[float]) -> float:
    try:
        if direction == 'long':
            risk = max(1e-9, (entry or 0) - (sl or 0))
            reward = max(0.0, (tp or 0) - (entry or 0))
        elif direction == 'short':
            risk = max(1e-9, (sl or 0) - (entry or 0))
            reward = max(0.0, (entry or 0) - (tp or 0))
        else:
            return 0.0
        return float(reward / risk) if risk > 0 else 0.0
    except Exception:
        return 0.0

def _ensure_sl_gap(entry: float, sl: float, atr: float, side: str, min_atr: float = 0.6) -> float:
    """Đảm bảo khoảng cách SL tối thiểu theo ATR; giữ đúng phía. (ý tưởng từ decision_engine) :contentReference[oaicite:1]{index=1}"""
    min_gap = max(1e-9, min_atr * max(atr, 0.0))
    if atr <= 0 or entry is None or sl is None or side not in ("long", "short"):
        return sl
    if side == 'long':
        return float(entry - min_gap) if (entry - sl) < min_gap else float(sl)
    else:
        return float(entry + min_gap) if (sl - entry) < min_gap else float(sl)

def _apply_sl_upgrades(side_meta: Any, side: str, entry: float, sl: float, cfg: SideCfg) -> float:
    """
    1) Ensure SL gap theo regime (low/normal/high) từ evidence_adaptive.regime:contentReference[oaicite:4]{index=4}
    2) Tôn trọng swing gần nhất: 
       - long: nếu có LL gần dưới entry, ưu tiên đặt SL < LL (trừ 0.1*ATR)
       - short: nếu có HH gần trên entry, ưu tiên đặt SL > HH (cộng 0.1*ATR)
    """
    # side_meta có thể là dict hoặc SI object
    atr = float(_safe_get(side_meta, "atr", 0.0) or 0.0)
    regime = str(_safe_get(side_meta, "regime", "normal"))
    if regime == "low":
        min_atr = cfg.sl_min_atr_low
    elif regime == "high":
        min_atr = cfg.sl_min_atr_high
    else:
        min_atr = cfg.sl_min_atr_normal

    sl_new = _ensure_sl_gap(entry, sl, atr, side, min_atr=min_atr)  # đã có sẵn trong core:contentReference[oaicite:5]{index=5}

    # nearest swing (4H ưu tiên; fallback 1H)
    import math
    def _nearest(vals, ref):
        try:
            vals = [float(v) for v in (vals or []) if v is not None and math.isfinite(float(v))]
        except Exception:
            vals = []
        if not vals or ref is None or not math.isfinite(ref):
            return None
        return min(vals, key=lambda v: abs(v - ref))

    last_LL = _nearest(_safe_get(side_meta, "last_LL_4h") or _safe_get(side_meta, "last_LL_1h"), entry)
    last_HH = _nearest(_safe_get(side_meta, "last_HH_4h") or _safe_get(side_meta, "last_HH_1h"), entry)

    if side == "long" and last_LL is not None:
        sl_floor = float(last_LL) - 0.1 * atr
        if sl_new > sl_floor:
            sl_new = sl_floor
    if side == "short" and last_HH is not None:
        sl_ceiling = float(last_HH) + 0.1 * atr
        if sl_new < sl_ceiling:
            sl_new = sl_ceiling

    # 3) EMA50 4H cushion (optional)
    if getattr(cfg, "use_ema50_sl_cushion", True):
        ema50 = _safe_get(side_meta, "ema50_4h", None)
        try:
            ema50 = float(ema50)
        except Exception:
            ema50 = None
        if isinstance(ema50, (int, float)) and (ema50 == ema50):  # not NaN
            c = max(0.0, float(getattr(cfg, "ema50_sl_cushion_atr_frac", 0.15)) * atr)
            if side == "long":
                sl_floor = float(ema50) - c
                if sl_new > sl_floor:
                    sl_new = sl_floor
            elif side == "short":
                sl_ceiling = float(ema50) + c
                if sl_new < sl_ceiling:
                    sl_new = sl_ceiling
    return float(sl_new)


def _tp_by_rr(entry: float, sl: float, side: str, targets: Tuple[float, ...]) -> List[float]:
    """
    Tạo TP theo RR tuyệt đối từ cấu hình rr_targets (ví dụ 1.2, 2.0, 3.0).
    RR = |TP - entry| / |entry - SL|  (tùy theo side).
    """
    try:
        if side not in ("long", "short"):
            return []
        risk = (entry - sl) if side == "long" else (sl - entry)
        if risk <= 0:
            return []
        tps: List[float] = []
        for r in targets:
            r = float(r)
            tp = entry + r * risk if side == "long" else entry - r * risk
            tps.append(float(tp))
        # đảm bảo thứ tự hợp lý theo side
        tps.sort(reverse=(side == "short"))
        return tps
    except Exception:
        return []


# -------------------------------------------------------
# Collect side indicators từ features + evidence bundle
# -------------------------------------------------------
def collect_side_indicators(features_by_tf: Dict[str, Dict[str, Any]], eb: Dict[str, Any], cfg: SideCfg) -> Any:
    # Force 4H for both trigger & execution when building setup params
    tf_primary = "4H"
    tf_confirm = "4H"
    f1 = features_by_tf.get(tf_primary, {}) or {}
    f4 = features_by_tf.get(tf_confirm, {}) or {}

    # Price theo trigger; ATR/NATR theo execution để dựng SL/TP
    df_trigger = f1.get('df')
    price = _price(df_trigger)
    atr  = float((f4.get('volatility', {}) or {}).get('atr', 0.0) or 0.0)
    natr = float((f4.get('volatility', {}) or {}).get('natr', 0.0) or 0.0)

    # read EMA 4H for SL cushion
    df_confirm = f4.get('df')
    try:
        ema50_4h = float(df_confirm['ema50'].iloc[-1]) if df_confirm is not None and len(df_confirm) else float('nan')
    except Exception:
        ema50_4h = float('nan')

    # trend/momentum/volume phía 1H (đưa về sign)
    def _trend_dir_from_features(ff) -> int:
        st = (ff.get('trend', {}) or {}).get('state')
        if st == 'up': return +1
        if st == 'down': return -1
        return 0

    def _momo_dir_from_features(ff) -> int:
        rsi = float((ff.get('momentum', {}) or {}).get('rsi', 50.0) or 50.0)
        return +1 if rsi > 50 else (-1 if rsi < 50 else 0)

    def _vol_dir_from_features(ff) -> int:
        vol = (ff.get('volume', {}) or {})
        vz = float(vol.get('vol_z20', 0.0) or 0.0)
        vr = float((ff.get('volume', {}) or {}).get('vol_ratio', 1.0) or 1.0)
        contraction = bool(vol.get('contraction', False))
        strong = bool(vol.get('break_vol_strong', False))
        ok = bool(vol.get('break_vol_ok', False))

        # Dead-zone để giảm nhiễu
        pos = 0
        neg = 0
        # z-score: ±1.0 là ngưỡng có ý nghĩa
        if vz >= 1.0:  pos += 1
        elif vz <= -1.0: neg += 1
        # ratio: ≥1.5 bùng nổ, ≤0.67 suy yếu
        if vr >= 1.5:  pos += 1
        elif vr <= 0.67: neg += 1

        # Nếu đang contraction và CHƯA có break → neutral
        if contraction and not (ok or strong):
            return 0

        return 1 if pos > neg else (-1 if neg > pos else 0)

    trend_strength = _trend_dir_from_features(f1)
    momo_strength = _momo_dir_from_features(f1)
    volume_tilt = _vol_dir_from_features(f1)

    # Inside-bar dùng directly từ candles 4H
    try:
        inside_bar = bool((f4.get('candles', {}) or {}).get('inside_bar', False))
    except Exception:
        inside_bar = False

    # levels để dựng TP ladder/SL confluence
    levels1h = f1.get('levels', {}) or {}
    levels4h = f4.get('levels', {}) or {}
        
    # Lấy evidences chính
    ev_pb  = _get_ev(eb, 'price_breakout')
    ev_pdn = _get_ev(eb, 'price_breakdown')
    ev_volb = _get_ev(eb, 'volatility_breakout')
    ev_bb   = _get_ev(eb, 'bb_expanding')
    ev_prc = _get_ev(eb, 'price_reclaim')
    ev_mr  = _get_ev(eb, 'mean_reversion')
    ev_div = _get_ev(eb, 'divergence')
    ev_rjt = _get_ev(eb, 'rejection')
    ev_tb  = _get_ev(eb, 'throwback')
    ev_pbk = _get_ev(eb, 'pullback')
    ev_fb_out = _get_ev(eb, 'false_breakout')
    ev_fb_dn  = _get_ev(eb, 'false_breakdown')
    ev_adapt  = _get_ev(eb, 'adaptive')  # meta: is_slow, liquidity_floor, regime ...
    ev_liq    = _get_ev(eb, 'liquidity')  # HVN guard (near heavy zone)
    ev_mini   = _get_ev(eb, 'mini_retest')

    # breakout flags
    breakout_ok = bool(ev_pb.get('ok') or ev_pdn.get('ok'))
    breakout_side = 'long' if ev_pb.get('ok') else ('short' if ev_pdn.get('ok') else None)
    # ref levels để set entry/SL cho BREAK
    break_level = None; break_buffer = None
    if breakout_side == 'long' and isinstance(ev_pb.get('ref'), dict):
        break_level = ev_pb['ref'].get('hh'); break_buffer = ev_pb['ref'].get('buffer')
    if breakout_side == 'short' and isinstance(ev_pdn.get('ref'), dict):
        break_level = ev_pdn['ref'].get('ll'); break_buffer = ev_pdn['ref'].get('buffer')

    # reclaim
    reclaim_ok = bool(ev_prc.get('ok'))
    reclaim_side = None
    try:
        ref = ev_prc.get('ref') or {}
        _s = ref.get('side')
        if _s in ('long','short'):
            reclaim_side = _s
    except Exception:
        pass

    # mean-reversion
    meanrev_ok = bool(ev_mr.get('ok'))
    meanrev_side = ev_mr.get('side') if ev_mr.get('side') in ('long','short') else None

    # divergence & rejection side
    div_side = ev_div.get('side') if ev_div.get('side') in ('long','short') else None
    rejection_side = ev_rjt.get('side') if ev_rjt.get('side') in ('long','short') else None

    # false-break (đảo hướng)
    false_break_long  = bool(ev_fb_dn.get('ok'))   # false breakdown → long bias
    false_break_short = bool(ev_fb_out.get('ok'))  # false breakout  → short bias

    # Retest zone (ưu tiên pullback/throwback; đã được normalize 'mid' trong build_evidence_bundle) :contentReference[oaicite:3]{index=3}
    def _zone_fields(ev: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        try:
            z = ev.get('zone')
            mid = float(ev.get('mid')) if ev.get('mid') is not None else None
            if isinstance(z, (list, tuple)) and len(z) == 2:
                lo, hi = float(z[0]), float(z[1])
                return lo, hi, mid
        except Exception:
            pass
        return None, None, None

    lo1, hi1, mid1 = _zone_fields(ev_pbk)
    lo2, hi2, mid2 = _zone_fields(ev_tb)
    retest_zone_lo = lo1 if lo1 is not None else lo2
    retest_zone_hi = hi1 if hi1 is not None else hi2
    retest_zone_mid = mid1 if mid1 is not None else mid2

    # khoảng cách hiện tại đến mid theo ATR (để guard proximity)
    dist_atr = abs(((price or 0.0) - (retest_zone_mid or (price or 0.0))) / max(atr, 1e-9)) if retest_zone_mid else None

    # -------------- 4H CONFIRM (two-tier) --------------
    regime = str(ev_adapt.get("regime") or "normal")  # 'low' | 'normal' | 'high'
    f4_trend = (f4.get('trend') or {})
    f4_momo  = (f4.get('momentum') or {})
    trend_ok_long  = (f4_trend.get('state') == 'up')
    trend_ok_short = (f4_trend.get('state') == 'down')
    rsi4 = float(f4_momo.get('rsi') or 50.0)
    rsi_ok_long  = rsi4 >= cfg.rsi_long_thr_4h
    rsi_ok_short = rsi4 <= cfg.rsi_short_thr_4h
    trend_not_side = f4_trend.get('state') in ('up','down')

    def _need_4h_confirm() -> bool:
        if not cfg.use_4h_confirm:
            return False
        if regime == 'high' and cfg.skip_4h_when_high_vol:
            # high-vol: cho phép mini_retest/inside_bar 1H phản ứng nhanh
            return False
        return True

    def _confirm_4h(side: str) -> bool:
        if not _need_4h_confirm():
            return True
        if side == 'long':
            return (trend_ok_long and rsi_ok_long) or trend_not_side
        if side == 'short':
            return (trend_ok_short and rsi_ok_short) or trend_not_side
        return False

    # Ghi vào meta để core sử dụng
    confirm4h = {
        "need": _need_4h_confirm(),
        "trend_state_4h": f4_trend.get('state'),
        "rsi4": rsi4,
        "ok_long": _confirm_4h('long'),
        "ok_short": _confirm_4h('short')
    }

    # đóng gói SI đơn giản bằng object kiểu dict
    class SIObj:
        pass

    si = SIObj()
    si.price = price
    si.atr = atr
    si.natr = natr
    si.dist_atr = dist_atr
    # attach EMA50 4H for SL cushion
    try:
        si.ema50_4h = float(ema50_4h)
    except Exception:
        si.ema50_4h = None

    si.trend_strength = trend_strength
    si.momo_strength = momo_strength
    si.volume_tilt = volume_tilt
    
    # candle pattern flags
    try:
        si.inside_bar = bool((f4.get('candles', {}) or {}).get('inside_bar', False))
    except Exception:
        si.inside_bar = False
    # mini-retest flags từ evidence
    try:
        si.mini_retest_long  = bool(ev_mini.get('long', False))
        si.mini_retest_short = bool(ev_mini.get('short', False))
    except Exception:
        si.mini_retest_long = si.mini_retest_short = False

    si.levels1h = levels1h
    si.levels4h = levels4h
    # attach swings & regime/confirm to si
    try:
        si.last_HH_1h = (levels1h or {}).get("last_HH")
        si.last_LL_1h = (levels1h or {}).get("last_LL")
        si.last_HH_4h = (levels4h or {}).get("last_HH")
        si.last_LL_4h = (levels4h or {}).get("last_LL")
    except Exception:
        si.last_HH_1h = si.last_LL_1h = si.last_HH_4h = si.last_LL_4h = None
    si.regime = regime
    si.confirm4h = confirm4h

    si.breakout_ok = breakout_ok
    si.breakout_side = breakout_side
    si.break_level = float(break_level) if break_level is not None else None
    si.break_buffer = float(break_buffer) if break_buffer is not None else None
    si.bb_expanding_ok = bool(ev_bb.get('ok'))
    si.volatility_breakout_ok = bool(ev_volb.get('ok'))
    # Volume regime flags từ 1H features
    vol1 = (f1.get('volume', {}) or {})
    si.vol_break_ok = bool(vol1.get('break_vol_ok'))
    si.vol_break_strong = bool(vol1.get('break_vol_strong'))

    si.reclaim_ok = reclaim_ok
    si.reclaim_side = reclaim_side

    # RETEST chỉ bật khi có throwback/pullback hợp lệ
    si.retest_ok = bool((ev_pbk.get('ok') if isinstance(ev_pbk, dict) else False) or
                        (ev_tb.get('ok')  if isinstance(ev_tb,  dict) else False))
    si.retest_zone_lo = retest_zone_lo
    si.retest_zone_hi = retest_zone_hi
    si.retest_zone_mid = retest_zone_mid

    si.meanrev_ok = meanrev_ok
    si.meanrev_side = meanrev_side

    si.div_side = div_side
    si.rejection_side = rejection_side
    # adaptive guards from evidence
    si.is_slow = bool(ev_adapt.get('is_slow')) if isinstance(ev_adapt, dict) else False
    si.liquidity_floor = float(ev_adapt.get('liquidity_floor', 0.0)) if isinstance(ev_adapt, dict) else 0.0

    si.false_break_long = false_break_long
    si.false_break_short = false_break_short    

    # inside-bar flag from candle features
    try:
        si.inside_bar = bool((f1.get('candles', {}) or {}).get('inside_bar', False))
    except Exception:
        si.inside_bar = False

    # adaptive guards
    si.is_slow = bool(ev_adapt.get('is_slow', False)) if isinstance(ev_adapt, dict) else False
    si.liquidity_floor = bool(ev_adapt.get('liquidity_floor', False)) if isinstance(ev_adapt, dict) else False
    si.regime = (ev_adapt.get('regime') if isinstance(ev_adapt, dict) else None) or 'normal'
    # HVN guard flag from evidence
    try:
        si.near_heavy_zone = bool(ev_liq.get('near_heavy_zone', False)) if isinstance(ev_liq, dict) else False
        si.hvn_ok = bool(ev_liq.get('ok', True)) if isinstance(ev_liq, dict) else True
    except Exception:
        si.near_heavy_zone = False
        si.hvn_ok = True
    return si


# -------------------------------
# Phân loại state + side (giữ nguyên)
# -------------------------------
def classify_state_with_side(si: SI, cfg: SideCfg) -> Tuple[str, Optional[str], Dict[str, Any]]:
    """
    Xác định state và side từ bộ chỉ báo 'si'.
    Trả về: (state, side, meta)
    """
    meta: Dict[str, Any] = {}

    # Helper an toàn (nếu field không tồn tại thì trả mặc định)
    def _safe_get_local(obj: Any, name: str, default: Any = None) -> Any:
        """getattr an toàn: nếu thuộc tính = None thì trả về default."""
        try:
            val = getattr(obj, name)
        except Exception:
            val = default
        return default if val is None else val

    def _to_float(x, default=float("nan")) -> float:
        try:
            return float(x)
        except Exception:
            return default

    # Hướng trend/momentum/volume: quy về {-1, 0, +1}
    def _trend_dir(x: SI) -> int:
        val = float(_safe_get_local(x, "trend_strength", 0.0))
        return 0 if val == 0 else int(math.copysign(1, val))

    def _momo_dir(x: SI) -> int:
        val = float(_safe_get_local(x, "momo_strength", 0.0))
        return 0 if val == 0 else int(math.copysign(1, val))

    def _volume_dir(x: SI) -> int:
        val = float(_safe_get_local(x, "volume_tilt", 0.0))
        return 0 if val == 0 else int(math.copysign(1, val))

    # Meta context
    natr = _to_float(_safe_get_local(si, "natr", float("nan")), float("nan"))
    dist_atr = _to_float(_safe_get_local(si, "dist_atr", float("nan")), float("nan"))
    tr = _trend_dir(si)
    momo = _momo_dir(si)
    vdir = _volume_dir(si)
    meta.update(dict(natr=natr, dist_atr=dist_atr, trend=tr, momo=momo, v=vdir))

    # --- 0) CONTINUATION gate (trước BREAK/RETEST) ---
    if bool(getattr(cfg, "use_continuation_gate", True)):
        aligned_long  = (tr > 0 and momo > 0 and vdir > 0)
        aligned_short = (tr < 0 and momo < 0 and vdir < 0)
        votes3 = aligned_long or aligned_short
        need_candle = bool(getattr(cfg, "continuation_need_inside_or_minitest", True))
        mini_ok_long  = bool(_safe_get_local(si, "mini_retest_long", False))
        mini_ok_short = bool(_safe_get_local(si, "mini_retest_short", False))
        inside = bool(_safe_get_local(si, "inside_bar", False))
        cond_long  = aligned_long  and ((inside or mini_ok_long)  if need_candle else True)
        cond_short = aligned_short and ((inside or mini_ok_short) if need_candle else True)
        liq_block  = bool(_safe_get_local(si, "liquidity_floor", False)) or (not bool(_safe_get_local(si, "hvn_ok", True)))
        if votes3 and (cond_long or cond_short) and (not liq_block):
            side_c = "long" if cond_long else "short"
            meta.update({"gate": "continuation"})
            return "trend_break", side_c, meta

    # --- 1) BREAK regime (trend_break) có guard NATR & volume ---
    # Early-breakout/Continuation: khi chưa có breakout flag nhưng đủ điều kiện động lượng trong regime NATR thấp
    if (not _safe_get_local(si, 'breakout_ok', False)) and bool(getattr(cfg, 'early_breakout_ok', True)):
        # NATR guard cho early-breakout
        if math.isfinite(natr) and (cfg.natr_break_min <= natr <= min(cfg.natr_break_max, getattr(cfg, 'early_breakout_natr_max', 0.012))):
            # yêu cầu co giãn/impulse + alignment trend & momentum 
            exp_ok = bool(_safe_get_local(si, 'bb_expanding_ok', False) or _safe_get_local(si, 'volatility_breakout_ok', False) or _safe_get_local(si, 'vol_break_ok', False) or _safe_get_local(si, 'vol_break_strong', False))
            aligned = (tr != 0 and momo != 0 and (tr == momo))
            # extra guards: volume tilt không âm + không dính liquidity/HVN/near heavy zone + (tuỳ chọn) inside/mini-retest
            liq_block  = bool(_safe_get_local(si, 'liquidity_floor', False)) or (not bool(_safe_get_local(si, 'hvn_ok', True))) or bool(_safe_get_local(si, 'near_heavy_zone', False))
            inside = bool(_safe_get_local(si, 'inside_bar', False))
            mini_ok_long  = bool(_safe_get_local(si, 'mini_retest_long', False))
            mini_ok_short = bool(_safe_get_local(si, 'mini_retest_short', False))
            need_micro = bool(getattr(cfg, 'early_breakout_need_inside_or_minitest', True))
            micro_ok = (inside or (mini_ok_long if tr > 0 else mini_ok_short)) if need_micro else True
            if exp_ok and aligned and (vdir >= 0) and (not liq_block) and micro_ok:
                side_b = 'long' if tr > 0 else 'short'
                meta['early_breakout'] = True
                return 'trend_break', side_b, meta

    if _safe_get_local(si, "breakout_ok", False) and _safe_get_local(si, "breakout_side") in ("long", "short"):
        side_b = _safe_get_local(si, "breakout_side")
        # 4H NATR guard
        if math.isfinite(natr):
            if not (cfg.natr_break_min <= natr <= cfg.natr_break_max):
                return "none_state", None, meta
        # Volume & expansion guards
        vol_ok = bool(_safe_get_local(si, "vol_break_ok", False) or _safe_get_local(si, "vol_break_strong", False))
        exp_ok = bool(_safe_get_local(si, "bb_expanding_ok", False) or _safe_get_local(si, "volatility_breakout_ok", False))
        # Allow early breakout when configured and NATR is low
        early_ok = bool(cfg.early_breakout_ok and math.isfinite(natr) and natr <= cfg.early_breakout_natr_max)
        # Determine if we can allow early without vol/expansion
        allow_early = False
        if not (vol_ok or exp_ok) and early_ok:
            if (side_b == "long" and tr > 0 and momo >= 0) or (side_b == "short" and tr < 0 and momo <= 0):
                allow_early = True
        if not (vol_ok or exp_ok or allow_early):
            return "none_state", None, meta
        # Extra guards when using early allowance
        if allow_early:
            meta["early_breakout"] = True
            # volume tilt phải không âm
            if _volume_dir(si) < 0:
                return "none_state", None, meta
            # chặn thanh khoản/HVN/khu vực nặng profile
            if bool(_safe_get_local(si, "liquidity_floor", False)) or (not bool(_safe_get_local(si, "hvn_ok", True))) or bool(_safe_get_local(si, "near_heavy_zone", False)):
                return "none_state", None, meta
            # (tuỳ chọn) yêu cầu inside/mini-retest cùng phía
            if bool(getattr(cfg, "early_breakout_need_inside_or_minitest", True)):
                inside = bool(_safe_get_local(si, "inside_bar", False))
                mini_ok_long  = bool(_safe_get_local(si, "mini_retest_long", False))
                mini_ok_short = bool(_safe_get_local(si, "mini_retest_short", False))
                micro_ok = inside or (mini_ok_long if side_b == "long" else mini_ok_short)
                if not micro_ok:
                    return "none_state", None, meta

        # Trend/momentum alignment với hướng break
        if side_b == "long" and not (tr > 0 and momo >= 0):
            return "none_state", None, meta
        if side_b == "short" and not (tr < 0 and momo <= 0):
            return "none_state", None, meta
        return "trend_break", side_b, meta

    # --- 2) Retest regime (support vs resistance) với soft scoring ---
    if _safe_get_local(si, "retest_ok", False):
        long_score = 0.0
        short_score = 0.0
        # Proximity gate: chỉ xét RETEST nếu gần mid (động theo regime)
        dist_atr = float(_safe_get_local(si, "dist_atr", float("nan")))
        # dynamic proximity by ATR regime
        thr_map = {'low': cfg.dist_atr_thr_low, 'normal': cfg.dist_atr_thr_normal, 'high': cfg.dist_atr_thr_high}
        reg = str(_safe_get_local(si, 'regime', 'normal'))
        dist_thr = thr_map.get(reg, cfg.dist_atr_thr_normal)
        if not (math.isfinite(dist_atr) and dist_atr <= float(dist_thr)):
            return "none_state", None, meta

        # Context signals
        # Cho phép (inside-bar + volume-tilt) đóng vai trò "major" khi trend & momentum align
        if (tr > 0 and momo > 0 and vdir > 0 and bool(_safe_get_local(si, "inside_bar", False))):
            long_score += 0.75
        if (tr < 0 and momo < 0 and vdir < 0 and bool(_safe_get_local(si, "inside_bar", False))):
            short_score += 0.75

        if _safe_get_local(si, "reclaim_ok", False):
            if _safe_get_local(si, "reclaim_side") == "long":
                long_score += 0.75
            elif _safe_get_local(si, "reclaim_side") == "short":
                short_score += 0.75

        # Vị trí so với zone
        zone_mid = _safe_get_local(si, "retest_zone_mid")
        price = _safe_get_local(si, "price")
        atr   = float(_safe_get_local(si, "atr", 0.0) or 0.0)
        if zone_mid is not None and price is not None and atr > 0:
            buf = 0.05 * atr
            if price <= zone_mid - buf:
                long_score += 0.5  # gần hỗ trợ/mean
            elif price >= zone_mid + buf:
                short_score += 0.5  # gần kháng cự/mean

        # Volume tilt (bonus nếu cùng hướng)
        if vdir > 0:
            long_score += 0.25
        elif vdir < 0:
            short_score += 0.25

        # Mean-reversion hint
        if _safe_get_local(si, "meanrev_ok", False) and _safe_get_local(si, "meanrev_side") in ("long", "short"):
            if si.meanrev_side == "long":
                long_score += 0.5
            else:
                short_score += 0.5

        # False-break nghiêng mạnh về retest ngược hướng break
        fbl = bool(_safe_get_local(si, "false_break_long", False))
        fbs = bool(_safe_get_local(si, "false_break_short", False))
        rcl = (_safe_get_local(si, "reclaim_side") == "long")
        rcs = (_safe_get_local(si, "reclaim_side") == "short")
        # Giảm stack khi cùng phía (reclaim + false-break)
        if fbl and rcl:
            long_score += 0.5
        elif fbl:
            long_score += 0.75
        if fbs and rcs:
            short_score += 0.5
        elif fbs:
            short_score += 0.75

        # Rejection & divergence (nghiêng nhẹ)
        rej_side = _safe_get_local(si, "rejection_side")
        if rej_side == "long":
            long_score += 0.5
        elif rej_side == "short":
            short_score += 0.5

        div_side = _safe_get_local(si, "div_side")
        if div_side == "long":
            long_score += 0.25
        elif div_side == "short":
            short_score += 0.25

        meta.update(dict(long_score=long_score, short_score=short_score))

        # Yêu cầu tối thiểu 1 major-evidence để tránh RETEST “trôi”
        major_long  = int((tr > 0 and momo > 0)) + int(rcl) + int(fbl)
        major_short = int((tr < 0 and momo < 0)) + int(rcs) + int(fbs)
        # Allow (inside-bar + volume tilt) to count as a major when trend&momo align
        ib = bool(_safe_get(si, 'inside_bar', False))
        vt = _to_float(_safe_get(si, 'volume_tilt', 0.0), 0.0)
        vdir = int(math.copysign(1, vt)) if vt != 0 else 0
        if (tr > 0 and momo > 0 and vdir > 0 and ib):
            major_long += 1
        if (tr < 0 and momo < 0 and vdir < 0 and ib):
            major_short += 1

        # Tie & margin policy:
        diff = long_score - short_score
        if abs(diff) <= cfg.tie_eps or abs(diff) < cfg.side_margin:
            return "none_state", None, meta

        if diff > 0 and long_score >= cfg.retest_long_threshold and major_long >= 1:
            return "retest_support", "long", meta

        if diff < 0 and short_score >= cfg.retest_short_threshold and major_short >= 1:
            return "retest_resistance", "short", meta

    # --- 3) None ---
    return "none_state", None, meta


# ---------------------------------------
# Build setup (entry/SL/TP) theo side/zone
# ---------------------------------------
def build_setup(si: SI, state: str, side: Optional[str], cfg: SideCfg) -> Setup:
    st = Setup()
    price = _safe_get(si, "price")
    atr = float(_safe_get(si, "atr", 0.0) or 0.0)

    if price is None or atr <= 0 or side not in ("long","short"):
        return st  # thiếu dữ liệu → setup rỗng

    # Entry:
    if state == "trend_break":
        lvl = _safe_get(si, "break_level")
        buf = float(_safe_get(si, "break_buffer", 0.0) or 0.0)
        if side == "long":
            ref = (lvl + (buf or 0.0)) if lvl is not None else price
            st.entry = float(max(price, ref))
            st.sl = float((lvl - 0.6 * atr) if lvl is not None else (st.entry - 0.8 * atr))
        else:
            ref = (lvl - (buf or 0.0)) if lvl is not None else price
            st.entry = float(min(price, ref))
            st.sl = float((lvl + 0.6 * atr) if lvl is not None else (st.entry + 0.8 * atr))
    else:
        # RETEST: ưu tiên mid
        z_mid = _safe_get(si, "retest_zone_mid")
        st.entry = float(z_mid if z_mid is not None else price)

    # SL: nếu có zone_lo/hi dùng làm mốc, có pad nhỏ; else dùng ATR
    if state != "trend_break":
        z_lo = _safe_get(si, "retest_zone_lo")
        z_hi = _safe_get(si, "retest_zone_hi")
        pad = 0.1 * atr
        if side == "long":
            if z_lo is not None:
                st.sl = float(z_lo - pad)
            else:
                st.sl = float(st.entry - 0.8 * atr)
        elif side == "short":
            if z_hi is not None:
                st.sl = float(z_hi + pad)
            else:
                st.sl = float(st.entry + 0.8 * atr)

    # enforce SL gap theo ATR
    st.sl = _ensure_sl_gap(st.entry, st.sl, atr, side, min_atr=0.6)

    # Ưu tiên: TP ladder cấu trúc (bands/HVN → Fib → ATR). Nếu rỗng, fallback R:R.
    try:
        struct_tps = _structure_tps(si, side, st.entry, st.sl, atr, state)
    except Exception:
        struct_tps = []
    if struct_tps:
        st.tps = struct_tps
    else:
        st.tps = _tp_by_rr(st.entry, st.sl, side, cfg.rr_targets)

    return st


# ---------------------------------------
# Quyết định 5-gates (tối giản, thực dụng)
# ---------------------------------------
def decide_5_gates(state: str, side: Optional[str], setup: Setup, si: SI, cfg: SideCfg, meta: Dict[str, Any]) -> Decision:
    dec = Decision(state=state, side=side, setup=setup, meta=dict(meta))
    # ---- enrich meta for detailed logging & guards ----
    try:
        regime = (_safe_get(si, "regime") or "normal")
    except Exception:
        regime = "normal"
    liq_thr_map = {"low": 0.7, "normal": 0.55, "high": 0.5}
    dec.meta["regime"] = regime
    dec.meta["liq_thr"] = liq_thr_map.get(regime, 0.55)
    # Side votes (numeric strengths; may be -1..+1 or scaled)
    try:
        dec.meta["side_votes"] = {
            "trend":   float(_safe_get(si, "trend_strength", 0.0) or 0.0),
            "momentum":float(_safe_get(si, "momo_strength", 0.0) or 0.0),
            "volume":  float(_safe_get(si, "volume_tilt", 0.0) or 0.0),
        }
    except Exception:
        dec.meta["side_votes"] = {}
        
    reasons: List[str] = []

    # Guards theo adaptive
    if _safe_get(si, "liquidity_floor", False):
        reasons.append("liquidity_floor")
    if _safe_get(si, "is_slow", False):
        reasons.append("slow_market")

    # HVN: near heavy volume profile zone blocks entries
    if _safe_get(si, "near_heavy_zone", False) or (not _safe_get(si, "hvn_ok", True)):
        reasons.append("near_heavy_zone")

    # Thiếu dữ liệu thiết yếu?
    price = _safe_get(si, "price")
    atr = float(_safe_get(si, "atr", 0.0) or 0.0)
    if price is None or atr <= 0 or side not in ("long","short") or setup.entry is None or setup.sl is None:
        dec.decision = "WAIT"
        if side is None:
            reasons.append("no_side")
        if price is None:
            reasons.append("no_price")
        if atr <= 0:
            reasons.append("no_atr")
        dec.reasons = sorted(set(reasons))
        return dec

    # Proximity guard: không vào nếu quá xa entry (>0.75 ATR)
    if abs(price - setup.entry) > (0.75 * atr):
        reasons.append("far_from_entry")

    # RR guard: RR tới TP1 tối thiểu 1.0
    tp1 = setup.tps[0] if setup.tps else None
    rr1 = _rr(side, setup.entry, setup.sl, tp1) if tp1 is not None else 0.0
    if rr1 < 1.0:
        reasons.append("rr_too_low")

    # Continuation requirement: cần (pullback OR inside OR mini-retest)
    try:
        is_cont = bool(dec.meta.get("gate") == "continuation")
        if is_cont and side in ("long","short"):
            inside_ok = bool(getattr(si, "inside_bar", False))
            mini_ok = bool(getattr(si, "mini_retest_long", False) if side=="long"
                           else getattr(si, "mini_retest_short", False))
            # si.retest_ok đã gộp pullback/throwback validator từ evidences
            pb_ok = bool(getattr(si, "retest_ok", False))
            if not (inside_ok or mini_ok or pb_ok):
                reasons.append("need_pullback_or_inside")
    except Exception:
        pass

    # ---------- Build rich 'missing_tags' for logging ----------
    def _sgn(x: float) -> int:
        if x > 0: return 1
        if x < 0: return -1
        return 0

    missing_tags: List[str] = []
    # 1) Direction diagnostics
    t_strength = float(_safe_get(si, "trend_strength", 0.0) or 0.0)
    m_strength = float(_safe_get(si, "momo_strength", 0.0) or 0.0)
    v_tilt     = float(_safe_get(si, "volume_tilt", 0.0) or 0.0)
    t_s, m_s, v_s = _sgn(t_strength), _sgn(m_strength), _sgn(v_tilt)
    votes_sum = t_s + m_s + v_s
    votes = {"trend": t_strength, "momentum": m_strength, "volume": v_tilt}
    signs = {"trend": t_s, "momentum": m_s, "volume": v_s, "sum": votes_sum}
    dec.meta["side_votes"] = votes
    dec.meta["side_vote_signs"] = signs

    if side is None:
        # Không đủ đồng thuận về hướng
        missing_tags.append("direction_undecided")
        # Thiếu/điểm yếu từng thành phần
        if t_s == 0: missing_tags.append("trend")
        if m_s == 0: missing_tags.append("momentum")
        if v_s == 0: missing_tags.append("volume")
        # Mâu thuẫn (không >= 2/3 cùng phía)
        nonzero = [s for s in (t_s, m_s, v_s) if s != 0]
        if not (len(nonzero) >= 2 and (nonzero.count(1) >= 2 or nonzero.count(-1) >= 2)):
            missing_tags.append("alignment_2of3")

    # 2) State-gate style requirements (generic, an toàn)
    if not bool(_safe_get(si, "retest_ok", False)):
        missing_tags.append("(pullback OR throwback)")
    if (not bool(_safe_get(si, "breakout_ok", False))) and (not bool(_safe_get(si, "reclaim_ok", False))):
        missing_tags.append("(breakout OR reclaim)")
    if (not bool(_safe_get(si, "bb_expanding_ok", False))) and (not bool(_safe_get(si, "volatility_breakout_ok", False))):
        missing_tags.append("(bb_expanding OR vol_impulse)")

    # 3) Guards/filters hữu ích để nhìn thấy ngay trên log
    if bool(_safe_get(si, "is_slow", False)):
        missing_tags.append("slow_market")
    if bool(_safe_get(si, "near_heavy_zone", False)) or (not bool(_safe_get(si, "hvn_ok", True))):
        missing_tags.append("near_heavy_zone")
    if setup.entry is None or setup.sl is None:
        missing_tags.append("incomplete_setup")
    if _safe_get(si, "price") is None:
        missing_tags.append("no_price")
    if float(_safe_get(si, "atr", 0.0) or 0.0) <= 0:
        missing_tags.append("no_atr")

    # Gộp một số 'reasons' quan trọng vào missing để hiển thị (tránh trùng)
    for r in ("far_from_entry", "rr_too_low"):
        if r in reasons and r not in missing_tags:
            missing_tags.append(r)
    dec.meta["missing_tags"] = sorted(set(missing_tags))
    # Tổng hợp quyết định
    if not reasons:
        dec.decision = "ENTER"
    else:
        dec.decision = "WAIT"
    dec.reasons = sorted(set(reasons))
    return dec


# ============== Orchestrator =====================
def run_side_state_core(
    features_by_tf: Dict[str, Dict[str, Any]],
    eb: Any,
    cfg: Optional[SideCfg] = None,
) -> Decision:
    """
    Orchestrator:
    - Thu thập side-indicators
    - Phân loại state/side
    - Build setup
    - Ra quyết định (5 cổng)
    """
    cfg = cfg or SideCfg()

    si: SI = collect_side_indicators(features_by_tf, eb, cfg)
    state, side, meta = classify_state_with_side(si, cfg)
    setup: Setup = build_setup(si, state, side, cfg)
    dec: Decision = decide_5_gates(state, side, setup, si, cfg, meta)

    # (2) Confirm 4H cho breakout/continuation theo two-tier
    if dec.state in ("trend_break","continuation") and dec.side in ("long","short"):
        ok4h = _safe_get(si, "confirm4h", {}).get("ok_long" if dec.side=="long" else "ok_short", True)
        need4h = _safe_get(si, "confirm4h", {}).get("need", False)
        # Nếu cần 4H mà không pass → WAIT để tránh whipsaw ở low/normal vol
        if need4h and not ok4h:
            dec.decision = "WAIT"
            dec.reasons.append("need_4H_confirm")

    # (3) Finalize setup (giữ nguyên nếu đã có)
    if dec.setup and dec.setup.entry is not None and dec.setup.sl is not None and dec.side in ("long","short"):
        # Nâng cấp SL: ensure-gap theo regime + tôn trọng swing gần nhất
        dec.setup.sl = _apply_sl_upgrades(si, dec.side, dec.setup.entry, dec.setup.sl, cfg)

    return dec

def _structure_tps(si: SI, side: str, entry: float, sl: float, atr: float, state: str) -> List[float]:
    """Build structure-based TP ladder (bands/HVN → Fib → ATR)."""
    tps: List[float] = []
    cands: List[float] = []

    def _add(p):
        try:
            p = float(p)
        except Exception:
            return
        if not (p == p):
            return
        if side == "long" and p > entry and abs(p - entry) >= 0.3 * max(atr, 1e-9):
            cands.append(p)
        elif side == "short" and p < entry and abs(entry - p) >= 0.3 * max(atr, 1e-9):
            cands.append(p)

    # 1) SR bands from features (4H first, then 1H)
    for levels in [getattr(si, "levels4h", {}) or {}, getattr(si, "levels1h", {}) or {}]:
        key = "bands_up" if side == "long" else "bands_down"
        bands = (levels.get(key) or []) if isinstance(levels, dict) else []
        for b in bands:
            p = None
            if isinstance(b, dict):
                p = b.get("tp")
                if p is None and isinstance(b.get("band"), (list, tuple)) and len(b["band"]) == 2:
                    p = (float(b["band"][0]) + float(b["band"][1])) / 2.0
            _add(p)

    # 2) Fib(0.618, 1.0) based on break level (if break) or retest mid
    ref = getattr(si, "break_level", None) if state == "trend_break" else getattr(si, "retest_zone_mid", None)
    try:
        if ref is not None and entry is not None:
            d = abs(float(entry) - float(ref))
            if d > 0:
                if side == "long":
                    _add(entry + 0.618 * d); _add(entry + 1.000 * d)
                else:
                    _add(entry - 0.618 * d); _add(entry - 1.000 * d)
    except Exception:
        pass

    # 3) ATR milestones
    for k in (2.0, 3.0):
        if side == "long":
            _add(entry + k * atr)
        else:
            _add(entry - k * atr)

    # De-duplicate near-equal and directionally sort
    uniq: List[float] = []
    tol = 0.1 * max(atr, 1e-9)
    for p in sorted(cands, reverse=(side == "short")):
        if all(abs(p - q) > tol for q in uniq):
            uniq.append(float(p))

    return uniq[:3]
