#!/usr/bin/env python3
"""
Main worker for Crypto Signal (Railway ready)

- Splits symbols into 6 blocks and scans twice per hour:
  block1 at :00 & :30, block2 at :05 & :35, block3 at :10 & :40,
  block4 at :15 & :45, block5 at :20 & :50, block6 at :25 & :55 (Asia/Ho_Chi_Minh)
- Workflow per symbol:
  1) fetch OHLCV for 1H/4H/1D (1H drop partial bar; 4H/1D keep realtime)
  2) enrich indicators (EMA/RSI/BB/ATR/volume, candle anatomy)
  3) compute features_by_tf (trend/momentum/volatility/SR + volume profile bands)
  4) build evidence bundle (STRUCT JSON)
  5) decide ENTER/WAIT/AVOID; optionally push Telegram
"""
import os, sys, time, json, logging
from typing import Any, Dict, List, TYPE_CHECKING
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from universe import get_universe_from_env  # uses DEFAULT_UNIVERSE if SYMBOLS not set
from kucoin_api import fetch_batch, _exchange  # spot-only; 1H drop-partial
from indicators import enrich_indicators, enrich_more
from feature_primitives import compute_features_by_tf
from engine_adapter import decide
from evidence_evaluators import build_evidence_bundle, Config

from notifier_telegram import TelegramNotifier
from storage import SignalPerfDB, JsonStore, UserDB
from templates import render_update

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
TIMEFRAMES = ("1H", "4H", "1D")

log = logging.getLogger("worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")

def _current_vn_window(now_local: datetime) -> tuple[int, int] | None:
    """
    Nếu now_local (Asia/Ho_Chi_Minh) đang nằm trong một trong hai khung:
      - 05:30–07:30
      - 17:30–19:30
    thì trả về (start_ts, end_ts) theo epoch seconds. Ngược lại trả None.
    """
    def _ts(h: int, m: int) -> int:
        dt = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
        return int(dt.timestamp())
    # hôm nay theo VN
    am_start, am_end = _ts(5, 30), _ts(7, 30)
    pm_start, pm_end = _ts(17, 30), _ts(19, 30)
    now_ts = int(now_local.timestamp())
    if am_start <= now_ts < am_end:
        return am_start, am_end
    if pm_start <= now_ts < pm_end:
        return pm_start, pm_end
    return None

# -------- helper: evidence detail formatting ----------
def _fmt_float(x, nd=2):
    try:
        xf = float(x)
        if not (xf == xf):  # NaN
            return "nan"
        return f"{xf:.{nd}f}"
    except Exception:
        return str(x)

def _fmt_ev_details(name: str, obj: dict) -> str:
    # Generic pretty-printer for an evidence dict
    if not isinstance(obj, dict):
        try:
            obj = obj.__dict__
        except Exception:
            obj = {}
    parts = []
    # common numeric fields
    for k in ("vol_ratio","vol_z20","grade","bbw_last","bbw_med","atr","ema_spread","distance","mid","nearest_zone_mid"):
        if k in obj and obj.get(k) is not None:
            v = obj.get(k)
            parts.append(f"{k}={_fmt_float(v) if isinstance(v,(int,float)) else v}")
    # side/why if helpful
    if obj.get("side") in ("long","short"):
        parts.append(f"side={obj.get('side')}")
    if obj.get("why"):
        w = str(obj.get("why"))
        if len(w) > 60:
            w = w[:60] + "…"
        parts.append(f"why={w}")
    if obj.get("near_heavy_zone") is not None:
        parts.append(f"near_hvn={bool(obj.get('near_heavy_zone'))}")
    return f"{name}{{{', '.join(parts)}}}" if parts else name

def _extract_evidence_ok_detailed(bundle: dict):
    """Return list of 'have' evidences with key metrics for logging."""
    try:
        ev = bundle.get('evidence', {}) if isinstance(bundle, dict) else {}
    except Exception:
        ev = {}
    out = []
    for name, obj in (ev or {}).items():
        # handle nested dict for 'trend_follow_ready'
        if name == "trend_follow_ready" and isinstance(obj, dict):
            for s in ("long","short"):
                o = obj.get(s) or {}
                ok = bool(o.get("ok")) if isinstance(o, dict) else bool(getattr(o, "ok", False))
                if ok:
                    out.append(_fmt_ev_details(f"{name}:{s}", o if isinstance(o, dict) else o.__dict__))
            continue
        ok = False
        if isinstance(obj, dict):
            ok = bool(obj.get('ok'))
        else:
            ok = bool(getattr(obj, 'ok', False))
            obj = getattr(obj, '__dict__', {}) or {}
        if ok:
            out.append(_fmt_ev_details(name, obj))
    return sorted(out)

def _no_side_reason(meta, bundle):
    """
    Giải thích vì sao 'no_side':
    - need_alignment_2of3: chưa đủ 2/3 phiếu cùng phía (trend/momentum/volume)
    - need_tf_ready: thiếu điều kiện trend-follow sẵn sàng (tf_long/tf_short)
    - need_state_gate(breakout|retest): chưa có cổng state (breakout|retest)
    """
    ev = bundle.get('evidence', {}) if isinstance(bundle, dict) else {}
    votes = meta.get("side_votes") or {}
    def _sgn(x):
        try:
            x = float(x)
        except Exception:
            return 0
        return 1 if x > 0 else (-1 if x < 0 else 0)
    v = [_sgn(votes.get("trend", 0.0)),
         _sgn(votes.get("momentum", 0.0)),
         _sgn(votes.get("volume", 0.0))]
    pos, neg = v.count(1), v.count(-1)
    two_of_three = (pos >= 2 or neg >= 2)

    tf_long  = bool(meta.get("tf_long"))
    tf_short = bool(meta.get("tf_short"))
    if not (tf_long or tf_short):
        tf_ev = ev.get('trend_follow_ready') or {}
        try:
            tf_long = bool((tf_ev.get('long') or {}).get('ok'))
            tf_short = bool((tf_ev.get('short') or {}).get('ok'))
        except Exception:
            pass

    # check state gates from evidence bundle
    ev = bundle.get("evidence", {}) if isinstance(bundle, dict) else {}
    def _ok(name):
        obj = ev.get(name) or {}
        if isinstance(obj, dict):
            return bool(obj.get("ok"))
        return bool(getattr(obj, "ok", False))
    has_brk = _ok("price_breakout") or _ok("price_breakdown")
    has_rt  = _ok("pullback") or _ok("throwback")
    # Continuation gate (trend-follow) nếu đủ alignment 2/3 và có tf_ready
    has_ctn = two_of_three and (tf_long or tf_short)

    if not two_of_three:
        reason = "need_alignment_2of3"
    elif not (tf_long or tf_short):
        reason = "need_tf_ready"
    elif not (has_brk or has_rt or has_ctn):
        reason = "need_state_gate(breakout|retest|continuation)"
    else:
        reason = "unspecified"
    return reason, has_brk, has_rt

def _describe_missing_tags(missing, bundle: dict, wait_meta: dict | None = None):
    """Return list of missing tags with details if available."""
    if not isinstance(missing, (list, tuple)):
        return missing
    ev = bundle.get('evidence', {}) if isinstance(bundle, dict) else {}
    meta = wait_meta or {}
    out = []
    def pick(*names):
        for nm in names:
            o = ev.get(nm)
            if isinstance(o, dict):
                return o
        return {}
    for tag in missing:
        t = str(tag)
        if t == "liquidity_floor":
            liq = ev.get("adaptive") or {}
            vr  = liq.get("liq_ratio") or liq.get("liquidity_ratio")  # đúng nguồn
            reg = liq.get("regime") or "normal"
            thr = liq.get("liq_thr")
            out.append(f"liquidity_floor{{liq_ratio={_fmt_float(vr)}, thr={_fmt_float(thr)}, regime={reg}}}")
        elif t in ("no_side","direction_undecided"):
            ta = ev.get("trend_alignment") or {}
            vol = ev.get("volume") or {}
            votes = meta.get("side_votes") or {}
            out.append(
                f"{t}{{trend_ok={bool(ta.get('ok'))}, vol_grade={(vol.get('grade') or '')}, votes={{ {', '.join([f'{k}={_fmt_float(v)}' for k,v in votes.items()])} }} }}"
            )
        elif t in ("near_heavy_zone","hvn_guard"):
            liq = ev.get("liquidity") or {}
            out.append(_fmt_ev_details("near_heavy_zone", liq))
        elif t in ("rr_too_low","far_from_entry","incomplete_setup"):
            out.append(t)
        else:
            out.append(t)
    return out

def _extract_evidence_ok(bundle: dict):
    """
    Trả về list evidence đang 'ok' (kèm side nếu có), ví dụ:
    ['retest:long', 'mean_reversion:long', 'volume_impulse_up']
    Bundle có dạng {'evidence': {...}} hoặc object tương đương.
    """
    try:
        ev = bundle.get('evidence', {}) if isinstance(bundle, dict) else {}
    except Exception:
        ev = {}
    out = []
    for name, obj in (ev or {}).items():
        ok = False
        side = None
        if isinstance(obj, dict):
            ok = bool(obj.get('ok'))
            side = obj.get('side')
        else:
            ok = bool(getattr(obj, 'ok', False))
            side = getattr(obj, 'side', None)
        if ok:
            out.append(f"{name}:{side}" if side in ("long","short") else name)
    return sorted(out)

# --- Telegram Teaser Notifier (init-once, lazy) ---
TN = None
def _get_notifier():
    """Create TelegramNotifier once; return False if init failed."""
    global TN
    if TN is None:
        try:
            TN = TelegramNotifier()
        except Exception as e:
            log.warning(f"TelegramNotifier init failed; disabled. reason={e}")
            TN = False
    return TN
# --- end telegram notifier helper ---

def split_into_6_blocks(symbols: List[str]) -> List[List[str]]:
    """Stable split into 6 blocks: [s[0], s[6], ...], [s[1], s[7], ...], ..."""
    return [symbols[i::6] for i in range(6)]

def which_block_for_minute(minute: int):
    # Twice per hour schedule for 6 blocks (VN time, every 5 minutes)
    mapping = {
        0: 0,  5: 1, 10: 2, 15: 3, 20: 4, 25: 5,
        30: 0, 35: 1, 40: 2, 45: 3, 50: 4, 55: 5,
    }
    return mapping.get(minute % 60)

def send_telegram(text: str):
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not chat or not text:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": text}
        )
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

def _enrich_all(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    out = {}
    for tf, df in dfs.items():
        if df is None or df.empty:
            out[tf] = df
            continue
        x = enrich_indicators(df)
        x = enrich_more(x)
        out[tf] = x
    return out

def process_symbol(symbol: str, cfg: Config, limit: int, ex=None):
    t0 = time.time()
    log.debug(f"[{symbol}] fetching OHLCV…")
    # fetch with partial-bar drop for 1H; realtime for 4H/1D (handled in fetch_batch)
    sleep_between_tf = float(os.getenv("SLEEP_BETWEEN_TF", "0.3"))
    dfs = fetch_batch(
        symbol,
        timeframes=TIMEFRAMES,
        limit=limit,
        drop_partial=True,        # only applied to 1H internally
        sleep_between_tf=sleep_between_tf,  # reduce burst per symbol
        ex=ex                     # reuse shared exchange to avoid 429
    )
    t_fetch = time.time() - t0
    df1 = dfs.get("1H")
    df4 = dfs.get("4H")
    dfD = dfs.get("1D")
    l1 = 0 if df1 is None else len(df1.index)
    l4 = 0 if df4 is None else len(df4.index)
    lD = 0 if dfD is None else len(dfD.index)
    log.debug(f"[{symbol}] fetched: 1H={l1}, 4H={l4}, 1D={lD} in {t_fetch:.2f}s")

    # enrich indicators → features_by_tf
    t1 = time.time()
    dfs = _enrich_all(dfs)
    log.debug(f"[{symbol}] enrich done in {time.time()-t1:.2f}s")
    t2 = time.time()
    feats_by_tf = compute_features_by_tf(dfs)   # builds trend/momentum/volatility/levels/vp-bands,…
    log.debug(f"[{symbol}] features done in {time.time()-t2:.2f}s")
    # attach df to 4H (primary/execution) & 1H (phụ nếu cần)
    if '4H' in feats_by_tf:
        feats_by_tf['4H']['df'] = dfs.get('4H')
    if '1H' in feats_by_tf:
        feats_by_tf['1H']['df'] = dfs.get('1H')

    # evidence bundle (STRUCT JSON)
    t3 = time.time()
    bundle = build_evidence_bundle(symbol, feats_by_tf, cfg)
    log.debug(f"[{symbol}] bundle done in {time.time()-t3:.2f}s")

    # decide on 4H as execution TF (1H trigger, 4H execution, 1D context)
    t4 = time.time()
    try:
        out = decide(symbol, "4H", feats_by_tf, bundle)
    except Exception as e:
        log.exception(f"[{symbol}] decide failed: {e}")
        # Fallback để tiếp tục vòng lặp, không làm gãy block
        out = {
            "symbol": symbol,
            "decision": "AVOID",
            "state": None,
            "plan": {},
            "logs": {"AVOID": {"reasons": ["internal_error"]}},
        }
        print(json.dumps(out, ensure_ascii=False), flush=True)
        return
    elapsed_dec = time.time() - t4
    total_time = time.time() - t0
    dec = out.get("decision")
    state = out.get("state")
    plan = out.get("plan") or {}
    log.debug(f"[{symbol}] decide done in {elapsed_dec:.2f}s; total {total_time:.2f}s")
    # Prefer concise headline from decision_engine if available (already includes DIR/TP ladder)
    headline = out.get("headline")
    if headline:
        log.info(headline)
    else:
        # Build TP ladder + RR ladder + direction
        dir_val = (plan.get("direction") or plan.get("dir") or "-")
        # Backward compatible: if only single tp/rr exists, map to TP1/RR1
        tp1 = plan.get("tp1", plan.get("tp"))
        tp2 = plan.get("tp2")
        tp3 = plan.get("tp3")
        rr1 = plan.get("rr1", plan.get("rr"))
        rr2 = plan.get("rr2")
        rr3 = plan.get("rr3")

        tp_parts = []
        if tp1 is not None: tp_parts.append(f"TP1={tp1}")
        if tp2 is not None: tp_parts.append(f"TP2={tp2}")
        if tp3 is not None: tp_parts.append(f"TP3={tp3}")
        rr_parts = []
        if rr1 is not None: rr_parts.append(f"RR1={rr1}")
        if rr2 is not None: rr_parts.append(f"RR2={rr2}")
        if rr3 is not None: rr_parts.append(f"RR3={rr3}")
        lev = plan.get("risk_size_hint")
        if isinstance(lev, (int, float)):
            import math
            lev_disp = math.floor(float(lev))
            lev_part = f"LEV={lev_disp:.1f}x"
        else:
            lev_part = None

        tp_str = " ".join(tp_parts)
        rr_str = " ".join(rr_parts)
        extra = (" " + lev_part) if lev_part else ""

        log.info(
            f"[{symbol}] DECISION={dec} | STATE={state} | "
            f"DIR={str(dir_val).upper()} | "
            f"entry={plan.get('entry')} entry2={plan.get('entry2')} "
            f"sl={plan.get('sl')} "
            f"{(tp_str + ' ' + rr_str).strip()}{extra}".strip()
        )
    if dec == "WAIT":
        # --- WAIT branch logging (detail) ---
        miss = None
        wait_meta = {}
        
        logs = out.get("logs")
        if isinstance(logs, dict):
            wait_log = logs.get("WAIT") or {}
            if isinstance(wait_log, dict):
                miss = wait_log.get("missing") or wait_log.get("reasons")
                wait_meta = wait_log.get("state_meta") or {}
        
        # Fallback khi WAIT không có missing/reasons
        if miss is None:
            miss = out.get("reasons")
        
        # In chi tiết missing/have
        miss_detail = _describe_missing_tags(miss, bundle, wait_meta)
        have_detail = _extract_evidence_ok_detailed(bundle)
        # --- Giải thích 'no_side' nếu có ---
        _has_no_side = False
        if isinstance(miss_detail, list):
            _has_no_side = any(str(x).startswith("no_side") or str(x) == "direction_undecided" for x in miss_detail)
        elif isinstance(miss_detail, str):
            _has_no_side = miss_detail.startswith("no_side") or miss_detail == "direction_undecided"
        if _has_no_side:
            _reason, _has_brk, _has_rt = _no_side_reason(wait_meta, bundle)
            log.info(f"[{symbol}] WHY no_side: {_reason} votes={wait_meta.get('side_votes')} tf_long={wait_meta.get('tf_long')} tf_short={wait_meta.get('tf_short')} gates={{breakout:{_has_brk}, retest:{_has_rt}}}")
        log.info(f"[{symbol}] WAIT missing={miss_detail} have={have_detail}")

    # --- post teaser to Telegram Channel when ENTER ---
    if dec == "ENTER":
        tn = _get_notifier()
        if tn:
            try:
                plan_for_teaser = dict(plan or {})
                plan_for_teaser.update({
                    "symbol": symbol,
                    "DIRECTION": (plan.get("direction") or plan.get("dir") or "-").upper() if isinstance(plan, dict) else "-",
                    "STATE": state,
                    "notes": out.get("notes", []),
                })
                perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR","./data")))
                # 24h cooldown
                if perf.cooldown_active(symbol, seconds=24*3600):
                    log.info(f"[{symbol}] skip ENTER due to cooldown (24h)")
                else:
                    # --- NEW: quota theo khung giờ VN (05:30–07:30, 17:30–19:30) — tối đa 1 tín hiệu ---
                    now_local = datetime.now(TZ)
                    win = _current_vn_window(now_local)
                    if win is not None:
                        start_ts, end_ts = win
                        released = perf.count_released_between(start_ts, end_ts)
                        if released >= 1:
                            log.info(f"[{symbol}] skip ENTER due to window quota ({released}) at {now_local.strftime('%H:%M')}")
                            # Chặn phát hành trong khung giờ nếu đã có 1 tín hiệu
                            return
                    sid, msg_id = tn.post_teaser(plan_for_teaser)
                    perf.open(sid, plan_for_teaser, message_id=msg_id)
            except Exception as e:
                log.warning(f"[{symbol}] teaser post failed: {e}")
                
    # --- end teaser post ---
    # --- progress check: update TP/SL hits for existing OPEN trades ---
    try:
        df_1h = dfs.get("1H")
        if df_1h is None or df_1h.empty:
            raise ValueError("missing 1H frame")
        price_now = float(df_1h["close"].iloc[-1])

        perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR","./data")))
        open_trades = perf.by_symbol(symbol)
        if open_trades:
            tn2 = _get_notifier()
            for t in open_trades:
                def crossed(side, price, level):
                    return (side=="LONG" and price>=level) or (side=="SHORT" and price<=level)
                side = (t.get("dir") or "").upper()
                msg_id = t.get("message_id")
                entry = float(t.get("entry") or 0.0)
                
                # Đã hit rồi thì bỏ qua (idempotent)
                hits = t.get("hits", {})
                msg_id = t.get("message_id")
                entry = float(t.get("entry") or 0.0)
                def margin_pct(hit_price: float) -> float:
                    if not entry: return 0.0
                    return ((hit_price - entry) / entry * 100.0) if side=="LONG" else ((entry - hit_price) / entry * 100.0)

                # TP1
                if t.get("status")=="OPEN" and not hits.get("TP1") and t.get("tp1") and crossed(side, price_now, t["tp1"]):
                    perf.set_hit(t["sid"], "TP1", (t.get("r_ladder",{}) or {}).get("tp1") or 0.0)
                    hits["TP1"] = int(__import__("time").time())
                    t["status"] = "TP1"
                    note = "🎯 TP1 hit — Nâng SL lên Entry để bảo toàn lợi nhuận."
                    extra = {"margin_pct": margin_pct(float(t["tp1"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))
                        
                # TP2
                if t.get("status") in ("OPEN","TP1") and not hits.get("TP2") and t.get("tp2") and crossed(side, price_now, t["tp2"]):
                    perf.set_hit(t["sid"], "TP2", (t.get("r_ladder",{}) or {}).get("tp2") or 0.0)
                    hits["TP2"] = int(__import__("time").time())
                    t["status"] = "TP2"
                    note = "🎯 TP2 hit — Nâng SL lên Entry để bảo toàn lợi nhuận."
                    extra = {"margin_pct": margin_pct(float(t["tp2"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

                # TP3 => đóng lệnh
                if t.get("status") in ("OPEN","TP1","TP2") and t.get("tp3") and crossed(side, price_now, t["tp3"]):
                    perf.close(t["sid"], "TP3")
                    t["status"] = "TP3"
                    note = "🎯 TP3 hit — Tất cả TP đều đạt — Đóng lệnh."
                    extra = {"margin_pct": margin_pct(float(t["tp3"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

                # NEW: Nếu đã đạt TP1/TP2 mà giá quay ngược về Entry -> ĐÓNG LỆNH,
                # và hiển thị lợi nhuận theo TP cao nhất đã đạt.
                if t.get("status") in ("TP1","TP2") and entry:
                    retraced = (side == "LONG" and price_now <= entry) or (side == "SHORT" and price_now >= entry)
                    if retraced:
                        # chọn TP cao nhất đã đạt để tính % lợi nhuận hiển thị
                        highest = None
                        hit_price = None
                        if (t.get("status") == "TP2") or (hits.get("TP2")):
                            highest = "TP2"
                            hit_price = float(t.get("tp2") or entry)
                        else:
                            highest = "TP1"
                            hit_price = float(t.get("tp1") or entry)
                        perf.close(t["sid"], "ENTRY")
                        t["status"] = "CLOSE"
                        note = f"📌 Đóng lệnh — Giá quay về Entry sau khi đã đạt {highest}."
                        extra = {"margin_pct": margin_pct(hit_price)}
                        if tn2:
                            if msg_id:
                                tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                            else:
                                tn2.send_channel(render_update(t, note, extra))

                # --- SL => đóng lệnh (CHỈ khi chưa từng chạm TP nào)
                slv = t.get("sl")
                has_tp_hit = bool(hits.get("TP1") or hits.get("TP2") or hits.get("TP3"))
                if t.get("status") == "OPEN" and not has_tp_hit and slv and (
                    (side == "LONG"  and price_now <= slv) or
                    (side == "SHORT" and price_now >= slv)
                ):
                    perf.close(t["sid"], "SL")
                    t["status"] = "SL"
                    note = "⚠️ SL hit — Đóng lệnh."
                    extra = {"margin_pct": margin_pct(float(slv))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

        # nếu không có open_trades -> không làm gì, không log warning
    except Exception as e:
        log.warning("progress-check failed: %s", e)

    print(json.dumps(out, ensure_ascii=False), flush=True)
    if out.get("telegram_signal"):
        send_telegram(out["telegram_signal"])

def run_block(block_idx: int, symbols: List[str], cfg: Config, limit: int, total_blocks: int, ex=None):
    log.info(f"=== Running block {block_idx+1}/{total_blocks} ({len(symbols)} symbols) ===")
    sleep_between_symbols = float(os.getenv("SLEEP_BETWEEN_SYMBOLS", "0.15"))
    for sym in symbols:
        try:
            process_symbol(sym, cfg, limit, ex=ex)
            time.sleep(sleep_between_symbols)  # tiny pause between symbols to smooth rate limit
        except Exception as e:
            log.exception(f"[{sym}] error: {e}")

def loop_scheduler():
    symbols = get_universe_from_env()
    blocks = split_into_6_blocks(symbols)
    cfg = Config()  # default thresholds per TF
    limit = int(os.getenv("BATCH_LIMIT", "300"))
    # Create ONE shared exchange to let ccxt throttler pace requests correctly
    shared_ex = _exchange(
        kucoin_key=os.getenv("KUCOIN_API_KEY"),
        kucoin_secret=os.getenv("KUCOIN_API_SECRET"),
        kucoin_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
    )

    if os.getenv("RUN_ONCE") == "1":
        # Run all blocks immediately (useful for CI/test)
        for i in range(len(blocks)):
            run_block(i, blocks[i], cfg, limit, len(blocks), ex=shared_ex)
        return

    log.info(f"Universe size={len(symbols)}; block sizes={[len(b) for b in blocks]}")
    log.info("Schedule each hour (Asia/Ho_Chi_Minh): "
             "block1 at :00 & :30, block2 at :05 & :35, block3 at :10 & :40, "
             "block4 at :15 & :45, block5 at :20 & :50, block6 at :25 & :55")

    last_tick = None
    last_kpi_day = None  # NEW: để gửi KPI 1 lần/ngày
    while True:
        now = datetime.now(TZ)
        blk = which_block_for_minute(now.minute)
        # Include half-hour slot so each block can run twice per hour
        half = 0 if now.minute < 30 else 1
        tick_key = (now.year, now.month, now.day, now.hour, half, blk)
        if blk is not None and tick_key != last_tick and now.second < 10:
            last_tick = tick_key
            run_block(blk, blocks[blk], cfg, limit, len(blocks), ex=shared_ex)
        # NEW: KPI lúc 18:18 local (VN) ~ 11:18 UTC
        try:
            if now.hour == 18 and now.minute == 18 and (last_kpi_day != (now.year, now.month, now.day)):
                last_kpi_day = (now.year, now.month, now.day)
                # Teaser KPI: list 24H (chỉ lệnh ĐÓNG-chưa-báo-cáo) + hiệu suất NGÀY
                perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR", "./data")))
                detail_24h, sids_to_mark = perf.kpis_24h_unreported()
                kpi_day = perf.kpis("day")           # sumR, wr, ...
                detail_day = perf.kpis_detail("day") # equity 1x + tp_counts
                report_date_str = now.strftime("%d/%m/%Y")
                from templates import render_kpi_teaser_two_parts
                tn = _get_notifier()
                if tn:
                    html = render_kpi_teaser_two_parts(detail_24h, kpi_day, detail_day, report_date_str)
                    tn.send_kpi24(html)
                    # đánh dấu đã báo cáo KPI 24h
                    try:
                        perf.mark_kpi24_reported(sids_to_mark)
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"KPI-24H send failed: {e}")
        # sleep until next 5-minute boundary
        secs = now.second + now.minute*60
        to_next = 300 - (secs % 300)
        time.sleep(max(5, min(60, to_next)))

if __name__ == "__main__":
    try:
        loop_scheduler()
    except KeyboardInterrupt:
        sys.exit(0)
