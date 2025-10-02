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
import math, os, sys, time, json, logging, uuid
import threading
from typing import Any, Dict, List, TYPE_CHECKING, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from universe import get_universe_from_env  # uses DEFAULT_UNIVERSE if SYMBOLS not set
from kucoin_api import fetch_batch, _exchange  # spot-only; 1H drop-partial
from indicators import enrich_indicators, enrich_more
from feature_primitives import compute_features_by_tf
from engine_adapter import decide
from evidence_evaluators import build_evidence_bundle, Config, _reversal_signal

from notifier_telegram import TelegramNotifier
from storage import SignalPerfDB, JsonStore, UserDB
from templates import render_update, render_teaser
from fb_notifier import FBNotifier

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
TIMEFRAMES = ("1H", "4H", "1D")

log = logging.getLogger("worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")

# ============================================================
# Portfolio Risk Governance (Pre-entry + Rolling Drawdown)
# ============================================================
# ENV overrides (giá trị mặc định theo yêu cầu)
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

MAX_OPEN_PER_SIDE = _env_int("MAX_OPEN_PER_SIDE", 4)         # không tính lệnh đã TP1
MAX_RISK_EXPOSURE_R = _env_float("MAX_RISK_EXPOSURE_R", 4.0) # tổng R đang treo
DD_60M_CAP = _env_float("DD_60M_CAP", -1.5)                   # R
LOSING_STREAK_N = _env_int("LOSING_STREAK_N", 3)              # 3 SL liên tiếp
COOLDOWN_2H = 2 * 3600
COOLDOWN_3H = 3 * 3600
MAX_PER_TRADE_R = _env_float("MAX_PER_TRADE_R", 2.0)          # trần R mỗi lệnh để chống dữ liệu lỗi

# Cụm beta cao (đơn giản, có thể tinh chỉnh sau). So khớp theo BASE (trước "/USDT")
_CLUSTERS = [
    {"name": "SOL-AVAX-NEAR", "members": {"SOL","AVAX","NEAR"}},
    {"name": "ARB-OP-SUI",     "members": {"ARB","OP","SUI"}},
    {"name": "BNB-LINK",       "members": {"BNB","LINK"}},
    {"name": "PENDLE",         "members": {"PENDLE"}},
]
_HIGH_BETA = {"SOL","AVAX","NEAR","ARB","SUI"}  # dùng cho rule “không mở 2 lệnh beta cao cùng cụm”

class PortfolioStore:
    """Quản lý state phụ trợ cho các rule danh mục (cluster timestamps…)."""
    def __init__(self, store: JsonStore):
        self.store = store
        self.name = "portfolio_policy"
    def _read(self) -> dict:
        return self.store.read(self.name) or {}
    def _write(self, data: dict) -> None:
        self.store.write(self.name, data)
    def update_cluster_open(self, cluster_name: str, side: str) -> None:
        d = self._read()
        t = int(time.time())
        clusters = d.get("cluster_open_ts", {})
        key = f"{cluster_name}:{side.upper()}"
        clusters[key] = t
        d["cluster_open_ts"] = clusters
        self._write(d)
    def last_cluster_open_within(self, cluster_name: str, side: str, seconds: int) -> bool:
        d = self._read()
        clusters = d.get("cluster_open_ts", {})
        key = f"{cluster_name}:{side.upper()}"
        ts = int(clusters.get(key) or 0)
        return bool(ts and (int(time.time()) - ts) <= seconds)

def _base_from_symbol(sym: str) -> str:
    try:
        return str(sym.split("/")[0]).upper()
    except Exception:
        return str(sym).upper()

def _cluster_of(base: str):
    for c in _CLUSTERS:
        if base in c["members"]:
            return c["name"]
    return None

def _has_any_tp_hit(it: dict) -> bool:
    """Phát hiện lệnh đã từng chạm bất kỳ TP nào (TP1–TP5) theo nhiều schema khác nhau."""
    keys_true = ("hit_tp", "tp_hit", "tp_filled", "scale_out", "TP_HIT", "HIT_TP")
    for k in keys_true:
        if str(it.get(k)).lower() in ("true","1"):
            return True
    # Cờ riêng từng mức TP
    for k in ("hit_tp1","tp1_hit","HIT_TP1","hit_tp2","tp2_hit","HIT_TP2",
              "hit_tp3","tp3_hit","HIT_TP3","hit_tp4","tp4_hit","HIT_TP4",
              "hit_tp5","tp5_hit","HIT_TP5"):
        if str(it.get(k)).lower() in ("true","1"):
            return True
    # Đếm số TP đã khớp
    try:
        cnt = float(it.get("tp_hit_count") or it.get("TP_HIT_COUNT") or 0)
        if cnt and cnt > 0:
            return True
    except Exception:
        pass
    return False

def _count_open_by_side(perf: "SignalPerfDB") -> dict:
    """Đếm số lệnh đang OPEN theo side, **chỉ** tính lệnh CHƯA có TP nào."""
    res = {"LONG":0, "SHORT":0}
    try:
        opens = perf.list_open_status()  # kỳ vọng trả về list dict
        for it in opens or []:
            # chỉ tính lệnh OPEN và chưa TP
            st = (it.get("status") or it.get("STATUS") or "").upper()
            if st != "OPEN":
                continue
            if _has_any_tp_hit(it):
                continue
            side = (it.get("side") or it.get("DIRECTION") or "").upper()
            if side not in res: 
                continue
            res[side] += 1
    except Exception as e:
        log.warning(f"count_open_by_side fallback due to {e}")
    return res

def _total_risk_exposure_R(perf: "SignalPerfDB") -> float:
    """
    Tổng R đang treo **chỉ** của các lệnh đang OPEN và **chưa từng chạm TP**.
    - Chỉ tính status == OPEN
    - Loại trừ mọi lệnh đã có TP (TP1–TP5)
    - Chặn trần mỗi lệnh = MAX_PER_TRADE_R (ENV) để chống dữ liệu ghi sai
    - Log top contributors khi vượt ngưỡng để tiện debug
    """
    total = 0.0
    contrib = []  # [(symbol, side, r_each)]
    try:
        opens = perf.list_open_status()
        for it in opens or []:
            st = (it.get("status") or it.get("STATUS") or "").upper()
            if st != "OPEN":
                continue
            if _has_any_tp_hit(it):
                continue
            r_left = it.get("risk_R_remaining")
            if r_left is None:
                r_left = it.get("risk_R")
            if r_left is None:
                r_left = it.get("R")
            try:
                r_left = float(r_left)
            except Exception:
                r_left = 1.0
            if not (r_left == r_left) or r_left <= 0:  # NaN hoặc âm
                continue
            r_each = min(float(r_left), float(MAX_PER_TRADE_R))
            total += r_each
            if len(contrib) < 20:
                sym = it.get("symbol") or it.get("SYMBOL") or "?"
                side = (it.get("side") or it.get("DIRECTION") or "").upper()
                contrib.append((str(sym), side, r_each))
    except Exception as e:
        log.warning(f"risk_exposure_R fallback due to {e}")
    try:
        if total >= MAX_RISK_EXPOSURE_R:
            contrib_sorted = sorted(contrib, key=lambda x: x[2], reverse=True)[:5]
            dbg = ", ".join([f"{s}/{sd}:{r:.2f}R" for s,sd,r in contrib_sorted])
            log.info(f"[ExposureR] total={total:.2f}R (top: {dbg})")
    except Exception:
        pass
    return float(total)

def _recent_losing_streak(perf: "SignalPerfDB", side: str, n: int) -> bool:
    """Kiểm tra có n lệnh liên tiếp SL cùng side gần nhất không."""
    try:
        hist = perf.list_recent_history(limit=50)  # kỳ vọng có; nếu không sẽ except
        streak = 0
        for it in reversed(hist or []):  # mới nhất ở cuối → đảo để đi từ mới → cũ
            s = (it.get("side") or it.get("DIRECTION") or "").upper()
            if s and s != side.upper():
                continue
            status = (it.get("status") or it.get("final_status") or "").upper()
            if status in ("SL","STOP","STOP_LOSS","CLOSE_SL"):
                streak += 1
                if streak >= n:
                    return True
            elif status:
                # reset streak khi gặp kết quả khác SL (TP/BE/CLOSE sớm…)
                streak = 0
        return False
    except Exception:
        return False

def _pnl_rolling_60m_R(perf: "SignalPerfDB") -> float:
    """Tổng PnL dạng R trong 60 phút gần nhất (ưu tiên realized; nếu thiếu, ước lượng =0)."""
    cutoff = int(time.time()) - 3600
    acc = 0.0
    try:
        # Ưu tiên bản ghi đã đóng (realized)
        closed = perf.list_closed_since(ts=cutoff)
        for it in closed or []:
            r = it.get("realized_R")
            if r is None:
                r = it.get("R_realized") or it.get("R") or 0.0
            try:
                acc += float(r)
            except Exception:
                pass
    except Exception:
        pass
    return float(acc)

# =========================
# Market Flip Guards (BTC+ETH) — portfolio level
# =========================
class _MarketState:
    """Persist on-disk to coordinate side cooldown across runs."""
    def __init__(self, store: JsonStore):
        self.store = store
    def _read(self) -> dict:
        return self.store.read("market_state") or {}
    def _write(self, data: dict) -> None:
        self.store.write("market_state", data)
    def disable_side_until(self, side: str, until_ts: int) -> None:
        side = (side or "").upper()
        data = self._read()
        ds = data.get("disable_side", {})
        ds[side] = int(until_ts)
        data["disable_side"] = ds
        self._write(data)
    def is_side_disabled(self, side: str) -> bool:
        side = (side or "").upper()
        now = int(time.time())
        ds = (self._read().get("disable_side") or {})
        ts = int(ds.get(side) or 0)
        return bool(ts and now < ts)
    def cooldown_side(self, side: str, seconds: int, reason: str = ""):
        until_ts = int(time.time()) + int(seconds)
        self.disable_side_until(side, until_ts)
        if reason:
            log.info(f"cooldown side {side} for {seconds//3600}h due to {reason}")

def _hold_above(df: pd.DataFrame, ema_len: int, bars: int) -> bool:
    try:
        if df is None or df.empty: return False
        ema = df[f"ema{ema_len}"].iloc[-bars:]
        cls = df["close"].iloc[-bars:]
        return bool((cls > ema).all())
    except Exception:
        return False

def _rsi_trough_ok(df: pd.DataFrame, thr: float) -> bool:
    try:
        if df is None or len(df) < 5: return False
        r = df["rsi14"].iloc[-5:]
        return float(r.min()) > float(thr)
    except Exception:
        return False

def _swing_high(df: pd.DataFrame) -> float | None:
    try:
        return float(df["high"].iloc[-5:-1].max())
    except Exception:
        return None

def _swing_low(df: pd.DataFrame) -> float | None:
    try:
        return float(df["low"].iloc[-5:-1].min())
    except Exception:
        return None

def _vol_ratio(df: pd.DataFrame) -> float:
    try:
        v = float(df["volume"].iloc[-2])
        ma20 = float(df["vol_sma20"].iloc[-2] if "vol_sma20" in df.columns else df["volume"].rolling(20).mean().iloc[-2])
        return v/ma20 if ma20>0 else 0.0
    except Exception:
        return 0.0

def _fast_flip_up_1h(btc1h: pd.DataFrame, eth1h: pd.DataFrame) -> bool:
    ok = 0
    try:
        if _hold_above(btc1h, 50, 3): ok += 1
        if float(btc1h["rsi14"].iloc[-2]) > 55 and _rsi_trough_ok(btc1h, 45): ok += 1
        sw = _swing_high(btc1h); atr = float(btc1h["atr14"].iloc[-2])
        if sw is not None and float(btc1h["close"].iloc[-2]) > sw + 0.7*atr: ok += 1
        if _vol_ratio(btc1h) >= 1.5: ok += 1
    except Exception:
        pass
    eth_ok = False
    try:
        eth_ok = sum([
            int(float(eth1h["close"].iloc[-2]) > float(eth1h["ema50"].iloc[-2])),
            int(float(eth1h["rsi14"].iloc[-2]) > 55),
            int((_swing_high(eth1h) or 0) and float(eth1h["close"].iloc[-2]) > _swing_high(eth1h) + 0.7*float(eth1h["atr14"].iloc[-2])),
        ]) >= 2
    except Exception:
        eth_ok = False
    return (ok >= 3) or (ok >= 2 and eth_ok)

def _fast_flip_down_1h(btc1h: pd.DataFrame, eth1h: pd.DataFrame) -> bool:
    ok = 0
    try:
        if (btc1h["close"].iloc[-3:] < btc1h["ema50"].iloc[-3:]).all(): ok += 1
        if float(btc1h["rsi14"].iloc[-2]) < 45 and (btc1h["rsi14"].iloc[-5:-1].max() < 55): ok += 1
        sw = _swing_low(btc1h); atr = float(btc1h["atr14"].iloc[-2])
        if sw is not None and float(btc1h["close"].iloc[-2]) < sw - 0.7*atr: ok += 1
        if _vol_ratio(btc1h) >= 1.5: ok += 1
    except Exception:
        pass
    eth_ok = False
    try:
        eth_ok = sum([
            int((eth1h["close"].iloc[-3:] < eth1h["ema50"].iloc[-3:]).all()),
            int(float(eth1h["rsi14"].iloc[-2]) < 45),
            int((_swing_low(eth1h) or 0) and float(eth1h["close"].iloc[-2]) < _swing_low(eth1h) - 0.7*float(eth1h["atr14"].iloc[-2])),
        ]) >= 2
    except Exception:
        eth_ok = False
    return (ok >= 3) or (ok >= 2 and eth_ok)

def run_market_guards(exchange) -> _MarketState:
    """Fetch BTC/ETH 1H quickly and toggle side cooldowns if flip detected."""
    try:
        limit = int(os.getenv("BATCH_LIMIT", "200"))
    except Exception:
        limit = 200
    dfs_btc = fetch_batch("BTC/USDT", timeframes=["1H"], limit=limit, drop_partial=True, ex=exchange)
    dfs_eth = fetch_batch("ETH/USDT", timeframes=["1H"], limit=limit, drop_partial=True, ex=exchange)
    btc1h = (dfs_btc or {}).get("1H"); eth1h = (dfs_eth or {}).get("1H")
    ms = _MarketState(JsonStore(os.getenv("DATA_DIR","./data")))
    tn = _get_notifier()
    now = int(time.time())
    if _fast_flip_up_1h(btc1h, eth1h):
        ms.disable_side_until("SHORT", now + 3*3600)
        if tn:
            try: tn.post_text("⛔ Phòng hộ: Ngưng phát lệnh SHORT do BTC/ETH dốc lên.")
            except Exception: pass
    if _fast_flip_down_1h(btc1h, eth1h):
        ms.disable_side_until("LONG", now + 3*3600)
        if tn:
            try: tn.post_text("⛔ Phòng hộ: Ngưng phát lệnh LONG do BTC/ETH dốc xuống.")
            except Exception: pass
    return ms

def run_portfolio_caps(perf: "SignalPerfDB", ms: _MarketState) -> None:
    """Kiểm tra rolling drawdown & losing streak để bật cooldown side tự động."""
    # 1) Rolling drawdown 60 phút (hai chiều)
    try:
        pnl60 = _pnl_rolling_60m_R(perf)
        if pnl60 <= DD_60M_CAP:
            # nếu đã âm mạnh trong giờ qua — cooldown cả hai side 2h, hoặc có thể tinh chỉnh theo side
            ms.cooldown_side("LONG", COOLDOWN_2H, reason="DD_60M_CAP")
            ms.cooldown_side("SHORT", COOLDOWN_2H, reason="DD_60M_CAP")
    except Exception as e:
        log.warning(f"run_portfolio_caps dd60 failed: {e}")
    # 2) Losing streak theo side
    try:
        if _recent_losing_streak(perf, "LONG", LOSING_STREAK_N):
            ms.cooldown_side("LONG", COOLDOWN_3H, reason="LOSING_STREAK")
        if _recent_losing_streak(perf, "SHORT", LOSING_STREAK_N):
            ms.cooldown_side("SHORT", COOLDOWN_3H, reason="LOSING_STREAK")
    except Exception as e:
        log.warning(f"run_portfolio_caps streak failed: {e}")

def _last_closed_row(df: pd.DataFrame) -> pd.Series | None:
    try:
        if df is None or df.empty:
            return None
        return df.iloc[-2] if len(df) >= 2 else df.iloc[-1]
    except Exception:
        return None

def _regime_from_bundle(bundle: dict) -> str:
    """
    Lấy regime ('low' | 'normal' | 'high') từ evidence.adaptive (nếu có),
    mặc định 'normal' khi không có dữ liệu.
    """
    try:
        ev = bundle.get("evidence", {}) if isinstance(bundle, dict) else {}
        ad = ev.get("adaptive") or {}
        reg = str(ad.get("regime") or "normal").lower()
        return reg if reg in ("low","normal","high") else "normal"
    except Exception:
        return "normal"

def _unrealized_R(trade: dict, px: float) -> float:
    """
    Tính R tức thời tại giá px.
    R = (P/L) / |entry - sl|
    """
    try:
        side  = (trade.get("dir") or trade.get("DIRECTION") or "").upper()
        entry = float(trade.get("entry"))
        sl    = float(trade.get("sl"))
        risk  = abs(entry - sl)
        if not px or not entry or not sl or risk <= 0:
            return 0.0
        if side == "LONG":
            return (px - entry) / risk
        elif side == "SHORT":
            return (entry - px) / risk
        return 0.0
    except Exception:
        return 0.0

def _mfe_R_since_open(df4: pd.DataFrame, trade: dict) -> float:
    """
    MFE tính theo 4H kể từ nến *đóng* gần thời điểm post lệnh.
    - Lấy posted_at (epoch) → tìm các nến 4H đóng sau thời điểm này
    - Với LONG: MFE dùng 'high'; SHORT: dùng 'low'
    - Quy đổi sang R: (extreme - entry)/risk (hoặc (entry - extreme)/risk cho SHORT)
    """
    try:
        if df4 is None or df4.empty:
            return 0.0
        side  = (trade.get("dir") or trade.get("DIRECTION") or "").upper()
        entry = float(trade.get("entry"))
        sl    = float(trade.get("sl"))
        risk  = abs(entry - sl)
        if risk <= 0 or side not in ("LONG","SHORT"):
            return 0.0
        posted_at = int(trade.get("posted_at") or 0)
        if not posted_at:
            return 0.0
        # Lọc các nến 4H đóng sau thời điểm post
        dff = df4.copy()
        # giả định index là epoch (sec) hoặc pandas timestamp → chuyển về epoch
        try:
            idx_epoch = dff.index.view('int64') // 10**9
        except Exception:
            try:
                idx_epoch = dff.index.astype('int64') // 10**9
            except Exception:
                idx_epoch = None
        if idx_epoch is None:
            return 0.0
        dff = dff[(idx_epoch >= posted_at)]
        if len(dff) == 0:
            return 0.0
        if side == "LONG":
            extreme = float(dff["high"].max())
            return (extreme - entry) / risk
        else:
            extreme = float(dff["low"].min())
            return (entry - extreme) / risk
    except Exception:
        return 0.0

def _bars_4h_since_ts(df4: pd.DataFrame, since_ts: int) -> int:
    """Đếm số nến 4H *đã đóng* kể từ epoch `since_ts`."""
    try:
        if df4 is None or df4.empty or not since_ts:
            return 0
        try:
            idx_epoch = df4.index.view('int64') // 10**9
        except Exception:
            try:
                idx_epoch = df4.index.astype('int64') // 10**9
            except Exception:
                idx_epoch = None
        if idx_epoch is None:
            return 0
        return int((idx_epoch >= int(since_ts)).sum())
    except Exception:
        return 0

def _mfe_R_since_ts(df4: pd.DataFrame, trade: dict, since_ts: int) -> float:
    """
    MFE quy đổi R kể từ nến 4H *đóng* sau thời điểm since_ts.
    Dùng high cho LONG, low cho SHORT. R = (extreme-entry)/risk (hoặc đảo dấu cho SHORT).
    """
    try:
        if df4 is None or df4.empty or not since_ts:
            return 0.0
        side  = (trade.get("dir") or trade.get("DIRECTION") or "").upper()
        entry = float(trade.get("entry"))
        sl    = float(trade.get("sl"))
        risk  = abs(entry - sl)
        if risk <= 0 or side not in ("LONG","SHORT"):
            return 0.0
        dff = df4.copy()
        try:
            idx_epoch = dff.index.view('int64') // 10**9
        except Exception:
            try:
                idx_epoch = dff.index.astype('int64') // 10**9
            except Exception:
                idx_epoch = None
        if idx_epoch is None:
            return 0.0
        dff = dff[(idx_epoch >= int(since_ts))]
        if len(dff) == 0:
            return 0.0
        if side == "LONG":
            extreme = float(dff["high"].max())
            return (extreme - entry) / risk
        else:
            extreme = float(dff["low"].min())
            return (entry - extreme) / risk
    except Exception:
        return 0.0

def _bars_4h_since_open(df4: pd.DataFrame, trade: dict) -> int:
    """Đếm số nến 4H *đã đóng* kể từ khi post lệnh."""
    try:
        if df4 is None or df4.empty:
            return 0
        posted_at = int(trade.get("posted_at") or 0)
        if not posted_at:
            return 0
        try:
            idx_epoch = df4.index.view('int64') // 10**9
        except Exception:
            try:
                idx_epoch = df4.index.astype('int64') // 10**9
            except Exception:
                idx_epoch = None
        if idx_epoch is None:
            return 0
        return int((idx_epoch >= posted_at).sum())
    except Exception:
        return 0

def _time_exit_and_breakeven_checks(symbol: str,
                                    df4: pd.DataFrame,
                                    price_now: float,
                                    bundle: dict,
                                    perfdb) -> None:
    """
    Thực thi 2 cơ chế:
    1) Time-based exit khi LOW/NORMAL:
       - Sau >=3 nến 4H kể từ open mà MFE_R < +0.3R ⇒ CLOSE sớm (cap −0.2R).
    2) Breakeven turbo khi LOW/NORMAL:
       - Chưa TP1; nếu R_now ≥ 0.6R (low) hoặc 0.8R (normal) ⇒ dời SL về Entry (sl_dyn=entry).
    Gửi thông báo qua Telegram bằng format chung.
    """
    try:
        reg = _regime_from_bundle(bundle)  # 'low'|'normal'|'high'
        if reg not in ("low", "normal"):
            return
        tn = _get_notifier()
        if not tn:
            pass
        # Duyệt tất cả lệnh đang sống của symbol
        open_trades = perfdb.by_symbol(symbol)
        for t in open_trades:
            status = (t.get("status") or "OPEN").upper()
            # -------- Breakeven Turbo (áp dụng khi chưa TP1) --------
            if status == "OPEN":
                R_now = _unrealized_R(t, price_now)
                thr = 0.6 if reg == "low" else 0.8
                be_flag = bool(t.get("breakeven_turbo"))
                # chống trùng lặp: đã từng gửi thông báo BE cho lệnh này?
                be_notified = bool(t.get("be_notify_ts"))
                if (R_now >= thr) and (not be_flag) and (not be_notified):
                    # dời SL động về Entry, đánh dấu đã kích hoạt BE và lưu mốc trigger để theo dõi stall-fail
                    now_ts = int(time.time())
                    upd = perfdb.update_fields(
                        t["sid"],
                        sl_dyn=float(t.get("entry")),
                        breakeven_turbo=True,
                        be_notify_ts=now_ts,           # chống trùng lặp thông báo BE
                        be_trigger_ts=now_ts,          # mốc kích hoạt 0.6R/0.8R
                        be_peak_R=float(R_now)         # peak R kể từ trigger
                    )
                    # notify (một lần duy nhất)
                    mid = int(upd.get("message_id") or 0)
                    if tn and mid:
                        html = render_update(
                            {"symbol": t.get("symbol"), "DIRECTION": t.get("dir")},
                            event="Dời SL về Entry.",
                            extra=None
                        )
                        tn.send_channel_update(mid, html)

                # -------- Stall-&-Fail sau trigger (LOW/NORMAL) --------
                # Chỉ xét khi đã kích hoạt BE, chưa TP1, còn OPEN
                hits = t.get("hits") or {}
                has_tp1 = bool(hits.get("TP1"))
                trig_ts = int(t.get("be_trigger_ts") or 0)
                if (status == "OPEN") and (trig_ts > 0) and (not has_tp1):
                    # Cửa sổ quan sát: LOW=2 nến 4H, NORMAL=3 nến 4H
                    window_n = 2 if reg == "low" else 3
                    bars = _bars_4h_since_ts(df4, trig_ts)
                    if bars >= window_n:
                        # A) Progress test: MFE kể từ trigger không tăng đủ
                        #    progress = MFE_since_trigger - threshold_at_trigger
                        thr_prog = 0.15 if reg == "low" else 0.20
                        mfe_trig = _mfe_R_since_ts(df4, t, trig_ts)
                        progress = max(0.0, float(mfe_trig - thr))
                        # Khoảng cách còn lại tới TP1 tính theo R (nếu có TP1)
                        try:
                            entry = float(t.get("entry") or 0.0)
                            sl    = float(t.get("sl") or 0.0)
                            risk  = abs(entry - sl) if entry and sl else 0.0
                            tp1_px = float(t.get("tp1")) if t.get("tp1") else None
                            if risk > 0 and tp1_px:
                                tp1_R = ((tp1_px - entry) / risk) if (t.get("dir","").upper()=="LONG") else ((entry - tp1_px) / risk)
                                dist_tp1 = float(tp1_R - R_now)
                                dist_ok = (dist_tp1 >= 0.15)
                            else:
                                dist_ok = True  # không có TP1 => không chặn bởi dist
                        except Exception:
                            dist_ok = True
                        progress_ok = (progress < thr_prog) and dist_ok

                        # Cập nhật peak_R kể từ trigger
                        try:
                            prev_peak = float(t.get("be_peak_R") or thr)
                        except Exception:
                            prev_peak = thr
                        peak_R = max(prev_peak, float(mfe_trig))
                        if peak_R > prev_peak:
                            perfdb.update_fields(t["sid"], be_peak_R=peak_R)

                        # B) Give-back test: peak_R - R_now đủ sâu
                        give_thr = 0.35 if reg == "low" else 0.30
                        give_back = float(max(0.0, peak_R - R_now))
                        give_ok = (give_back >= give_thr)

                        # REVERSAL (đã có sẵn) — thay thế momentum flip
                        try:
                            df_4h = df4
                            df_1h = None  # có thể truyền 1H nếu muốn chặt hơn
                            is_rev, _why = _reversal_signal((t.get("dir") or "").upper(), df_4h, df_1h)
                        except Exception:
                            is_rev = False

                        # QUY TẮC: CLOSE nếu A & (B hoặc REVERSAL)
                        if progress_ok and (give_ok or is_rev):
                            # Tính R_now và ghi KPI weighted 20%
                            R_cap = R_now  # không cap cứng trong stall-fail
                            new_R = float(t.get("realized_R") or 0.0) + 0.2 * R_cap
                            perfdb.update_fields(t["sid"], realized_R=new_R)
                            perfdb.close(t["sid"], reason="STALL_FAIL_AFTER_TRIGGER")
                            # notify
                            mid = int((t.get("message_id") or 0))
                            msg = "Đóng lệnh sớm - Giá chững lại, có dấu hiệu suy yếu/đảo chiều."
                            html = render_update({"symbol": t.get("symbol"),
                                                  "DIRECTION": t.get("dir")},
                                                  event=msg,
                                                  extra={"margin_pct": None})
                            if tn and mid:
                                tn.send_channel_update(mid, html)

            # -------- Time-based exit 3 x 4H (< +0.3R) --------
            bars = _bars_4h_since_open(df4, t)
            if bars >= 3:
                mfeR = _mfe_R_since_open(df4, t)
                if mfeR < 0.3:
                    # Tính R ở giá hiện tại và cap −0.2R (weighted 20%)
                    R_now = _unrealized_R(t, price_now)
                    R_cap = max(R_now, -0.2)
                    new_R = float(t.get("realized_R") or 0.0) + 0.2 * R_cap
                    perfdb.update_fields(t["sid"], realized_R=new_R)
                    perfdb.close(t["sid"], reason="TIME_EXIT")
                    # notify
                    mid = int((t.get("message_id") or 0))
                    msg = "Đóng lệnh sớm - Giá không có tiến triển."
                    html = render_update({"symbol": t.get("symbol"),
                                          "DIRECTION": t.get("dir")},
                                          event=msg,
                                          extra={"margin_pct": None})
                    if tn and mid:
                        tn.send_channel_update(mid, html)
    except Exception as e:
        log.warning(f"time-exit/breakeven checks failed for {symbol}: {e}")

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

# --- Facebook Fanpage Notifier (init-once, lazy) ---
FB = None
def _get_fb_notifier():
    global FB
    if FB is None:
        try:
            FB = FBNotifier()
        except Exception as e:
            log.warning(f"FBNotifier init failed; disabled. reason={e}")
            FB = False
    return FB
# --- end fb notifier helper ---

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
        fb = _get_fb_notifier()
        try:
            plan_for_teaser = dict(plan or {})
            plan_for_teaser.update({
                "symbol": symbol,
                "DIRECTION": (plan.get("direction") or plan.get("dir") or "-").upper() if isinstance(plan, dict) else "-",
                "STATE": state,
                "notes": out.get("notes", []),
            })
            perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR","./data")))
            # 0) Block by market-side cooldown (Early Flip Guard)
            try:
                _ms = _MarketState(JsonStore(os.getenv("DATA_DIR","./data")))
                _side = (plan_for_teaser.get("DIRECTION") or "-").upper()
                if _side in ("LONG","SHORT") and _ms.is_side_disabled(_side):
                    log.info(f"[{symbol}] skip ENTER due to market guard side-block: {_side}")
                    return
            except Exception:
                pass
            # 0.1) Pre-entry guards — đếm lệnh/ R đang treo / tương quan-beta
            try:
                counts = _count_open_by_side(perf)
                side_up = (plan_for_teaser.get("DIRECTION") or "").upper()
                if side_up in ("LONG","SHORT"):
                    if counts.get(side_up, 0) >= MAX_OPEN_PER_SIDE:
                        log.info(f"[{symbol}] skip ENTER: MAX_OPEN_PER_SIDE reached for {side_up}")
                        return
                # Giới hạn tổng R đang treo
                exposureR = _total_risk_exposure_R(perf)
                if exposureR >= MAX_RISK_EXPOSURE_R:
                    log.info(f"[{symbol}] skip ENTER: exposureR {exposureR:.2f} ≥ {MAX_RISK_EXPOSURE_R}")
                    return
                # Lọc tương quan/beta theo cụm trong 60 phút
                base = _base_from_symbol(symbol)
                cluster = _cluster_of(base)
                if cluster and base in _HIGH_BETA:
                    pstore = PortfolioStore(JsonStore(os.getenv("DATA_DIR","./data")))
                    if pstore.last_cluster_open_within(cluster, side_up, 60*60):
                        log.info(f"[{symbol}] skip ENTER: cluster '{cluster}' {side_up} within 60m (beta-high)")
                        return
                    # nếu mở mới hợp lệ, cập nhật timestamp cụm
                    pstore.update_cluster_open(cluster, side_up)
            except Exception as e:
                log.warning(f"pre-entry guards skipped due to {e}")
            # 1) Check cooldown 24h trước khi post
            if perf.cooldown_active(symbol, seconds=24*3600):
                log.info(f"[{symbol}] skip ENTER due to cooldown (24h)")
            else:
                # 2) HL baseline để theo dõi TP/SL intrabar
                def _cur_hl(df):
                    return (float(df["high"].iloc[-1]), float(df["low"].iloc[-1])) if df is not None and not df.empty else (None, None)
                hi4, lo4 = _cur_hl(dfs.get("4H"))
                hi1, lo1 = _cur_hl(dfs.get("1H"))
                # 3) Gửi Telegram nếu có; nếu không, tự sinh sid
                sid = None
                msg_id = None
                if tn:
                    try:
                        sid, msg_id = tn.post_teaser(plan_for_teaser)
                    except Exception as e:
                        log.warning(f"[{symbol}] teaser post failed: {e}")
                if not sid:
                    try:
                        import uuid as _uuid
                        sid = str(_uuid.uuid4())[:8]
                    except Exception:
                        sid = f"{symbol}-{int(time.time())}"
                # 4) Ghi DB NGAY để kích hoạt cooldown, không phụ thuộc Telegram
                perf.open(
                    sid,
                    plan_for_teaser,
                    message_id=msg_id,
                    posted_at=int(time.time()),
                    hl0_4h_hi=hi4, hl0_4h_lo=lo4,
                    hl0_1h_hi=hi1, hl0_1h_lo=lo1,
                )
                # 5) Đăng lên Fanpage (độc lập với Telegram)
                try:
                    if fb:
                        html_teaser = render_teaser(plan_for_teaser)
                        origin_url = None
                        try:
                            if msg_id and hasattr(tn, "_build_origin_link"):
                                origin_url = tn._build_origin_link(int(msg_id))
                        except Exception:
                            origin_url = None
                        fb.post_teaser(html_teaser, origin_url=origin_url)
                except Exception as e:
                    log.warning(f"[{symbol}] fanpage teaser failed: {e}")
        except Exception as e:
            log.warning(f"[{symbol}] ENTER flow failed: {e}")
                
    # --- end teaser post ---

    # Sau khi có dữ liệu df4 và price hiện tại, chạy các check thoát sớm/BE
    try:
        price_now = None
        if df4 is not None and len(df4):
            price_now = float(df4["close"].iloc[-1])
        elif df1 is not None and len(df1):
            price_now = float(df1["close"].iloc[-1])
        if price_now is not None:
            # Dùng lại 'bundle' đã build từ feats_by_tf ở trên (đúng cấu trúc)
            _time_exit_and_breakeven_checks(
                symbol,
                dfs.get("4H"),
                price_now,
                bundle,
                SignalPerfDB(JsonStore(os.getenv("DATA_DIR", "./data")))
            )
    except Exception as e:
        log.warning(f"[{symbol}] post-scan checks failed: {e}")
      
    # --- progress check: update TP/SL hits for existing OPEN trades ---
    try:
        df_1h = dfs.get("1H")
        if df_1h is None or df_1h.empty:
            raise ValueError("missing 1H frame")
        # Ưu tiên khung 4H; nếu thiếu dùng 1H. Dùng cả HIGH/LOW để bắt intrabar.
        def _last_hl(df):
            return float(df["high"].iloc[-1]), float(df["low"].iloc[-1]), float(df["close"].iloc[-1])
        hi = lo = price_now = None
        try:
            df_4h = dfs.get("4H")
            if df_4h is not None and not df_4h.empty:
                hi, lo, price_now = _last_hl(df_4h)
        except Exception:
            pass
        if price_now is None:
            hi, lo, price_now = _last_hl(df_1h)

        perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR","./data")))
        open_trades = perf.by_symbol(symbol)
        if open_trades:
            tn2 = _get_notifier()
            for t in open_trades:
                # Baseline HL lúc phát lệnh — dùng 4H trước, thiếu thì 1H
                hi0 = t.get("hl0_4h_hi") or t.get("hl0_1h_hi") or None
                lo0 = t.get("hl0_4h_lo") or t.get("hl0_1h_lo") or None
                eps = max(1e-8, (price_now or 0) * 1e-6)
                # Cross theo intrabar nhưng phải vượt baseline để không đếm “quá khứ trong cùng nến”
                def crossed(side, level):
                    lvl = float(level)
                    if side == "LONG":
                        cond = (hi is not None) and (hi >= lvl)
                        if hi0 is not None:
                            cond = cond and (hi > float(hi0) + eps)
                        return cond
                    if side == "SHORT":
                        cond = (lo is not None) and (lo <= lvl)
                        if lo0 is not None:
                            cond = cond and (lo < float(lo0) - eps)
                        return cond
                    return False
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

                # --- CLOSE SỚM KHI ĐẢO CHIỀU (chưa đạt TP nào) ---
                has_tp_hit = bool(hits.get("TP1") or hits.get("TP2") or hits.get("TP3"))
                if t.get("status") == "OPEN" and not has_tp_hit:
                    # Dùng 4H làm chính, 1H xác nhận
                    df_4h = dfs.get("4H")
                    df_1h = dfs.get("1H")
                    is_rev, why = _reversal_signal(side, df_4h, df_1h)
                    # --- Anti-whipsaw buffers ---
                    ok_buffers = True
                    try:
                        # 1) lockout thời gian sau khi post: ≥90 phút hoặc ≥2 nến 1H đóng
                        posted_at = int(t.get("posted_at") or 0)
                        mins_since = (int(time.time()) - posted_at) / 60.0 if posted_at else 999
                        bars1h = len(df_1h) if df_1h is not None else 0
                        lock_time_ok = mins_since >= 90 or bars1h >= 2
                        # 2) adverse move vs entry phải ≥ 0.35*ATR(4H)
                        atr4 = float(df_4h["atr14"].iloc[-2]) if (df_4h is not None and "atr14" in df_4h.columns and len(df_4h)>=2) else 0.0
                        adverse = abs(price_now - entry)
                        adverse_ok = (atr4 > 0) and (adverse >= 0.35 * atr4)
                        ok_buffers = bool(lock_time_ok and adverse_ok)
                    except Exception:
                        ok_buffers = False
                    if is_rev and ok_buffers:
                        # --- TÍNH PCT & R TẠI THỜI ĐIỂM CLOSE ---
                        # dùng giá 'price_now' đã lấy ở trên (HL/close của 4H hoặc 1H)
                        try:
                            close_px = float(price_now)
                        except Exception:
                            close_px = float(t.get("entry") or 0.0)
                        try:
                            entry = float(t.get("entry") or 0.0)
                        except Exception:
                            entry = 0.0
                        try:
                            sl = float(t.get("sl") or 0.0)
                        except Exception:
                            sl = 0.0
                        def _pct(entry_px: float, px: float, _side: str) -> float:
                            if not entry_px or not px:
                                return 0.0
                            return ((px - entry_px) / entry_px * 100.0) if _side == "LONG" else ((entry_px - px) / entry_px * 100.0)
                        def _risk_pct(entry_px: float, sl_px: float, _side: str) -> float:
                            if not entry_px or not sl_px:
                                return 0.0
                            return ((entry_px - sl_px) / entry_px * 100.0) if _side == "LONG" else ((sl_px - entry_px) / entry_px * 100.0)
                        close_pct = float(_pct(entry, close_px, side))
                        risk_pct  = float(_risk_pct(entry, sl, side))
                        R = float(close_pct / risk_pct) if risk_pct > 0 else 0.0
                        R_weighted = 0.2 * R   # theo convention scale-out 20%

                        # ĐÓNG & LƯU SỐ LIỆU ĐỂ KPI ĐỌC
                        perf.close(t["sid"], "REVERSAL")   # map -> status="CLOSE"
                        t["status"] = "CLOSE"
                        perf.update_fields(
                            t["sid"],
                            close_px=close_px,
                            close_pct=close_pct,
                            realized_R=R_weighted  # đã weighted 20%
                        )
                        note = f"📌 Đóng lệnh sớm do có tín hiệu đảo chiều."
                        extra = {"margin_pct": close_pct}
                        if tn2:
                            if msg_id:
                                tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                            else:
                                tn2.send_channel(render_update(t, note, extra))

                # TP1
                if t.get("status")=="OPEN" and not hits.get("TP1") and t.get("tp1") and crossed(side, t["tp1"]):
                    perf.set_hit(t["sid"], "TP1", (t.get("r_ladder",{}) or {}).get("tp1") or 0.0)
                    hits["TP1"] = int(__import__("time").time())
                    t["status"] = "TP1"
                    note = "🎯 TP1 hit — Nâng SL lên Entry để bảo toàn lợi nhuận."
                    t["sl_dyn"] = float(entry)  # BE
                    perf.update_fields(t["sid"], sl_dyn=float(entry))
                    extra = {"margin_pct": margin_pct(float(t["tp1"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))
                        
                # TP2
                if t.get("status") in ("OPEN","TP1") and not hits.get("TP2") and t.get("tp2") and crossed(side, t["tp2"]):
                    perf.set_hit(t["sid"], "TP2", (t.get("r_ladder",{}) or {}).get("tp2") or 0.0)
                    hits["TP2"] = int(__import__("time").time())
                    t["status"] = "TP2"
                    note = "🎯 TP2 hit — Khóa SL về TP1."
                    t["sl_dyn"] = float(t.get("tp1") or entry)
                    perf.update_fields(t["sid"], sl_dyn=float(t.get("tp1") or entry))
                    extra = {"margin_pct": margin_pct(float(t["tp2"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

                # TP3 (fix): KHÔNG đóng lệnh tại TP3; chỉ đánh dấu hit và dời SL động về TP2
                if t.get("status") in ("OPEN","TP1","TP2") and not hits.get("TP3") and t.get("tp3") and crossed(side, t["tp3"]):
                    perf.set_hit(t["sid"], "TP3", (t.get("r_ladder",{}) or {}).get("tp3") or 0.0)
                    hits["TP3"] = int(__import__("time").time())
                    t["status"] = "TP3"
                    note = "🎯 TP3 hit — Khóa SL về TP2."
                    t["sl_dyn"] = float(t.get("tp2") or entry)
                    perf.update_fields(t["sid"], sl_dyn=float(t.get("tp2") or entry))
                    extra = {"margin_pct": margin_pct(float(t["tp3"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

                 # TP4
                if t.get("status") in ("OPEN","TP1","TP2","TP3") and not hits.get("TP4") and t.get("tp4") and crossed(side, t["tp4"]):
                    perf.set_hit(t["sid"], "TP4", (t.get("r_ladder",{}) or {}).get("tp4") or 0.0)
                    hits["TP4"] = int(__import__("time").time())
                    t["status"] = "TP4"
                    note = "🎯 TP4 hit — Khóa SL về TP3."
                    t["sl_dyn"] = float(t.get("tp3") or entry)
                    perf.update_fields(t["sid"], sl_dyn=float(t.get("tp3") or entry))
                    extra = {"margin_pct": margin_pct(float(t["tp4"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))
                          
                # TP5 (đóng lệnh)
                if t.get("status") in ("OPEN","TP1","TP2","TP3","TP4") and not hits.get("TP5") and t.get("tp5") and crossed(side, t["tp5"]):
                    perf.set_hit(t["sid"], "TP5", (t.get("r_ladder",{}) or {}).get("tp5") or 0.0)
                    hits["TP5"] = int(__import__("time").time())
                    perf.close(t["sid"], "TP5")
                    t["status"] = "CLOSE"
                    note = "✨ TP5 hit — Đóng lệnh."
                    extra = {"margin_pct": margin_pct(float(t["tp5"]))}
                    if tn2:
                        if msg_id:
                            tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                        else:
                            tn2.send_channel(render_update(t, note, extra))

                # NEW: Nếu đã đạt ≥TP1 và giá quay ngược về SL động -> ĐÓNG LỆNH,
                # và hiển thị lợi nhuận theo TP cao nhất đã đạt.
                sl_dyn = t.get("sl_dyn")
                if t.get("status") in ("TP1", "TP2", "TP3", "TP4") and sl_dyn is not None:
                    try:
                        _sld = float(sl_dyn)
                    except Exception:
                        _sld = None
                
                    retraced = (
                        _sld is not None and (
                            (side == "LONG" and price_now <= _sld) or
                            (side == "SHORT" and price_now >= _sld)
                        )
                    )
                
                    if retraced:
                        # chọn TP cao nhất đã đạt để hiển thị
                        highest = None
                        hit_price = None
                        if hits.get("TP5"):
                            highest, hit_price = "TP5", float(t.get("tp5") or t.get("tp4") or t.get("tp3") or t.get("tp2") or t.get("tp1") or entry)
                        elif hits.get("TP4"):
                            highest, hit_price = "TP4", float(t.get("tp4") or t.get("tp3") or t.get("tp2") or t.get("tp1") or entry)
                        elif hits.get("TP3"):
                            highest, hit_price = "TP3", float(t.get("tp3") or t.get("tp2") or t.get("tp1") or entry)
                        elif hits.get("TP2") or t.get("status") == "TP2":
                            highest, hit_price = "TP2", float(t.get("tp2") or t.get("tp1") or entry)
                        else:
                            highest, hit_price = "TP1", float(t.get("tp1") or entry)
                
                        perf.close(t["sid"], "TRAIL")   # khác biệt: đóng theo SL động
                        t["status"] = "CLOSE"
                        note = f"📌 Đóng lệnh — Giá quay về SL động sau khi đã đạt {highest}."
                        extra = {"margin_pct": margin_pct(hit_price)}
                        if tn2:
                            if msg_id:
                                tn2.send_channel_update(int(msg_id), render_update(t, note, extra))
                            else:
                                tn2.send_channel(render_update(t, note, extra))


                # --- SL => đóng lệnh (fallback, nếu chưa CLOSE bởi reversal)
                slv = t.get("sl")
                has_tp_hit = bool(hits.get("TP1") or hits.get("TP2") or hits.get("TP3"))
                if t.get("status") == "OPEN" and not has_tp_hit and slv:
                    if side == "LONG":
                        hit_sl = (lo is not None and lo <= slv)
                        if lo0 is not None:
                            hit_sl = hit_sl and (lo < float(lo0) - eps)
                    else:  # SHORT
                        hit_sl = (hi is not None and hi >= slv)
                        if hi0 is not None:
                            hit_sl = hit_sl and (hi > float(hi0) + eps)
                    if hit_sl:
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
    # --- Early Flip Guard: evaluate BTC/ETH and toggle side cooldowns ---
    market_state = run_market_guards(shared_ex)

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
    last_kpi_day = None   # NEW: KPI ngày 1 lần
    last_kpi_week = None  # NEW: KPI tuần 1 lần/tuần
    last_status57_key = None   # NEW: chống gửi trùng báo cáo :57
    while True:
        now = datetime.now(TZ)
        blk = which_block_for_minute(now.minute)
        # Include half-hour slot so each block can run twice per hour
        half = 0 if now.minute < 30 else 1
        tick_key = (now.year, now.month, now.day, now.hour, half, blk)
        if blk is not None and tick_key != last_tick and now.second < 10:
            last_tick = tick_key
            run_block(blk, blocks[blk], cfg, limit, len(blocks), ex=shared_ex)
        # NEW: Lên lịch gửi báo cáo lệnh mở vào 08:57 & 20:57 (giờ VN)
        try:
            # Vòng lặp tick mỗi 5 phút, nên đặt lịch ở :55 rồi Timer 120s để bắn đúng :57
            if now.minute == 55 and now.hour in (8,) and now.second < 10:
                key = (now.year, now.month, now.day, now.hour)
                if globals().get("_last_status57_timer_key") != key:
                    globals()["_last_status57_timer_key"] = key
                    delay = max(0, 120 - now.second)  # tới :57:00
                    when = now + timedelta(seconds=delay)
                    def _job(ts_when=when):
                        _send_open_status(ts_when)
                    threading.Timer(delay, _job).start()
                    log.info(f"Scheduled open-status report for {when.strftime('%d/%m %H:%M')}")
        except Exception as e:
            log.warning("schedule 08:57/20:57 failed: %s", e)
          
        # NEW: KPI lúc 18:30 local (VN)
        try:
            if now.hour == 18 and now.minute == 30 and (last_kpi_day != (now.year, now.month, now.day)):
                last_kpi_day = (now.year, now.month, now.day)
                # Teaser KPI: list 24H (chỉ lệnh ĐÓNG-chưa-báo-cáo) + hiệu suất NGÀY
                perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR", "./data")))
                detail_24h, sids_to_mark = perf.kpis_24h_unreported()
                kpi_day = perf.kpis("day")           # sumR, wr, ...
                detail_day = perf.kpis_detail("day") # equity 1x + tp_counts
                report_date_str = now.strftime("%d/%m/%Y")
          
                tn = _get_notifier()
                fb = _get_fb_notifier()
                from templates import render_kpi_teaser_two_parts
                html = render_kpi_teaser_two_parts(detail_24h, kpi_day, detail_day, report_date_str)
                # Telegram (nếu bật)
                if tn:
                    tn.send_kpi24(html)
                # Fanpage (độc lập)
                if fb:
                    try:
                        fb.post_kpi_24h(html)
                    except Exception as e:
                        log.warning(f"KPI-24H fanpage failed: {e}")
                # Đánh dấu đã report (không phụ thuộc TN)
                try:
                    perf.mark_kpi24_reported(sids_to_mark)
                except Exception:
                    pass
            # NEW: KPI TUẦN — 08:16 sáng Thứ Bảy (Asia/Ho_Chi_Minh) — Telegram & Fanpage độc lập
            if now.weekday() == 5 and now.hour == 8 and now.minute == 16:
                wk_key = (now.isocalendar().year, now.isocalendar().week)
                if last_kpi_week != wk_key:
                    last_kpi_week = wk_key
                    # cửa sổ tuần: từ 00:00 thứ Bảy tuần trước đến thời điểm chạy hiện tại
                    today_00 = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    week_start = today_00 - timedelta(days=7)
                    start_ts = int(week_start.timestamp())
                    end_ts   = int(now.timestamp())
                    perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR", "./data")))
                    detail_week = perf.kpis_between(start_ts, end_ts)  # hàm đã có trong storage hoặc kpis("week") fallback
                    week_label = f"{week_start.strftime('%d/%m')}–{now.strftime('%d/%m')}"
                    from templates import render_kpi_week
                    html_w = render_kpi_week(detail_week, week_label)
                    tn = _get_notifier()
                    fb = _get_fb_notifier()
                    if tn:
                        try:
                            tn.send_kpi24(html_w)  # tái dùng sender (hoặc tạo send_kpi_week nếu bạn muốn tách)
                        except Exception as e:
                            log.warning(f"KPI-week telegram failed: {e}")
                    if fb:
                        try:
                            fb.post_kpi_week(html_w)
                        except Exception as e:
                            log.warning(f"KPI-week fanpage failed: {e}")
                    # label như ví dụ: 20-27/9/2025
                    def _ds(d):
                        dd = d.strftime("%d").lstrip("0")
                        mm = d.strftime("%m").lstrip("0")
                        yy = d.strftime("%Y")
                        return dd, mm, yy
                    d1, m1, y1 = _ds(week_start)
                    d2, m2, y2 = _ds(now)
                    week_label = f"{d1}-{d2}/{m2}/{y2}" if m1 == m2 and y1 == y2 else f"{d1}/{m1}-{d2}/{m2}/{y2}"
                    perf = SignalPerfDB(JsonStore(os.getenv("DATA_DIR", "./data")))
                    detail_week = perf.kpis_week_detail(start_ts, end_ts)
                    from templates import render_kpi_week
                    html = render_kpi_week(detail_week, week_label, risk_per_trade_usd=100.0)
                    tn = _get_notifier()
                    fb = _get_fb_notifier()
                    if tn:
                        tn.send_kpi24(html)
                    # post fanpage
                    try:
                        if fb:
                            fb.post_kpi_week(html)
                    except Exception as e:
                        log.warning(f"KPI-week fanpage failed: {e}")
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
