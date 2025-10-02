import os, json, time, threading
from typing import Optional, Dict, Any, List

# ---- pct helper used by KPI calculations ------------------------------------
# Try to reuse the implementation from templates.py (keeps logic consistent),
# and fallback to a local equivalent if that import isn't available.
try:
    from templates import _pct_for_hit as _tmpl_pct_for_hit  # reuse if present
except Exception:
    _tmpl_pct_for_hit = None

def _pct_for_hit(t: dict, price_hit: float) -> float:
    """
    % thay đổi so với entry theo side (LONG dương khi giá tăng; SHORT dương khi giá giảm).
    Trả về đơn vị % (ví dụ 1.23 nghĩa là +1.23%).
    """
    # Use the templates implementation when import succeeded.
    if _tmpl_pct_for_hit is not None:
        try:
            return float(_tmpl_pct_for_hit(t, price_hit))
        except Exception:
            pass
    # Fallback local logic (mirrors templates._pct_for_hit)
    try:
        e = float(t.get("entry") or 0.0)
        if not e or not price_hit:
            return 0.0
        pct = (float(price_hit) - e) / e * 100.0
        side = (t.get("dir") or t.get("DIRECTION") or "").upper()
        if side == "SHORT":
            pct = -pct
        return float(pct)
    except Exception:
        return 0.0

# -------------------------------
# Json store (atomic-ish writes)
# -------------------------------
class JsonStore:
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self._locks: Dict[str, threading.Lock] = {}

    def _path(self, name: str) -> str:
        return os.path.join(self.data_dir, name + ".json")

    def _lock(self, name: str) -> threading.Lock:
        if name not in self._locks:
            self._locks[name] = threading.Lock()
        return self._locks[name]

    def read(self, name: str) -> dict:
        path = self._path(name)
        if not os.path.exists(path):
            return {}
        with self._lock(name):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    return json.load(f)
                except Exception:
                    return {}

    def write(self, name: str, data: dict) -> None:
        path = self._path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with self._lock(name):
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)


# --------------------------------
# Signal performance (R-based)
# --------------------------------
class SignalPerfDB:
    def __init__(self, store: JsonStore):
        self.store = store

    def _all(self) -> dict:
        return self.store.read("trades")

    def _write(self, data: dict) -> None:
        self.store.write("trades", data)

    def open(self, sid: str, plan: dict, message_id: int | None = None, **extra) -> None:
        data = self._all()
        # Cho phép override posted_at và bổ sung các trường baseline (hl0_*, v.v.)
        _now = int(time.time())
        posted_at = int(extra.pop("posted_at", _now) or _now)
        rec = {
            "sid": sid,
            "symbol": plan.get("symbol"),
            "dir": plan.get("DIRECTION"),
            "entry": plan.get("entry"),
            "sl": plan.get("sl"),
            "risk_size_hint": plan.get("risk_size_hint") or plan.get("leverage") or plan.get("lev"),
            "tp1": plan.get("tp1") or plan.get("tp"),
            "tp2": plan.get("tp2"),
            "tp3": plan.get("tp3"),
            "tp4": plan.get("tp4"),
            "tp5": plan.get("tp5"),
            "posted_at": posted_at,
            "message_id": int(message_id) if message_id is not None else None,
            "status": "OPEN",
            "hits": {},
            "r_ladder": {"tp1": plan.get("rr1"), "tp2": plan.get("rr2"), "tp3": plan.get("rr3"), "tp4": plan.get("rr4"), "tp5": plan.get("rr5")},
            "weights": {"tp1": 0.2, "tp2": 0.2, "tp3": 0.2, "tp4": 0.2, "tp5": 0.2},
            "realized_R": 0.0,
            "close_reason": None,
            # NEW: đánh dấu đã được tính trong báo cáo KPI 24h hay chưa
            "kpi24_reported_at": None,
        }
        # Merge các field bổ sung (vd: hl0_4h_hi/lo, hl0_1h_hi/lo, close_px, close_pct…)
        try:
            for k, v in (extra or {}).items():
                rec[k] = v
        except Exception:
            pass
        data[sid] = rec
        self._write(data)

    def cooldown_active(self, symbol: str, seconds: int = 4*3600) -> bool:
        """
        Có lệnh đang sống (OPEN hoặc TP1..TP5) trong <seconds> gần nhất không?
        Nếu đã SL hoặc CLOSE/REVERSAL thì cho phép re-entry.
        """
        def _norm(s: str) -> str:
            return "".join(ch for ch in (s or "").upper() if ch.isalnum())

        now = int(time.time())
        symN = _norm(symbol)
        for t in self._all().values():
            if _norm(t.get("symbol") or "") != symN:
                continue
            st = (t.get("status") or "").upper()
            if st in ("OPEN","TP1","TP2","TP3","TP4","TP5"):
                posted = int(t.get("posted_at") or 0)
                if posted and (now - posted) < seconds:
                    return True
        return False
        
    def by_symbol(self, symbol: str) -> list:
        # Theo dõi mọi lệnh chưa đóng (loại CLOSE/SL). Cho phép TP3/TP4 tiếp tục được track.
        return [
            t for t in self._all().values()
            if t.get("symbol") == symbol and (t.get("status") or "").upper() not in ("CLOSE","SL")
        ]

    # NEW: Liệt kê tất cả lệnh đang mở (OPEN/TP1..TP5) cho báo cáo định kỳ
    def list_open_status(self) -> list[dict]:
        """Trả về list item dạng {sid, symbol, status} cho mọi lệnh chưa đóng."""
        out = []
        for t in self._all().values():
            st = (t.get("status") or "OPEN").upper()
            if st in ("CLOSE", "SL"):
                continue
            sym = (t.get("symbol") or "").replace("/", "").upper()
            out.append({"sid": t.get("sid"), "symbol": sym, "status": st})
        out.sort(key=lambda x: (x["symbol"], x["status"]))
        return out

    def update_fields(self, sid: str, **fields) -> dict:
        """Cập nhật một số field tuỳ ý (vd: sl_dyn) và ghi lại."""
        data = self._all()
        t = data.get(sid, {})
        if not t:
            return {}
        t.update({k: v for k, v in fields.items()})
        data[sid] = t
        self._write(data)
        return t

    def set_hit(self, sid: str, level: str, R: float) -> dict:
        data = self._all()
        t = data.get(sid, {})
        if not t:
            return {}
        t["hits"][level] = int(time.time())
        t["status"] = level.upper()
        w = float((t.get("weights") or {}).get(level.lower(), 0.2))
        t["realized_R"] = float(t.get("realized_R", 0.0) + w * (R or 0.0))
        data[sid] = t
        self._write(data)
        return t

    # === KPI tuần: các lệnh ĐÓNG trong [start_ts, end_ts) ===
    def kpis_week_detail(self, start_ts: int, end_ts: int) -> dict:
        def _pct_for_status(t: dict, status: str) -> float:
            try:
                e = float(t.get("entry") or 0.0)
                if not e: return 0.0
                side = (t.get("dir") or "").upper()
                def px(name): 
                    v = t.get(name)
                    return float(v) if v is not None else None
                # Ưu tiên mốc TP cao nhất đã có giá (fallback chuỗi xuống mốc dưới)
                ladder = ["tp5","tp4","tp3","tp2","tp1"]
                if status.startswith("TP"):
                    idx = int(status[-1])
                    pick = ladder[5-idx:]  # ví dụ TP3 -> ["tp3","tp2","tp1"]
                    hit_p = None
                    for nm in pick:
                        val = px(nm)
                        if val is not None:
                            hit_p = val; break
                    if hit_p is None: hit_p = px("tp1") or e
                elif status == "SL":
                    hit_p = px("sl") or e
                else:
                    # CLOSE theo trail: chọn TP cao nhất đã từng hit
                    hits = t.get("hits") or {}
                    if hits.get("TP5"): hit_p = px("tp5")
                    elif hits.get("TP4"): hit_p = px("tp4")
                    elif hits.get("TP3"): hit_p = px("tp3")
                    elif hits.get("TP2"): hit_p = px("tp2")
                    elif hits.get("TP1"): hit_p = px("tp1")
                    else: hit_p = e
                if hit_p is None: hit_p = e
                if side == "LONG":
                    return (hit_p - e) / e * 100.0
                return (e - hit_p) / e * 100.0
            except Exception:
                return 0.0

        items = []
        tp_counts = {"TP1":0,"TP2":0,"TP3":0,"TP4":0,"TP5":0,"SL":0}
        sum_pct = 0.0
        sum_R   = 0.0
        wins = 0; losses = 0
        for t in self._all().values():
            st = (t.get("status") or "OPEN").upper()
            closed_at = int(t.get("closed_at") or 0)
            if st not in ("TP5","TP4","TP3","SL","CLOSE"): 
                continue
            if not (start_ts <= closed_at < end_ts):
                continue
            # xác định nhãn kết quả để đếm TP/SL
            label = "SL" if st == "SL" else (
                "TP5" if (t.get("hits") or {}).get("TP5") else
                "TP4" if (t.get("hits") or {}).get("TP4") else
                "TP3" if (t.get("hits") or {}).get("TP3") else
                "TP2" if (t.get("hits") or {}).get("TP2") else
                "TP1" if (t.get("hits") or {}).get("TP1") else "CLOSE"
            )
            if label in tp_counts: tp_counts[label] += 1
            # % lợi nhuận trước đòn bẩy
            pct = _pct_for_status(t, label if label in ("TP1","TP2","TP3","TP4","TP5","SL") else st)
            sum_pct += pct
            # R weighted (đã cộng dồn trong realized_R)
            r_w = float(t.get("realized_R") or 0.0)
            sum_R += r_w
            # CLOSE chưa có TP (đảo chiều/entry) không tính win
            has_tp = any((t.get("hits") or {}).get(k) for k in ("TP1","TP2","TP3","TP4","TP5"))
            win = (label != "SL") and not (label == "CLOSE" and not has_tp)
            wins += int(win); losses += int(not win)
            items.append({
                "sid": t.get("sid"),
                "symbol": t.get("symbol"),
                "status": label,
                "pct": pct,
                # Chuẩn hóa: R đã scale-out 20% để template nhân leverage
                "R_weighted": r_w,
                "R": r_w,  # giữ tương thích ngược
                # Đòn bẩy tư vấn per-signal để template đọc
                "risk_size_hint": t.get("risk_size_hint"),
            })
        n = len(items)
        win_rate = (wins / n) if n else 0.0
        avg_pct = (sum_pct / n) if n else 0.0
        avg_R   = (sum_R / n) if n else 0.0
        totals = {
            "n": n, "wins": wins, "losses": losses, "win_rate": win_rate,
            "sum_pct": sum_pct, "avg_pct": avg_pct,
            "sum_R": sum_R, "avg_R": avg_R,
            "sum_R_weighted": sum_R, "sum_R_w": sum_R,
            "sum_pct_weighted": sum_pct, "sum_pct_w": sum_pct,
            "tp_counts": tp_counts,
        }
        return {"items": items, "totals": totals}

    def close(self, sid: str, reason: str) -> dict:
        data = self._all()
        t = data.get(sid, {})
        if not t:
            return {}
        # Map reason → status
        # TP5/TP4/TP3 => chốt lời; ENTRY/REVERSAL => đóng trung tính; còn lại => SL
        r = (reason or "").upper()
        if r == "TP5":
            t["status"] = "TP5"
        elif r == "TP4":
            t["status"] = "TP4"
        elif r == "TP3":
            t["status"] = "TP3"
        elif r in ("ENTRY", "REVERSAL"):
            t["status"] = "CLOSE"
        else:
            t["status"] = "SL"
        t["close_reason"] = r
        t["closed_at"] = int(time.time())
        data[sid] = t
        self._write(data)
        return t

    # NEW: count trades posted within a [start_ts, end_ts) range
    def count_released_between(self, start_ts: int, end_ts: int) -> int:
        """
        Đếm số tín hiệu đã đăng (open) trong khoảng [start_ts, end_ts).
        Dùng để giới hạn số lượng tín hiệu được release theo time-window.
        """
        n = 0
        for t in self._all().values():
            ts = int(t.get("posted_at", 0))
            if start_ts <= ts < end_ts:
                n += 1
        return n

    def kpis(self, period: str = "day") -> dict:
        """
        Trả về KPI PnL đơn giản trong khoảng thời gian:
        - period='day': từ 00:00 hôm nay
        - period='week': 7 ngày gần nhất (rolling)
        Tính dựa trên trades.posted_at và realized_R hiện tại.
        """
        trades = list(self._all().values())
        now = int(time.time())

        if period == "day":
            lt = time.localtime(now)
            start_ts = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))
        elif period == "week":
            start_ts = now - 7 * 24 * 3600
        else:
            # mặc định: 24h gần nhất
            start_ts = now - 24 * 3600

        sample = [t for t in trades if int(t.get("posted_at", 0)) >= start_ts]
        n = len(sample)
        sumR = sum(float(t.get("realized_R", 0.0)) for t in sample)
        wins = sum(1 for t in sample if float(t.get("realized_R", 0.0)) > 0)
        wr = (wins / n) if n else 0.0
        avgR = (sumR / n) if n else 0.0

        return {
            "period": period,
            "n": n,
            "wr": wr,
            "avgR": avgR,
            "sumR": sumR,
            "from_ts": start_ts,
            "to_ts": now,
        }

    # NEW: breakdown 24H theo % dựa trên TP cao nhất/SL đã đạt
    def kpis_24h_detail(self) -> dict:
        """
        Trả về danh sách và KPI dựa trên **các lệnh đã ĐÓNG và CÓ TP/SL** tại thời điểm quét,
        KHÔNG phụ thuộc thời điểm mở lệnh (posted_at).
        - items: list[{symbol, status, pct, win, R}]
        - totals: {n, wins, losses, win_rate, sum_pct, avg_pct, sum_R, avg_R, tp_counts}
        Quy ước:
          • Chỉ lấy status ∈ {"TP3","SL","CLOSE"}.
          • Với CLOSE: chỉ giữ nếu đã có ít nhất một TPx trong hits (close do retrace).
          • pct: tính theo mốc TP cao nhất đạt được (TP3 > TP2 > TP1) hoặc SL.
          • R: ưu tiên realized_R; nếu thiếu thì ước lượng theo r_ladder.
        """
        def _pct_for_hit(t: dict, price_hit) -> float:
            try:
                e = float(t.get("entry") or 0.0)
                if not e: return 0.0
                if (t.get("dir") or "").upper() == "LONG":
                    return (float(price_hit) - e) / e * 100.0
                else:
                    return (e - float(price_hit)) / e * 100.0
            except Exception:
                return 0.0

        def _r_estimate(t: dict, status: str) -> float:
            """Ước lượng R theo status; ưu tiên r_ladder nếu có, SL = -1.0"""
            rl = (t.get("r_ladder") or {})
            if status == "TP3":
                return float(rl.get("tp3") or rl.get("TP3") or 3.0)
            if status == "TP2":
                return float(rl.get("tp2") or rl.get("TP2") or 2.0)
            if status == "TP1":
                return float(rl.get("tp1") or rl.get("TP1") or 1.0)
            if status == "SL":
                return -1.0
            return 0.0

        items = []
        tp_counts = {"TP1": 0, "TP2": 0, "TP3": 0, "TP4": 0, "TP5": 0, "SL": 0}
        for t in self._all().values():
            status = (t.get("status") or "OPEN").upper()
            hits = (t.get("hits") or {})

            # Chỉ nhận các lệnh đã ĐÓNG: TP3 / SL / CLOSE
            if status not in ("TP1","TP2","TP3","TP4","TP5","SL","CLOSE"):
                continue
            # CLOSE sớm chưa có TP: ghi nhận với pct=0, R=0, win=False
            early_close_no_tp = (
                status == "CLOSE"
                and not any(k in hits for k in ("TP1","TP2","TP3","TP4","TP5"))
                and str(t.get("close_reason") or "").upper() in ("REVERSAL","ENTRY")
            )

            win = False
            price_hit = None
            show_status = status

            if early_close_no_tp:
                # Giữ show_status = "CLOSE", pct/R xử lý phía dưới
                win = False
            elif status == "TP3" or ("TP3" in hits):
                price_hit = t.get("tp3"); win = True; show_status = "TP3"; tp_counts["TP3"] += 1
            elif ("TP2" in hits):
                price_hit = t.get("tp2"); win = True; show_status = "TP2"; tp_counts["TP2"] += 1
            elif ("TP1" in hits):
                price_hit = t.get("tp1"); win = True; show_status = "TP1"; tp_counts["TP1"] += 1
            else:
                price_hit = t.get("sl"); win = False; show_status = "SL"; tp_counts["SL"] += 1

            if early_close_no_tp:
                # ưu tiên dùng số đã lưu tại thời điểm close
                pct = float(t.get("close_pct") or 0.0)
                R = float(t.get("realized_R") or 0.0)  # đã weighted 20% nếu theo patch main.py
                show_status = "CLOSE"; win = False
            else:
                pct = _pct_for_hit(t, price_hit)
                R = float(t.get("realized_R", 0.0) or 0.0)
            if R == 0.0:
                R = _r_estimate(t, show_status)
            items.append({
                "symbol": (t.get("symbol") or "").upper(),
                "status": show_status,
                "pct": float(pct),
                "win": bool(win),
                # Chuẩn hóa: R đã scale-out 20%
                "R_weighted": float(R),
                "R": float(R),  # giữ tương thích ngược
                # Đòn bẩy tư vấn per-signal
                "risk_size_hint": t.get("risk_size_hint"),
            })

        n = len(items)
        wins = sum(1 for i in items if i["win"])
        losses = sum(1 for i in items if i["status"] == "SL")
        sum_pct = sum(float(i["pct"]) for i in items)
        avg_pct = (sum_pct / n) if n else 0.0
        win_rate = (wins / n) if n else 0.0
        sum_R = sum(float(i.get("R") or 0.0) for i in items)
        avg_R = (sum_R / n) if n else 0.0

        totals = {
            "n": n,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "sum_pct": sum_pct,
            "avg_pct": avg_pct,
            "sum_R": sum_R,
            "avg_R": avg_R,
            "tp_counts": tp_counts,
        }
        # NEW: weighted aliases (sum_R đã là weighted vì realized_R đã nhân weight 20% mỗi TP)
        totals["sum_R_weighted"] = sum_R
        totals["sum_R_w"] = sum_R
        # Hiện chưa có % theo từng phần chốt → dùng alias = sum_pct để template tiêu thụ ổn định
        totals["sum_pct_weighted"] = sum_pct
        totals["sum_pct_w"] = sum_pct
        return {"items": items, "totals": totals}

    # === KPI 24h: các lệnh ĐÓNG trong 24h qua và CHƯA từng được báo cáo ===
    def kpis_24h_unreported(self) -> tuple[dict, list[str]]:
        def _pct_for_hit(t: dict, price_hit) -> float:
            try:
                e = float(t.get("entry") or 0.0)
                if not e:
                    return 0.0
                if (t.get("dir") or "").upper() == "LONG":
                    return (float(price_hit) - e) / e * 100.0
                return (e - float(price_hit)) / e * 100.0
            except Exception:
                return 0.0
        def _r_estimate(t: dict, status: str) -> float:
            rl = (t.get("r_ladder") or {})
            if status == "TP5": return float(rl.get("tp5") or rl.get("TP5") or 3.0)
            if status == "TP4": return float(rl.get("tp4") or rl.get("TP4") or 2.5)
            if status == "TP3": return float(rl.get("tp3") or rl.get("TP3") or 2.0)
            if status == "TP2": return float(rl.get("tp2") or rl.get("TP2") or 2.0)
            if status == "TP1": return float(rl.get("tp1") or rl.get("TP1") or 1.0)
            if status == "SL":  return -1.0
            return 0.0
        import time
        now = int(time.time())
        cutoff_ts = now - 24*3600
        items, sids_to_mark = [], []
        tp_counts = {"TP1": 0, "TP2": 0, "TP3": 0, "TP4": 0, "TP5": 0, "SL": 0}
        for t in self._all().values():
            status = (t.get("status") or "OPEN").upper()
            hits = (t.get("hits") or {})
            # Nhận các lệnh đã đóng: TP5/TP4/TP3/SL/CLOSE
            if status not in ("TP5", "TP4", "TP3", "SL", "CLOSE"):
                continue
            # Chỉ lấy lệnh đóng trong 24h qua
            closed_at = int(t.get("closed_at") or 0)
            if closed_at < cutoff_ts or closed_at > now:
                continue
            early_close_no_tp = (
                status == "CLOSE"
                and not any(k in hits for k in ("TP1","TP2","TP3","TP4","TP5"))
                and str(t.get("close_reason") or "").upper() in ("REVERSAL","ENTRY")
            )
            if t.get("kpi24_reported_at"):
                continue
            win = False; price_hit = None; show_status = status
            if early_close_no_tp:
                show_status = "CLOSE"; win = False
                pct = float(t.get("close_pct") or 0.0)
                R = float(t.get("realized_R") or 0.0)
            elif status == "TP5" or ("TP5" in hits):
                price_hit = t.get("tp5"); win = True; show_status = "TP5"; tp_counts["TP5"] += 1
            elif status == "TP4" or ("TP4" in hits):
                price_hit = t.get("tp4"); win = True; show_status = "TP4"; tp_counts["TP4"] += 1
            elif status == "TP3" or ("TP3" in hits):
                price_hit = t.get("tp3"); win = True; show_status = "TP3"; tp_counts["TP3"] += 1
            elif "TP2" in hits:
                price_hit = t.get("tp2"); win = True; show_status = "TP2"; tp_counts["TP2"] += 1
            elif "TP1" in hits:
                price_hit = t.get("tp1"); win = True; show_status = "TP1"; tp_counts["TP1"] += 1
            else:
                price_hit = t.get("sl");  win = False; show_status = "SL";  tp_counts["SL"]  += 1
            if not early_close_no_tp:
                pct = _pct_for_hit(t, price_hit)
                R = float(t.get("realized_R", 0.0) or 0.0)
                if R == 0.0:
                    R = _r_estimate(t, show_status)
            items.append({
                "sid": (t.get("sid") or ""),
                "symbol": (t.get("symbol") or "").upper(),
                "dir": (t.get("dir") or t.get("direction") or "").upper(),
                "status": show_status,
                "pct": float(pct),
                "win": bool(win),
                # Chuẩn hóa: R đã scale-out 20%
                "R_weighted": float(R),
                "R": float(R),  # giữ tương thích ngược
                # Đòn bẩy tư vấn per-signal
                "risk_size_hint": t.get("risk_size_hint"),
            })
            sids_to_mark.append(t.get("sid"))
        n = len(items)
        wins = sum(1 for i in items if i["win"])
        losses = sum(1 for i in items if i["status"] == "SL")
        sum_pct = sum(float(i["pct"]) for i in items)
        avg_pct = (sum_pct / n) if n else 0.0
        win_rate = (wins / n) if n else 0.0
        sum_R = sum(float(i.get("R") or 0.0) for i in items)
        avg_R = (sum_R / n) if n else 0.0
        totals = {
            "n": n,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "sum_pct": sum_pct,
            "avg_pct": avg_pct,
            "sum_R": sum_R,
            "avg_R": avg_R,
            "tp_counts": tp_counts,
        }
        # NEW: weighted aliases (sum_R đã là weighted vì realized_R đã nhân weight)
        totals["sum_R_weighted"] = sum_R
        totals["sum_R_w"] = sum_R
        # Alias % weighted (hiện dùng = sum_pct cho tương thích)
        totals["sum_pct_weighted"] = sum_pct
        totals["sum_pct_w"] = sum_pct
        detail = {"items": items, "totals": totals}
        return detail, sids_to_mark

    def mark_kpi24_reported(self, sids: list[str]) -> None:
        if not sids:
            return
        data = self._all()
        now = int(time.time())
        for sid in sids:
            t = data.get(sid)
            if not t:
                continue
            t["kpi24_reported_at"] = now
        self._write(data)

    # NEW: breakdown theo period (hiện dùng 'day' cho khối hiệu suất)
    def kpis_detail(self, period: str = "day") -> dict:
        """
        Trả về:
          - items: list[{symbol, status, pct, win(bool)}] cho các lệnh trong 'period'
          - totals: {n, wins, losses, win_rate, sum_pct, avg_pct, equity_change_pct, tp_counts}
        period:
          - 'day'   : từ 00:00 local hôm nay
          - '24h'   : 24 giờ gần nhất (nếu cần)
        """
        import time
        now = int(time.time())
        if period == "day":
            lt = time.localtime(now)
            start_ts = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))
        else:
            start_ts = now - 24*3600

        items = []
        for t in self._all().values():
            if int(t.get("posted_at", 0)) < start_ts:
                continue
            status0 = (t.get("status") or "OPEN").upper()
            hits = (t.get("hits") or {})

            show_status = None
            pct = 0.0
            R = 0.0
            win = False

            # Ưu tiên liệt kê: TP5..TP1, SL, và CLOSE (đóng sớm do REVERSAL/ENTRY)
            if status0 in ("TP5","TP4","TP3","TP2","TP1") or any(k in hits for k in ("TP5","TP4","TP3","TP2","TP1")):
                if status0 == "TP5" or ("TP5" in hits):
                    price_hit = t.get("tp5"); show_status = "TP5"; win = True
                elif status0 == "TP4" or ("TP4" in hits):
                    price_hit = t.get("tp4"); show_status = "TP4"; win = True
                elif status0 == "TP3" or ("TP3" in hits):
                    price_hit = t.get("tp3"); show_status = "TP3"; win = True
                elif status0 == "TP2" or ("TP2" in hits):
                    price_hit = t.get("tp2"); show_status = "TP2"; win = True
                else:
                    price_hit = t.get("tp1"); show_status = "TP1"; win = True
                pct = float(_pct_for_hit(t, price_hit))
                # R đã có convention scale-out 20% ở pipeline hiện hành
                R = float(t.get("realized_R", 0.0) or 0.0) or float(_r_estimate(t, show_status))
            elif status0 == "SL":
                price_hit = t.get("sl"); show_status = "SL"; win = False
                pct = float(_pct_for_hit(t, price_hit))
                R = float(t.get("realized_R", 0.0) or 0.0) or float(_r_estimate(t, show_status))
            elif status0 == "CLOSE":
                # Đóng sớm do REVERSAL/ENTRY – đưa vào danh sách 24H
                show_status = "CLOSE"
                pct = float(t.get("close_pct") or 0.0)
                R = float(t.get("realized_R") or 0.0)
                try:
                    win = (float(pct) > 0)
                except Exception:
                    win = False
            else:
                # OPEN/khác — bỏ qua
                continue

            item = {
                "sid": (t.get("sid") or ""),
                "symbol": (t.get("symbol") or "").upper(),
                "dir": (t.get("dir") or t.get("direction") or "").upper(),
                "status": show_status,
                "pct": float(pct),
                "win": bool(win),
                "R_weighted": float(R),
                "R": float(R),
                "risk_size_hint": t.get("risk_size_hint"),
            }
            items.append(item)

        # Totals (today)
        n = len(items)
        wins = sum(1 for i in items if i["win"])
        losses = sum(1 for i in items if i["status"] == "SL")
        sum_pct = sum(i["pct"] for i in items)
        avg_pct = (sum_pct / n) if n else 0.0
        win_rate = (wins / n) if n else 0.0
        # Geometric compounding 1x
        eq_mult = 1.0
        for i in items:
            eq_mult *= (1.0 + float(i["pct"])/100.0)
        equity_change_pct = (eq_mult - 1.0) * 100.0
        # TP counts (không gom CLOSE vào WR để giữ logic win-rate theo TP)
        tp_counts = {"TP5": 0, "TP4": 0, "TP3": 0, "TP2": 0, "TP1": 0, "SL": 0}
        for i in items:
            s = (i.get("status") or "").upper()
            if s in tp_counts:
                tp_counts[s] += 1

        return {
            "items": items,
            "totals": {
                "n": n,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "sum_pct": sum_pct,
                "avg_pct": avg_pct,
                "equity_change_pct": equity_change_pct,
                "tp_counts": tp_counts
            }
        }

# -------------------------------
# Users / subscriptions
# -------------------------------
class UserDB:
    def __init__(self, store: JsonStore):
        self.store = store

    def _now(self) -> int:
        return int(time.time())

    def list_all(self) -> dict:
        """Trả về dict {telegram_id_str: {...}}"""
        return self.store.read("users")

    def list_active(self) -> dict:
        now = self._now()
        users = self.store.read("users")
        return {uid: u for uid, u in users.items() if int(u.get("expires_at", 0)) > now}

    def get(self, telegram_id: int) -> dict:
        users = self.store.read("users")
        return users.get(str(telegram_id), {})

    def is_plus_active(self, telegram_id: int) -> bool:
        u = self.get(telegram_id)
        exp = int(u.get("expires_at", 0))
        return exp > self._now()

    def upsert(self, telegram_id: int, username: str | None = None, months: int = 1) -> dict:
        """Gia hạn theo tháng (mặc định 1 tháng)."""
        users = self.store.read("users")
        key = str(telegram_id)
        now = self._now()
        delta = months * 30 * 24 * 3600
        if key in users and int(users[key].get("expires_at", 0)) > now:
            users[key]["expires_at"] = int(users[key]["expires_at"]) + delta
        else:
            users[key] = users.get(key, {})
            users[key]["expires_at"] = now + delta
        if username:
            users[key]["username"] = username
        users[key]["plan"] = "plus"
        self.store.write("users", users)
        return users[key]

    def extend_days(self, telegram_id: int, days: int) -> dict:
        """Cộng trực tiếp số ngày."""
        users = self.store.read("users")
        key = str(telegram_id)
        now = self._now()
        delta = int(days) * 24 * 3600
        if key in users and int(users[key].get("expires_at", 0)) > now:
            users[key]["expires_at"] = int(users[key]["expires_at"]) + delta
        else:
            username = users.get(key, {}).get("username")
            users[key] = {"username": username, "created_at": now}
            users[key]["expires_at"] = now + delta
        users[key]["plan"] = "plus"
        self.store.write("users", users)
        return users[key]

    def revoke(self, telegram_id: int) -> None:
        """Thu hồi ngay (set expires_at = 0)."""
        users = self.store.read("users")
        key = str(telegram_id)
        if key in users:
            users[key]["expires_at"] = 0
            self.store.write("users", users)

    def set_expiry(self, telegram_id: int, ts: int) -> None:
        users = self.store.read("users")
        key = str(telegram_id)
        u = users.get(key, {})
        u["expires_at"] = int(ts)
        users[key] = u
        self.store.write("users", users)


# -------------------------------
# Payments (manual approve)
# -------------------------------
class PaymentDB:
    def __init__(self, store: JsonStore):
        self.store = store

    def add(
        self,
        telegram_id: int,
        amount: Optional[int],
        bank_ref: Optional[str],
        months: int = 1,
        approved: bool = False,
        admin_id: Optional[int] = None,
        order_id: Optional[str] = None,
    ) -> str:
        payments = self.store.read("payments")
        pid = f"{int(time.time())}-{telegram_id}"
        payments[pid] = {
            "telegram_id": telegram_id,
            "amount": amount,
            "bank_ref": bank_ref,
            "months": months,
            "approved": approved,
            "admin_id": admin_id,
            "order_id": order_id,
            "created_at": int(time.time()),
        }
        self.store.write("payments", payments)
        return pid

    def approve(self, payment_id: str, admin_id: int) -> None:
        payments = self.store.read("payments")
        if payment_id in payments:
            payments[payment_id]["approved"] = True
            payments[payment_id]["admin_id"] = admin_id
            self.store.write("payments", payments)


# -------------------------------
# Signal cache (teaser/full/plan)
# -------------------------------
class SignalCache:
    def __init__(self, store: JsonStore):
        self.store = store

    def put_full(self, signal_id: str, text: str) -> None:
        data = self.store.read("signals")
        data[signal_id] = {**data.get(signal_id, {}), "text": text, "ts": int(time.time())}
        self.store.write("signals", data)

    def get_full(self, signal_id: str) -> Optional[str]:
        data = self.store.read("signals")
        s = data.get(signal_id)
        return s.get("text") if s else None

    def put_plan(self, signal_id: str, plan: dict) -> None:
        data = self.store.read("signals")
        data[signal_id] = {**data.get(signal_id, {}), "plan": plan, "ts": int(time.time())}
        data["_latest_id"] = signal_id
        self.store.write("signals", data)

    def get_plan(self, signal_id: str) -> Optional[dict]:
        data = self.store.read("signals")
        s = data.get(signal_id)
        return s.get("plan") if s else None

    def get_latest_id(self) -> Optional[str]:
        data = self.store.read("signals")
        return data.get("_latest_id")
