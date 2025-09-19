import os, json, time, threading
from typing import Optional, Dict, Any, List

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

    def open(self, sid: str, plan: dict, message_id: int | None = None) -> None:
        data = self._all()
        data[sid] = {
            "sid": sid,
            "symbol": plan.get("symbol"),
            "dir": plan.get("DIRECTION"),
            "entry": plan.get("entry"),
            "sl": plan.get("sl"),
            "tp1": plan.get("tp1") or plan.get("tp"),
            "tp2": plan.get("tp2"),
            "tp3": plan.get("tp3"),
            "posted_at": int(time.time()),
            "message_id": int(message_id) if message_id is not None else None,
            "status": "OPEN",
            "hits": {},
            "r_ladder": {
                "tp1": plan.get("rr1") or plan.get("rr"),
                "tp2": plan.get("rr2"),
                "tp3": plan.get("rr3"),
            },
            "realized_R": 0.0,
            "close_reason": None,
            # NEW: đánh dấu đã được tính trong báo cáo KPI 24h hay chưa
            "kpi24_reported_at": None,
        }
        self._write(data)

    def cooldown_active(self, symbol: str, seconds: int = 4*3600) -> bool:
        """Có lệnh đang mở/TP1/TP2 trong <seconds> gần nhất không?"""
        now = int(time.time())
        for t in self._all().values():
            if t.get("symbol") != symbol: 
                continue
            if t.get("status") in ("OPEN", "TP1", "TP2") and now - int(t.get("posted_at", 0)) < seconds:
                return True
        return False
        
    def by_symbol(self, symbol: str) -> list:
        return [
            t for t in self._all().values()
            if t.get("symbol") == symbol and t.get("status") in ("OPEN", "TP1", "TP2")
        ]

    def set_hit(self, sid: str, level: str, R: float) -> dict:
        data = self._all()
        t = data.get(sid, {})
        if not t:
            return {}
        t["hits"][level] = int(time.time())
        t["status"] = level.upper()
        t["realized_R"] = float(t.get("realized_R", 0.0) + (R or 0.0))
        data[sid] = t
        self._write(data)
        return t

    def close(self, sid: str, reason: str) -> dict:
        data = self._all()
        t = data.get(sid, {})
        if not t:
            return {}
        # Map reason → status
        # TP3 => chốt lời cuối; ENTRY/REVERSAL => đóng trung tính; còn lại => SL
        r = (reason or "").upper()
        if r == "TP3":
            t["status"] = "TP3"
        elif r in ("ENTRY", "REVERSAL"):
            t["status"] = "CLOSE"
        else:
            t["status"] = "SL"
        t["close_reason"] = r
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
        tp_counts = {"TP1": 0, "TP2": 0, "TP3": 0, "SL": 0}
        for t in self._all().values():
            status = (t.get("status") or "OPEN").upper()
            hits = (t.get("hits") or {})

            # Chỉ nhận các lệnh đã ĐÓNG: TP3 / SL / CLOSE (CLOSE phải có TP trong hits)
            if status not in ("TP3","SL","CLOSE"):
                continue
            if status == "CLOSE" and not any(k in hits for k in ("TP1","TP2","TP3")):
                continue

            win = False
            price_hit = None
            show_status = status

            if status == "TP3" or ("TP3" in hits):
                price_hit = t.get("tp3"); win = True; show_status = "TP3"; tp_counts["TP3"] += 1
            elif ("TP2" in hits):
                price_hit = t.get("tp2"); win = True; show_status = "TP2"; tp_counts["TP2"] += 1
            elif ("TP1" in hits):
                price_hit = t.get("tp1"); win = True; show_status = "TP1"; tp_counts["TP1"] += 1
            else:
                price_hit = t.get("sl"); win = False; show_status = "SL"; tp_counts["SL"] += 1

            pct = _pct_for_hit(t, price_hit)
            R = float(t.get("realized_R", 0.0) or 0.0)
            if R == 0.0:
                R = _r_estimate(t, show_status)
            items.append({
                "symbol": (t.get("symbol") or "").upper(),
                "status": show_status,
                "pct": float(pct),
                "win": bool(win),
                "R": float(R),
            })

        n = len(items)
        wins = sum(1 for i in items if i["win"])
        losses = sum(1 for i in items if i["status"] == "SL")
        sum_pct = sum(float(i["pct"]) for i in items)
        avg_pct = (sum_pct / n) if n else 0.0
        win_rate = (wins / n) if n else 0.0
        sum_R = sum(float(i.get("R") or 0.0) for i in items)
        avg_R = (sum_R / n) if n else 0.0

        return {
            "items": items,
            "totals": {
                "n": n,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "sum_pct": sum_pct,
                "avg_pct": avg_pct,
                "sum_R": sum_R,
                "avg_R": avg_R,
                "tp_counts": tp_counts
            }
        }
    # === KPI 24h: chỉ lấy các lệnh ĐÓNG nhưng CHƯA từng được báo cáo ===
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
            if status == "TP3": return float(rl.get("tp3") or rl.get("TP3") or 3.0)
            if status == "TP2": return float(rl.get("tp2") or rl.get("TP2") or 2.0)
            if status == "TP1": return float(rl.get("tp1") or rl.get("TP1") or 1.0)
            if status == "SL":  return -1.0
            return 0.0
        items, sids_to_mark = [], []
        tp_counts = {"TP1": 0, "TP2": 0, "TP3": 0, "SL": 0}
        for t in self._all().values():
            status = (t.get("status") or "OPEN").upper()
            hits = (t.get("hits") or {})
            if status not in ("TP3", "SL", "CLOSE"):
                continue
            if status == "CLOSE" and not any(k in hits for k in ("TP1", "TP2", "TP3")):
                continue
            if t.get("kpi24_reported_at"):
                continue
            win = False; price_hit = None; show_status = status
            if status == "TP3" or ("TP3" in hits):
                price_hit = t.get("tp3"); win = True; show_status = "TP3"; tp_counts["TP3"] += 1
            elif "TP2" in hits:
                price_hit = t.get("tp2"); win = True; show_status = "TP2"; tp_counts["TP2"] += 1
            elif "TP1" in hits:
                price_hit = t.get("tp1"); win = True; show_status = "TP1"; tp_counts["TP1"] += 1
            else:
                price_hit = t.get("sl");  win = False; show_status = "SL";  tp_counts["SL"]  += 1
            pct = _pct_for_hit(t, price_hit)
            R = float(t.get("realized_R", 0.0) or 0.0)
            if R == 0.0:
                R = _r_estimate(t, show_status)
            items.append({
                "sid": (t.get("sid") or ""),
                "symbol": (t.get("symbol") or "").upper(),
                "status": show_status,
                "pct": float(pct),
                "win": bool(win),
                "R": float(R),
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
        detail = {
            "items": items,
            "totals": {
                "n": n,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "sum_pct": sum_pct,
                "avg_pct": avg_pct,
                "sum_R": sum_R,
                "avg_R": avg_R,
                "tp_counts": tp_counts
            }
        }
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

        def _pct(t, price_hit):
            try:
                e = float(t.get("entry") or 0.0)
                if not e: return 0.0
                if (t.get("dir") or "").upper() == "LONG":
                    return (float(price_hit) - e) / e * 100.0
                else:
                    return (e - float(price_hit)) / e * 100.0
            except Exception:
                return 0.0

        items = []
        for t in self._all().values():
            if int(t.get("posted_at", 0)) < start_ts:
                continue
            status = (t.get("status") or "OPEN").upper()
            price_hit = None
            win = False
            hits = (t.get("hits") or {})
            # Chỉ tính các lệnh đã hit SL hoặc TP (TP3 > TP2 > TP1).
            if not (status in ("SL", "TP1", "TP2", "TP3")
                    or ("TP1" in hits) or ("TP2" in hits) or ("TP3" in hits)):
                continue
            if status == "TP3" or ("TP3" in hits):
                price_hit = t.get("tp3"); win = True; status = "TP3"
            elif status == "TP2" or ("TP2" in hits):
                price_hit = t.get("tp2"); win = True; status = "TP2"
            elif status == "TP1" or ("TP1" in hits):
                price_hit = t.get("tp1"); win = True; status = "TP1"
            else:
                price_hit = t.get("sl");  win = False; status = "SL"
            items.append({
                "symbol": (t.get("symbol") or "").upper(),
                "status": status,
                "pct": _pct(t, price_hit),
                "win": bool(win),
            })

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
        # TP counts
        tp_counts = {"TP3": 0, "TP2": 0, "TP1": 0, "SL": 0}
        for i in items:
            s = i["status"]
            if s in tp_counts: tp_counts[s] += 1

        return {
            "items": items,
            "totals": {
                "n": n, "wins": wins, "losses": losses,
                "win_rate": win_rate, "sum_pct": sum_pct, "avg_pct": avg_pct,
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
