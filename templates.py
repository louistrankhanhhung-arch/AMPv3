
from datetime import datetime, timezone
from typing import Dict, Any
import math, os

# Module exports (giúp static import/IDE & đảm bảo namespace đầy đủ)
__all__ = [
    "_side_of", "_entry_of", "_pct_for_hit",
    "_report_leverage", "_item_leverage", "fmt_price",
    "render_teaser", "render_full", "render_update",
    "render_summary", "render_kpi_teaser_two_parts", "render_kpi_week",
]

# ---------- helpers for KPI % calculation ----------
def _side_of(t: Dict[str, Any]) -> str:
    """
    Lấy side của lệnh theo các khóa phổ biến.
    """
    return str(t.get("side") or t.get("DIRECTION") or "").upper()

def _entry_of(t: Dict[str, Any]) -> float:
    """
    Lấy giá entry theo các khóa phổ biến. Thiếu thì trả 0 để tránh ZeroDivisionError.
    """
    for k in ("entry", "ENTRY", "entry_price", "price_entry"):
        try:
            v = float(t.get(k))
            if v and v > 0:
                return v
        except Exception:
            continue
    return 0.0

def _pct_for_hit(t: Dict[str, Any], price_hit: float) -> float:
    """
    % thay đổi so với entry theo side (LONG dương khi giá tăng; SHORT dương khi giá giảm).
    Trả về đơn vị % (ví dụ 1.23 nghĩa là +1.23%).
    An toàn với dữ liệu thiếu: nếu không đủ entry/price thì trả 0.0.
    """
    try:
        entry = _entry_of(t)
        if not entry or not price_hit:
            return 0.0
        pct = (float(price_hit) - float(entry)) / float(entry) * 100.0
        if _side_of(t) == "SHORT":
            pct = -pct
        return float(pct)
    except Exception:
        return 0.0


# --------- leverage helper for reports ----------
# (Compat aliases — để nơi khác gọi theo tên không dấu gạch dưới)
pct_for_hit = _pct_for_hit
entry_of = _entry_of
side_of = _side_of
# Defensive: đảm bảo helpers luôn có trong globals khi module được import
globals().setdefault("_pct_for_hit", _pct_for_hit)
globals().setdefault("_entry_of", _entry_of)
globals().setdefault("_side_of", _side_of)

def _report_leverage() -> float:
    """
    Hệ số đòn bẩy cho mục KPI 24H/tuần.
    Lấy từ ENV REPORT_LEVERAGE (vd: 3 cho x3). Mặc định 1.0 nếu không set/không hợp lệ.
    """
    try:
        lv = float(os.getenv("REPORT_LEVERAGE", "1"))
        return lv if lv > 0 else 1.0
    except Exception:
        return 1.0

# Lấy leverage tư vấn từ 1 item (signal/trade)
def _item_leverage(it: dict) -> float:
    for k in ("risk_size_hint", "leverage", "lev", "advice_leverage"):
        try:
            v = float(it.get(k)) if it and (k in it) else 0.0
            if v and v > 0:
                return v
        except Exception:
            continue
    return 0.0  # 0 nghĩa là không có dữ liệu

def fmt_price(v):
    """
    Auto-format cho CRYPTO:
    - Tự chọn số lẻ theo biên độ giá
    - Không dùng dấu phân tách nghìn (tránh nhầm dấu thập phân)
    """
    try:
        x = float(v)
    except Exception:
        return "-"
    ax = abs(x)
    if   ax >= 1000:  fmt = "{:,.2f}"
    elif ax >= 100:   fmt = "{:,.2f}"
    elif ax >= 10:    fmt = "{:,.2f}"
    elif ax >= 1:     fmt = "{:,.3f}"
    elif ax >= 0.1:   fmt = "{:,.4f}"
    elif ax >= 0.01:  fmt = "{:,.5f}"
    else:             fmt = "{:,.6f}"
    s = fmt.format(x).replace(",", "")     # bỏ dấu nghìn
    if "." in s:
        s = s.rstrip("0").rstrip(".")      # bỏ số 0 thừa cuối
    return s

def render_teaser(plan: Dict[str, Any]) -> str:
    sym = plan.get("symbol", "")
    direction = plan.get("DIRECTION", "LONG")
    state = plan.get("STATE", "")
    strategy = " • ".join([n for n in plan.get("notes", [])[:1]])  # one-liner
    return (
        f"🧭 <b>{sym} | {direction}</b>\n"
        f"<b>Entry:</b> —    <b>SL:</b> —\n"
        f"<b>TP:</b> — • — • — • — • —\n"
        f"<b>Scale-out:</b> 20% mỗi mốc TP\n"
        f"<b>Chiến lược:</b> T+"
    )

def render_full(plan: Dict[str, Any], username: str | None = None, watermark: bool = True) -> str:
    sym = plan.get("symbol", "")
    direction = plan.get("DIRECTION", "LONG")
    entry = fmt_price(plan.get("entry"))
    sl = fmt_price(plan.get("sl"))
    tp1 = fmt_price(plan.get("tp1")); tp2 = fmt_price(plan.get("tp2")); tp3 = fmt_price(plan.get("tp3")); tp4 = fmt_price(plan.get("tp4")); tp5 = fmt_price(plan.get("tp5"))
    # leverage (gợi ý)
    risk = plan.get("risk_size_hint")
    if isinstance(risk, (int, float)):
        risk_disp = math.floor(float(risk))
        don_bay_line = f"<b>Đòn bẩy:</b> x{int(risk_disp)}"
    else:
        don_bay_line = None
    lines = [
        f"🧭 <b>{sym} | {direction}</b>",
        "",  # dòng trống sau tiêu đề
        
        f"<b>Entry:</b> {entry}",
        f"<b>SL:</b> {sl}",
        "",  # dòng trống sau Entry/SL
        
        f"<b>TP1:</b> {tp1}",
        f"<b>TP2:</b> {tp2}",
        f"<b>TP3:</b> {tp3}",
        f"<b>TP4:</b> {tp4}",
        f"<b>TP5:</b> {tp5}",
        "",  # dòng trống sau Entry/SL
    ]
    if don_bay_line:
        lines.append(don_bay_line)

    if watermark and username:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines.append(f"— sent to @{username} • {ts}")
    return "\n".join(lines)

def render_update(plan_or_trade: dict, event: str, extra: dict|None=None) -> str:
    sym = plan_or_trade.get("symbol",""); d = plan_or_trade.get("DIRECTION","")
    m = extra.get("margin_pct") if extra else None
    tail = f"\n<b>Lợi nhuận:</b> {m:.2f}%" if isinstance(m,(int,float)) else ""
    return f"<b>{sym} | {d}</b>\n<b>Update:</b> {event}{tail}"

def render_summary(kpi: dict, scope: str="Daily") -> str:
    return (
      f"<b>PNL {scope}</b>\n"
      f"• Trades: {kpi['n']}, Win-rate: {kpi['wr']:.0%}\n"
      f"• Avg R: {kpi['avgR']:.2f}\n"
      f"• Total R: {kpi['sumR']:.2f}"
    )

# NEW: Teaser 2 phần — Header + danh sách 24H, rồi khối hiệu suất NGÀY (today)
def render_kpi_teaser_two_parts(detail_24h: dict,
                                kpi_day: dict,
                                detail_day: dict,
                                report_date_str: str,
                                upgrade_url: str | None = None) -> str:
    lines = [f"🧭 <b>Kết quả giao dịch 24H qua — {report_date_str}</b>", ""]
    items = detail_24h.get("items", []) or []
    if not items:
        lines += ["Không có tín hiệu nào phù hợp.", ""]
    else:
        # Danh sách lệnh đã đóng (tận dụng lại code/biến sẵn có)
        lines.append("<b>Danh sách lệnh đã đóng (24H):</b>")
        icons = {"TP1": "🟢", "TP2": "🟢", "TP3": "🟢", "TP4": "🟢", "TP5": "🟢", "SL": "⛔"}
        for it in items:
            status = str(it.get("status") or it.get("closed_reason") or "").upper()
            icon = icons.get(status, "⚪")
            # pct ưu tiên số đã weight; fallback số thô
            try:
                pct = float(it.get("pct_weighted") or it.get("pct") or 0.0)
            except Exception:
                pct = 0.0
            # R nếu có (ưu tiên R_weighted)
            try:
                Rv = float(it.get("R_weighted") or it.get("R") or 0.0)
            except Exception:
                Rv = 0.0
            sym  = it.get("symbol") or "?"
            diru = (it.get("dir") or it.get("direction") or "").upper()
            lev  = _item_leverage(it)
            lev_s = f" x{int(lev)}" if lev and lev > 0 else ""
            dir_part = f" {diru}" if diru else ""
            lines.append(f"{icon} {sym}{dir_part} • {status}: {pct:+.2f}% ({Rv:+.2f}R){lev_s}")
        lines.append("")

    totals = (detail_24h.get("totals") or {}) if isinstance(detail_24h, dict) else {}
    n = int(totals.get("n", 0) or 0)
    sumR_w = float(
        totals.get("sum_R_weighted") or  # đề xuất back-end: tổng R đã nhân weight
        totals.get("sum_R_w") or         # alias nếu bạn dùng tên khác
        totals.get("sum_R", 0.0) or 0.0  # fallback cũ (có thể chưa weighted)
    )
    sum_pct_w = float(
        totals.get("sum_pct_weighted") or
        totals.get("sum_pct_w") or
        totals.get("sum_pct", 0.0) or 0.0
    )

    tp_counts = (totals.get("tp_counts") or {})
    # Lấy đủ 5TP với fallback 0
    c5 = int(tp_counts.get("TP5", 0) or 0)
    c4 = int(tp_counts.get("TP4", 0) or 0)
    c3 = int(tp_counts.get("TP3", 0) or 0)
    c2 = int(tp_counts.get("TP2", 0) or 0)
    c1 = int(tp_counts.get("TP1", 0) or 0)
    cs = int(tp_counts.get("SL", 0) or 0)

    # Win-rate: số lệnh chạm bất kỳ TP (TP1..TP5) / tổng lệnh đã đóng trong danh sách
    wins_tp = c1 + c2 + c3 + c4 + c5
    n_closed = n
    wr_pct = (wins_tp / n_closed * 100.0) if n_closed else 0.0

    # (KPI 24H) after-leverage calculations — per-signal leverage
    items_for_lev = detail_24h.get("items") or []
    sum_R_items_lev = 0.0
    sum_pct_items_lev = 0.0
    have_item_level = False
    lev_list = []
    for it in items_for_lev:
        lev_i = _item_leverage(it)
        if lev_i > 0:
            lev_list.append(lev_i)
        # Nếu item có R thì dùng theo item; nếu không, sẽ fallback sau
        try:
            Rw_i = float(it.get("R_weighted") or it.get("R_w") or it.get("R") or 0.0)
            if lev_i > 0 and Rw_i != 0.0:
                sum_R_items_lev += Rw_i * lev_i
                have_item_level = True
        except Exception:
            pass
        # % theo item (nếu có)
        try:
            pctw_i = float(it.get("pct_weighted") or it.get("pct_w") or it.get("pct") or 0.0)
            if lev_i > 0 and pctw_i != 0.0:
                sum_pct_items_lev += pctw_i * lev_i
        except Exception:
            pass

    if have_item_level:
        sum_R_lev = sum_R_items_lev
        # Nếu không gom được % theo item, fallback theo lev_avg
        if sum_pct_items_lev != 0.0:
            sum_pct_lev = sum_pct_items_lev
        else:
            lev_avg = (sum(lev_list) / len(lev_list)) if lev_list else 0.0
            if lev_avg > 0:
                sum_pct_lev = sum_pct_w * lev_avg
            else:
                # Không có lev per-item → fallback ENV
                LEV = _report_leverage()
                sum_pct_lev = sum_pct_w * LEV
                sum_R_lev   = sumR_w   * LEV
    else:
        # Không có R per-item → dùng lev_avg nếu có, ngược lại ENV
        lev_avg = (sum(lev_list) / len(lev_list)) if lev_list else 0.0
        if lev_avg > 0:
            sum_R_lev   = sumR_w   * lev_avg
            sum_pct_lev = sum_pct_w * lev_avg
        else:
            LEV = _report_leverage()
            sum_R_lev   = sumR_w   * LEV
            sum_pct_lev = sum_pct_w * LEV

    pnl_per_100_lev = sum_R_lev * 100.0
    avgR_lev        = (sum_R_lev / max(1, n))
    avg_usd_lev     = avgR_lev * 100.0

    # Build lines (new format/order)
    lines = [
        "📊 <b>Hiệu suất giao dịch:</b>",
        f"- Tổng lệnh đã đóng: {n}",
        f"- Tỉ lệ thắng: {wr_pct:.2f}%",
        f"- Lợi nhuận sau đòn bẩy: {sum_pct_lev:.2f}%",
        f"- Lợi nhuận thực (risk $100/lệnh): ${pnl_per_100_lev:.0f}",
        f"- Lợi nhuận trung bình/lệnh: {avgR_lev:.2f}R (~${avg_usd_lev:.0f})",
        f"- Tổng R: {sum_R_lev:.2f}R",
        f"- TP theo số lệnh: TP5: {c5} / TP4: {c4} / TP3: {c3} / TP2: {c2} / TP1: {c1} / SL: {cs}",
    ]
    # Lời mời nâng cấp
    if upgrade_url:
        lines.append("🔒 <b>Nâng cấp Plus</b> để xem full tín hiệu & nhận thông báo sớm hơn.")
        lines.append(f'<a href="{upgrade_url}">👉 Nâng cấp ngay</a>')
    return "\n".join(lines)

# NEW: KPI tuần (8:16 thứ 7)
def render_kpi_week(detail: dict,
                    week_label: str,
                    risk_per_trade_usd: float = 100.0,
                    upgrade_url: str | None = None) -> str:
    totals = detail.get("totals") or {}
    n   = int(totals.get("n") or 0)
    wr  = float(totals.get("win_rate") or 0.0) * 100.0
    sum_pct = float(totals.get("sum_pct_weighted") or totals.get("sum_pct_w") or totals.get("sum_pct") or 0.0)
    sum_R   = float(totals.get("sum_R_weighted") or totals.get("sum_R") or 0.0)
    pnl_real = sum_R * risk_per_trade_usd
    avg_real = (pnl_real / n) if n else 0.0
    tpc = totals.get("tp_counts") or {}
    def _i(x): return int(tpc.get(x) or 0)
    # (KPI TUẦN) after-leverage calculations — per-signal leverage
    items_for_lev = detail.get("items") or []
    sum_R_items_lev = 0.0
    sum_pct_items_lev = 0.0
    have_item_level = False
    lev_list = []
    for it in items_for_lev:
        lev_i = _item_leverage(it)
        if lev_i > 0:
            lev_list.append(lev_i)
        try:
            Rw_i = float(it.get("R_weighted") or it.get("R_w") or it.get("R") or 0.0)
            if lev_i > 0 and Rw_i != 0.0:
                sum_R_items_lev += Rw_i * lev_i
                have_item_level = True
        except Exception:
            pass
        try:
            pctw_i = float(it.get("pct_weighted") or it.get("pct_w") or it.get("pct") or 0.0)
            if lev_i > 0 and pctw_i != 0.0:
                sum_pct_items_lev += pctw_i * lev_i
        except Exception:
            pass

    if have_item_level:
        sum_R_lev = sum_R_items_lev
        if sum_pct_items_lev != 0.0:
            sum_pct_lev = sum_pct_items_lev
        else:
            lev_avg = (sum(lev_list) / len(lev_list)) if lev_list else 0.0
            if lev_avg > 0:
                sum_pct_lev = sum_pct * lev_avg
            else:
                LEV = _report_leverage()
                sum_pct_lev = sum_pct * LEV
                sum_R_lev   = sum_R   * LEV
    else:
        lev_avg = (sum(lev_list) / len(lev_list)) if lev_list else 0.0
        if lev_avg > 0:
            sum_R_lev   = sum_R   * lev_avg
            sum_pct_lev = sum_pct * lev_avg
        else:
            LEV = _report_leverage()
            sum_R_lev   = sum_R   * LEV
            sum_pct_lev = sum_pct * LEV

    pnl_real_lev  = sum_R_lev * 100.0           # risk $100/lệnh
    avgR_lev      = (sum_R_lev / max(1, n))
    avg_real_lev  = avgR_lev * 100.0

    # Build lines (new format/order)
    lines = [
        f"<b>🧭 Kết quả giao dịch tuần qua - {week_label}</b>",
        f"- Tổng lệnh đã đóng: {n}",
        f"- Tỉ lệ thắng: {wr:.2f}%",
        f"- Lợi nhuận sau đòn bẩy: {sum_pct_lev:.2f}%",
        f"- Lợi nhuận thực (risk $100/lệnh): ${pnl_real_lev:.0f}",
        f"- Lợi nhuận trung bình/lệnh: {avgR_lev:.2f}R (~${avg_real_lev:.0f})",
        f"- Tổng R: {sum_R_lev:.2f}R",
        f"- TP theo số lệnh: TP5: {_i('TP5')} / TP4: {_i('TP4')} / TP3: {_i('TP3')} / TP2: {_i('TP2')} / TP1: {_i('TP1')} / SL: {_i('SL')}",
    ]
    # Lời mời nâng cấp
    if upgrade_url:
        lines.append("🔒 <b>Nâng cấp Plus</b> để xem full tín hiệu & nhận báo cáo sớm hơn.")
        lines.append(f'<a href="{upgrade_url}">👉 Nâng cấp ngay</a>')
    return "\n".join(lines)


