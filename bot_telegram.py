
import re
from typing import Dict, Any, Tuple
from templates import render_full
from datetime import datetime, timezone, timedelta
import math
from storage import JsonStore, UserDB, SignalCache, PaymentDB
from config import BOT_TOKEN, OWNER_IDS, DATA_DIR, BANK_INFO, PLAN_DEFAULT_MONTHS, PROTECT_CONTENT, WATERMARK
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

store = JsonStore(DATA_DIR)
users = UserDB(store)
signals = SignalCache(store)
payments = PaymentDB(store)

MAX_TG = 4096

def _split_for_tg(text: str, limit: int = 4000):
    if not text or len(text) <= limit:
        return [text or ""]
    parts, s = [], text
    while s:
        if len(s) <= limit:
            parts.append(s); break
        head = s[:limit+1]
        # ưu tiên cắt ở ranh giới tự nhiên
        for sep in ["\n\n", "\n", " "]:
            p = head.rfind(sep)
            if p >= int(limit*0.6):
                parts.append(s[:p]); s = s[p:].lstrip(); break
        else:
            parts.append(s[:limit]); s = s[limit:]
    return parts

def is_owner(uid: int) -> bool:
    return uid in OWNER_IDS

# ===== Helpers =====
def _fmt_ts(ts: int) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

async def plus_list_cmd(update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Chỉ admin."); return
    mode = (context.args[0] if context.args else "active").lower()
    data = users.list_active() if mode == "active" else users.list_all()
    if not data:
        await update.message.reply_text("Không có user nào." if mode=="all" else "Chưa có user ACTIVE."); return
    lines = []
    for uid, u in data.items():
        exp = int(u.get("expires_at", 0))
        lines.append(f"{uid}\tHSD={_fmt_ts(exp)}")
    text = "Danh sách " + ("ACTIVE" if mode=="active" else "ALL") + f" ({len(lines)}):\n" + "\n".join(lines[:200])
    await update.message.reply_text(text)

async def _notify_admins(context, text, reply_markup=None):
    for aid in OWNER_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception:
            pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Lấy payload từ cả context.args và fallback từ text (khi client chỉ gửi /start)
    payload = " ".join(context.args) if context.args else ""
    if not payload and update.message and update.message.text:
        parts = update.message.text.split(" ", 1)
        if len(parts) == 2:
            payload = parts[1].strip()
    # log để debug nhanh
    try:
        print("START payload=", payload, "uid=", update.effective_user.id)
    except Exception:
        pass
    if payload.startswith("show_"):
        signal_id = payload.split("_", 1)[1]
        uid = update.effective_user.id
        uname = update.effective_user.username or ""
        if users.is_plus_active(uid):
            # Ưu tiên render từ PLAN (để watermark theo user). Fallback text nếu thiếu.
            plan = signals.get_plan(signal_id)
            if plan:
                txt = render_full(plan, uname, watermark=WATERMARK)
            else:
                full = signals.get_full(signal_id)
                if not full:
                    await update.message.reply_text("Xin lỗi, tín hiệu đã hết hạn cache.", quote=True)
                    return
                txt = full
            await update.message.reply_text(txt, parse_mode="HTML", protect_content=PROTECT_CONTENT)
        else:
            await upsell(update, context)
    elif payload.startswith("kpi_"):
            # mở KPI đầy đủ từ cache và gửi trong DM
            kpi_id = payload.strip()
            full = signals.get_full(kpi_id.replace("kpi_", "", 1)) or signals.get_full(kpi_id)
            if not full:
                await update.message.reply_text("KPI đã hết hạn cache hoặc không tồn tại.")
                return
            chunks = _split_for_tg(full, 4000)
            for i, ch in enumerate(chunks, 1):
                header = f"<b>KPI đầy đủ (phần {i}/{len(chunks)})</b>\n" if len(chunks) > 1 else ""
                await update.message.reply_text(header + ch, parse_mode="HTML", protect_content=PROTECT_CONTENT)
            return
    elif payload.startswith("upgrade"):
        await upsell(update, context)
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Xem full mới nhất", callback_data="show_latest")],
            [InlineKeyboardButton("Nâng cấp Plus", callback_data="upgrade")]
        ])
        await update.message.reply_text(
            "Chào bạn!\n• Nếu bạn vừa bấm từ Channel mà không thấy paywall, hãy dùng các nút dưới đây.",
            reply_markup=kb
        )

# New: /latest – xem tín hiệu mới nhất theo tier
async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid = signals.get_latest_id()
    if not sid:
        await update.message.reply_text("Chưa có tín hiệu nào được đăng.")
        return
    uid = update.effective_user.id
    if users.is_plus_active(uid):
        uname = update.effective_user.username or ""
        plan = signals.get_plan(sid)
        if plan:
            txt = render_full(plan, uname, watermark=WATERMARK)
            await update.message.reply_text(txt, parse_mode="HTML", protect_content=PROTECT_CONTENT)
        else:
            await update.message.reply_text("Tín hiệu mới nhất đã hết hạn cache.")
    else:
        await upsell(update, context)

# New: /show <id> – xem theo id thủ công
async def show_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Dùng: /show <signal_id>")
        return
    sid = context.args[0]
    uid = update.effective_user.id
    if users.is_plus_active(uid):
        uname = update.effective_user.username or ""
        plan = signals.get_plan(sid)
        if plan:
            txt = render_full(plan, uname, watermark=WATERMARK)
            await update.message.reply_text(txt, parse_mode="HTML", protect_content=PROTECT_CONTENT)
        else:
            await update.message.reply_text("ID này đã hết hạn cache hoặc không tồn tại.")
    else:
        await upsell(update, context)

async def upsell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # sinh order ngắn, duy nhất theo thời điểm + 4 số cuối user_id
    from datetime import datetime
    uid = update.effective_user.id
    ts = datetime.now().strftime("%y%m%d%H%M")
    order_id = f"ORD{ts}-{str(uid)[-4:]}"
    pay_note = f"{order_id} {uid}"

    # có thể sửa BANK_INFO qua ENV; bên dưới có giá trị mặc định fallback theo yêu cầu của bạn
    text = (
        "🧭 Nâng cấp để truy cập 10+ full signal/ngày.\n"
        "<b>Phí:</b> 399k/30 ngày - 999k/90 ngày\n\n"
        "Thanh toán qua TK ngân hàng:\n"
        f"<b>Số TK:</b> {BANK_INFO.get('account_number','0378285345')}\n"
        f"<b>Chủ TK:</b> {BANK_INFO.get('account_name','TRAN KHANH HUNG')}\n"
        f"<b>Ngân hàng:</b> {BANK_INFO.get('name','Ngân hàng Quân đội - MBBank')}\n"
        f"<b>Nội dung CK:</b> <code>{pay_note}</code>\n\n"
        "Sau khi chuyển, bấm nút bên dưới để gửi xác nhận."
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Đã chuyển xong", callback_data=f"paid:{order_id}")]])
    if update.message:
        try:
            qr_path = BANK_INFO.get("qr_image_path")
            if qr_path:
                await update.message.reply_photo(photo=InputFile(qr_path), caption=text, parse_mode="HTML", reply_markup=kb)
                return
        except Exception:
            pass
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await update.callback_query.message.reply_text(text, parse_mode="HTML", reply_markup=kb)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    if data.startswith("paid"):
        # User xác nhận đã chuyển
        parts = data.split(":", 1)
        order_id = parts[1] if len(parts) == 2 else None
        uid = update.effective_user.id
        uname = update.effective_user.username or "—"
        payments.add(uid, amount=None, bank_ref=None, months=PLAN_DEFAULT_MONTHS,
                     approved=False, admin_id=None, order_id=order_id)
        await q.answer("Đã ghi nhận. Admin sẽ duyệt trong ít phút.")
        await q.edit_message_reply_markup(None)

        # Notify admin ngay với nút duyệt nhanh
        mention = f'<a href="tg://user?id={uid}">{uname}</a>'
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Duyệt +30d", callback_data=f"admin_approve:{uid}:30:{order_id}")],
            [InlineKeyboardButton("Duyệt +90d", callback_data=f"admin_approve:{uid}:90:{order_id}")],
            [InlineKeyboardButton("Từ chối",    callback_data=f"admin_reject:{uid}:{order_id}")]
        ])
        txt = (f"📥 <b>Yêu cầu nâng cấp</b>\n"
               f"• User: {mention} (id={uid})\n"
               f"• Order: <code>{order_id or '—'}</code>\n"
               f"• Thời điểm: {_fmt_ts(int(datetime.now().timestamp()))}")
        await _notify_admins(context, txt, reply_markup=kb)
        return
    # Admin inline actions
    if data.startswith("admin_"):
        actor = q.from_user.id
        if not is_owner(actor):
            await q.answer("Chỉ admin.", show_alert=True); return
        parts = data.split(":")
        kind = parts[0]            # admin_approve / admin_reject
        tgt  = int(parts[1])
        if kind == "admin_approve":
            days = int(parts[2])
            # cộng ngày cho user
            users.extend_days(tgt, days)
            new_exp = users.get(tgt).get("expires_at", 0)
            await q.edit_message_text(f"✅ Đã duyệt +{days}d cho {tgt}. HSD mới: {_fmt_ts(int(new_exp))}")
            # báo cho user
            try:
                await context.bot.send_message(chat_id=tgt,
                    text=f"🎉 PLUS đã được kích hoạt thêm {days} ngày. HSD mới: {_fmt_ts(int(new_exp))}")
            except Exception:
                pass
        elif kind == "admin_reject":
            await q.edit_message_text(f"❌ Đã đánh dấu từ chối cho user {tgt}.")
        return
    elif data == "upgrade":
        await upsell(update, context)
    elif data == "show_latest":
        # Giống lệnh /latest nhưng chạy qua callback
        sid = signals.get_latest_id()
        if not sid:
            await q.answer("Chưa có tín hiệu nào.", show_alert=True)
            return
        uid = update.effective_user.id
        if users.is_plus_active(uid):
            uname = update.effective_user.username or ""
            plan = signals.get_plan(sid)
            if plan:
                txt = render_full(plan, uname, watermark=WATERMARK)
                await q.message.reply_text(txt, parse_mode="HTML", protect_content=PROTECT_CONTENT)
            else:
                await q.message.reply_text("Tín hiệu mới nhất đã hết hạn cache.")
        else:
            await upsell(update, context)
async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    # /approve @username 30d  OR  /approve 123456789 2M
    try:
        args = update.message.text.split()[1:]
        target = args[0]
        months = 1
        if len(args) > 1:
            m = re.match(r"(\\d+)([mMdD])?", args[1])
            if m:
                val = int(m.group(1))
                unit = (m.group(2) or "M").upper()
                months = val if unit == "M" else max(1, val // 30)
        if target.startswith("@"):
            # find by username
            # In practice, you should map usernames to IDs. For simplicity, require numeric ID for now.
            await update.message.reply_text("Hãy dùng ID số (ví dụ: /approve 123456789 1M).")
            return
        uid = int(target)
        users.upsert(uid, months=months)
        await update.message.reply_text(f"Đã kích hoạt Plus cho {uid} trong {months} tháng.")
        try:
            await context.bot.send_message(chat_id=uid, text=f"Plus đã kích hoạt đến hạn sau {months} tháng. Bạn có thể bấm 'Xem full' ở teaser mới nhất.")
        except Exception:
            pass
    except Exception as e:
        await update.message.reply_text(f"Sai cú pháp. Dùng: /approve <telegram_id> <1M|30D>. Lỗi: {e}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = users.get(uid)
    if not u or not u.get("expires_at"):
        await update.message.reply_text("Trạng thái: Free")
        return
    left = max(0, u["expires_at"] - int(__import__("time").time()))
    days = left // (24*3600)
    await update.message.reply_text(f"Trạng thái: Plus • còn {days} ngày.")

def run_bot():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("approve", approve))          # cũ, vẫn giữ nếu bạn dùng
    # ===== Admin commands mới =====
    app.add_handler(CommandHandler("plus_add", plus_add_cmd))
    app.add_handler(CommandHandler("plus_remove", plus_remove_cmd))
    app.add_handler(CommandHandler("plus_status", plus_status_cmd))
    app.add_handler(CommandHandler("upgrade", upsell))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("latest", latest))
    app.add_handler(CommandHandler("show", show_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.run_polling(drop_pending_updates=True)
    app.add_handler(CommandHandler("plus_list", plus_list_cmd))  # /plus_list [all|active]

# ===== Admin command handlers =====
async def plus_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Chỉ admin."); return
    try:
        uid = int(context.args[0]); days = int(context.args[1])
    except Exception:
        await update.message.reply_text("Cách dùng: /plus_add <user_id> <số_ngày>"); return
    users.extend_days(uid, days)
    exp = users.get(uid).get("expires_at", 0)
    await update.message.reply_text(f"✅ Đã cộng {days} ngày cho {uid}. HSD mới: {_fmt_ts(int(exp))}")
    try:
        await context.bot.send_message(chat_id=uid, text=f"🎉 PLUS đã kích hoạt thêm {days} ngày. HSD mới: {_fmt_ts(int(exp))}")
    except Exception:
        pass

async def plus_remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Chỉ admin."); return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("Cách dùng: /plus_remove <user_id>"); return
    users.revoke(uid)
    await update.message.reply_text(f"🧹 Đã gỡ PLUS của {uid}.")
    try:
        await context.bot.send_message(chat_id=uid, text="⚠️ PLUS của bạn đã bị gỡ bởi admin.")
    except Exception:
        pass

async def plus_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Chỉ admin."); return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("Cách dùng: /plus_status <user_id>"); return
    u = users.get(uid)
    exp = int(u.get("expires_at", 0))
    left = max(0, exp - int(datetime.now().timestamp()))
    days_left = left // 86400
    await update.message.reply_text(
        f"👤 {uid}\n• HSD: {_fmt_ts(exp)}\n• Còn lại: {days_left} ngày\n• Trạng thái: {'ACTIVE' if users.is_plus_active(uid) else 'EXPIRED'}"
    )
    
if __name__ == "__main__":
    run_bot()
