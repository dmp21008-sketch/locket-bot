"""Bot Telegram bán Locket Gold - thanh toán tự động qua SePay.

Luồng: khách chọn gói -> gửi username -> bot tra user (lookup) và cho khách xác nhận
-> bot gửi QR chuyển khoản (số tiền + mã đơn) -> khách chuyển tiền
-> SePay gọi webhook /webhook/sepay -> bot so khớp mã đơn + số tiền -> gọi /grant -> báo khách.
Admin vẫn có nút Duyệt/Thử lại và lệnh /cap, /huy để xử lý tay khi cần.

Biến môi trường bắt buộc:
  BOT_TOKEN, LOCKET_API_KEY, ADMIN_ID, BANK_CODE, BANK_ACC
  và một trong: SEPAY_WEBHOOK_SECRET (HMAC, khuyên dùng) hoặc SEPAY_WEBHOOK_APIKEY
Tùy chọn: LOCKET_BASE_URL, BANK_HOLDER, PAY_PREFIX (mặc định LK), DB_PATH, PORT
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import sqlite3
import time
from urllib.parse import urlencode

import httpx
from aiohttp import web
from telegram import InlineKeyboardButton as Btn
from telegram import InlineKeyboardMarkup as Markup
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_KEY = os.environ["LOCKET_API_KEY"]
ADMIN_ID = int(os.environ["ADMIN_ID"])  # Telegram user id của bạn
BASE_URL = os.getenv("LOCKET_BASE_URL", "https://locket.coz.vn")

# Tài khoản nhận tiền (đã liên kết SePay). BANK_CODE là mã ngắn theo vietqr.app/banks.json
BANK_CODE = os.environ["BANK_CODE"]
BANK_ACC = os.environ["BANK_ACC"]
BANK_HOLDER = os.getenv("BANK_HOLDER", "")
# Tiền tố mã thanh toán, phải khớp "Cấu trúc mã thanh toán" đã cấu hình trên SePay
PAY_PREFIX = os.getenv("PAY_PREFIX", "LK").upper()

SEPAY_SECRET = os.getenv("SEPAY_WEBHOOK_SECRET")
SEPAY_APIKEY = os.getenv("SEPAY_WEBHOOK_APIKEY")
if not (SEPAY_SECRET or SEPAY_APIKEY):
    raise SystemExit("Cần đặt SEPAY_WEBHOOK_SECRET (HMAC) hoặc SEPAY_WEBHOOK_APIKEY")

# packageId phải đúng với API: 1month / 1year / lifetime. Sửa giá bán ở đây.
PACKAGES = {
    "1month": ("Gold 1 tháng", 30000),
    "1year": ("Gold 1 năm", 150000),
    "lifetime": ("Gold vĩnh viễn", 250000),
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("locket-bot")

CODE_RE = re.compile(rf"{re.escape(PAY_PREFIX)}[0-9A-F]{{8}}")
CLAIMABLE = ("pending_payment", "paid_waiting", "paid_waiting_installation", "install_confirmed", "failed")

# ---------- DB ----------
db = sqlite3.connect(os.getenv("DB_PATH", "orders.db"), check_same_thread=False)
db.execute(
    """CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY, tg_id INTEGER, tg_name TEXT,
        locket_user TEXT, display_name TEXT, package TEXT,
        price INTEGER, status TEXT, created INTEGER)"""
)
db.execute(
    """CREATE TABLE IF NOT EXISTS webhook_logs (
        transaction_id INTEGER PRIMARY KEY, body TEXT, created INTEGER)"""
)
# Nếu bot tắt giữa lúc đang cấp, đơn kẹt 'processing' -> cho phép thử lại (Idempotency-Key chống cấp trùng)
db.execute("UPDATE orders SET status='failed' WHERE status='processing'")
db.commit()


def set_status(order_id: str, status: str) -> None:
    db.execute("UPDATE orders SET status=? WHERE id=?", (status, order_id))
    db.commit()


def get_order(order_id: str):
    return db.execute(
        "SELECT id, tg_id, locket_user, display_name, package, price, status "
        "FROM orders WHERE id=?",
        (order_id,),
    ).fetchone()


def new_order_id() -> str:
    return PAY_PREFIX + secrets.token_hex(4).upper()  # khó đoán, không có ký tự đặc biệt


# ---------- Locket API ----------
async def locket_post(path: str, payload: dict, headers: dict | None = None) -> dict:
    h = {"X-API-Key": API_KEY, **(headers or {})}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{BASE_URL}{path}", json=payload, headers=h)
    try:
        body = r.json()
    except ValueError:
        body = {"success": False, "error": r.text[:200]}
    body["_http"] = r.status_code
    return body


def is_ok(res: dict) -> bool:
    return res.get("_http") == 200 and res.get("success", True) is not False


# Mã lỗi theo tài liệu Locket Gold -> gợi ý cho admin / lời nhắn cho khách
ADMIN_HINTS = {
    "INVALID_API_KEY": "API key sai/đã đổi. Kiểm tra LOCKET_API_KEY.",
    "INVALID_REQUEST": "Thiếu tham số hoặc dữ liệu không hợp lệ.",
    "USER_NOT_FOUND": "Không tìm thấy tài khoản Locket của khách.",
    "INSUFFICIENT_CREDITS": "Hết lượt Gold. Nạp thêm lượt rồi bấm Thử lại.",
    "ORDER_NOT_FOUND": "Không tìm thấy đơn Gold thuộc CTV.",
    "RATE_LIMITED": "Gửi quá nhiều request, đợi một lúc rồi Thử lại.",
    "SERVICE_UNAVAILABLE": "Dịch vụ tạm thời không sẵn sàng, Thử lại sau.",
}
CUSTOMER_MSGS = {
    "USER_NOT_FOUND": "Không tìm thấy tài khoản này, bạn kiểm tra lại username/link rồi gửi lại nhé.",
    "INVALID_REQUEST": "Thông tin chưa hợp lệ, bạn gửi lại username/link/UID giúp mình nhé.",
    "RATE_LIMITED": "Hệ thống đang bận, bạn thử lại sau ít giây nhé.",
    "SERVICE_UNAVAILABLE": "Hệ thống tạm thời bảo trì, bạn thử lại sau ít phút nhé.",
}


def error_code(res: dict) -> str:
    """Lấy mã lỗi từ response (chưa biết chính xác nằm ở trường nào nên thử vài chỗ)."""
    err = res.get("error")
    candidates = [res.get("code"), res.get("error_code")]
    if isinstance(err, dict):
        candidates += [err.get("code"), err.get("error_code")]
    elif isinstance(err, str):
        candidates.append(err)
    for c in candidates:
        if isinstance(c, str) and c.upper() in ADMIN_HINTS:
            return c.upper()
    return ""


# ---------- Cấp Gold (dùng chung cho webhook và admin) ----------
async def fulfill(order_id: str, bot) -> tuple[bool, str]:
    """Gọi /grant cho đơn. Trả (True, "") nếu cấp xong; (False, "already") nếu đơn đang/đã
    được xử lý; (False, <lý do>) nếu lỗi."""
    order = get_order(order_id)
    if not order:
        return False, "Không tìm thấy đơn"
    _, tg_id, user, dname, pkg, _price, _status = order

    # Giành quyền xử lý bằng 1 câu UPDATE nguyên tử: 2 webhook/2 lần bấm cùng lúc chỉ 1 bên thắng.
    cur = db.execute(
        f"UPDATE orders SET status='processing' WHERE id=? AND status IN "
        f"({','.join('?' * len(CLAIMABLE))})",
        (order_id, *CLAIMABLE),
    )
    db.commit()
    if cur.rowcount == 0:
        return False, "already"

    # Idempotency-Key = mã đơn: gọi lại không bị cấp/trừ lượt hai lần.
    res = await locket_post(
        "/api/v1/grant",
        {"user": user, "packageId": pkg, "confirm": True,
         "displayName": dname, "note": f"order {order_id}"},
        headers={"Idempotency-Key": f"order-{order_id}"},
    )
    granted = res.get("data") or {}
    if is_ok(res) and granted.get("status") == "active" and granted.get("packageId") == pkg:
        set_status(order_id, "done")
        await bot.send_message(
            tg_id,
            "✨ <b>LOCKET GOLD ĐÃ KÍCH HOẠT!</b>\n\n"
            f"👤 Tài khoản: <code>@{user}</code>\n"
            f"📦 Gói: <b>{PACKAGES[pkg][0]}</b>\n\n"
            "Mở Locket để kiểm tra trạng thái Gold nhé. Cảm ơn bạn đã ủng hộ!",
            parse_mode="HTML",
        )
        return True, ""

    set_status(order_id, "failed")
    log.error("grant fail %s: %s", order_id, res)
    code = error_code(res)
    return False, f"[{code or res.get('_http')}] " + ADMIN_HINTS.get(code, str(res)[:300])


async def edit(q, text: str, markup=None) -> None:
    """Sửa tin nhắn dù nó là ảnh (caption) hay chữ."""
    if q.message.photo:
        await q.edit_message_caption(text, reply_markup=markup, parse_mode="HTML")
    else:
        await q.edit_message_text(text, reply_markup=markup, parse_mode="HTML")


def retry_markup(order_id: str) -> Markup:
    return Markup([[Btn("🔁 Cấp Gold / Thử lại", callback_data=f"ok:{order_id}")]])


def dns_markup(order_id: str) -> Markup:
    return Markup([
        [Btn("✅ Tôi đã cài DNS xong", callback_data=f"dnsdone:{order_id}")],
        [Btn("↩️ Hủy / quay lại đổi gói", callback_data=f"change:{order_id}")],
    ])


def activate_markup(order_id: str) -> Markup:
    return Markup([
        [Btn("⚡ KÍCH HOẠT GOLD", callback_data=f"activate:{order_id}")],
        [Btn("📖 Xem lại hướng dẫn DNS", callback_data=f"dnshelp:{order_id}")],
    ])


def dns_instructions(order_id: str, user: str, package: str) -> str:
    return (
        "🎉 <b>THANH TOÁN THÀNH CÔNG!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🧾 Mã đơn: <code>{order_id}</code>\n"
        f"👤 Tài khoản: <code>@{user}</code>\n"
        f"📦 Gói đã mua: <b>{package}</b>\n\n"
        "📲 <b>HƯỚNG DẪN CÀI DNS</b>\n\n"
        "<b>1.</b> Mở Safari, truy cập https://dns.coz.vn để tải Profile.\n"
        "<b>2.</b> Bấm <b>Cho phép (Allow)</b> khi máy hỏi tải hồ sơ cấu hình.\n"
        "<b>3.</b> Vào <b>Cài đặt → Đã tải hồ sơ (Profile Downloaded) → Cài đặt</b>, "
        "nhập mật khẩu máy nếu được yêu cầu rồi tiếp tục cài đặt.\n"
        "<b>4.</b> Nếu hồ sơ không tự hiện, vào <b>Cài đặt → Cài đặt chung → VPN & Quản lý thiết bị</b> để cài.\n\n"
        "💡 Nếu đã cài DNS trước đó thì không cần cài lại.\n\n"
        "⚠️ <b>LƯU Ý QUAN TRỌNG</b>\n"
        "Không tắt hoặc xóa DNS và không tắt tin cậy chứng chỉ LocketGold. "
        "Nếu tắt/xóa, Gold có thể bị rớt và không còn hoạt động.\n\n"
        "Cài xong hãy bấm <b>“Tôi đã cài DNS xong”</b> để tiếp tục."
    )


# ---------- Telegram handlers ----------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    rows = [
        [Btn(f"{name} - {price:,}đ", callback_data=f"pkg:{pid}")]
        for pid, (name, price) in PACKAGES.items()
    ]
    await update.message.reply_text(
        "💎 <b>LOCKET GOLD STORE</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Nâng cấp tài khoản nhanh chóng, tự động qua SePay.\n\n"
        "📦 <b>Chọn gói Gold phù hợp:</b>",
        reply_markup=Markup(rows), parse_mode="HTML"
    )


async def choose_package(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pid = q.data.split(":")[1]
    ctx.user_data.update(pkg=pid, awaiting="username")
    await q.edit_message_text(
        f"💎 <b>GÓI ĐÃ CHỌN</b>\n{PACKAGES[pid][0]}\n\n"
        "👤 Gửi username Locket, link Locket hoặc UID của tài khoản cần nâng cấp:",
        parse_mode="HTML"
    )


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if ctx.user_data.get("awaiting") != "username":
        await update.message.reply_text("Gõ /start để bắt đầu mua nhé.")
        return
    text = update.message.text.strip()
    res = await locket_post("/api/v1/lookup", {"user": text})
    if not is_ok(res):
        code = error_code(res)
        await update.message.reply_text(
            CUSTOMER_MSGS.get(code, CUSTOMER_MSGS["USER_NOT_FOUND"])
        )
        log.warning("lookup fail (%s): %s", code, res)
        if code == "INVALID_API_KEY":  # lỗi phía bạn, báo admin
            await ctx.bot.send_message(ADMIN_ID, "⚠️ Lookup lỗi: " + ADMIN_HINTS[code])
        return
    profile = (res.get("data") or {}).get("profile") or {}
    username = profile.get("username") or text
    name = profile.get("name") or username
    avatar = profile.get("avatar")
    ctx.user_data.update(target=username, display_name=name, awaiting="confirm")
    caption = f"Tài khoản: {name} (@{username})\nĐúng tài khoản cần nâng cấp chưa?"
    kb = Markup([[Btn("✅ Đúng", callback_data="confirm"), Btn("↩️ Nhập lại", callback_data="redo")]])
    try:
        if not avatar:
            raise ValueError("no avatar")
        await update.message.reply_photo(avatar, caption=caption, reply_markup=kb)
    except Exception:  # avatar lỗi/không tải được thì gửi chữ
        await update.message.reply_text(caption, reply_markup=kb)


async def redo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx.user_data["awaiting"] = "username"
    await q.edit_message_reply_markup(reply_markup=None)  # tin có thể là ảnh nên chỉ gỡ nút
    await q.message.reply_text("Gửi lại username / link / UID Locket:")


async def confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = ctx.user_data
    if d.get("awaiting") != "confirm":
        return
    name, price = PACKAGES[d["pkg"]]
    order_id = new_order_id()
    u = q.from_user
    db.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
        (order_id, u.id, u.full_name, d["target"], d["display_name"],
         d["pkg"], price, "pending_payment", int(time.time())),
    )
    db.commit()
    d.update(awaiting="proof", order_id=order_id)
    await q.edit_message_reply_markup(reply_markup=None)

    qr = "https://vietqr.app/img?" + urlencode(
        {"acc": BANK_ACC, "bank": BANK_CODE, "amount": price, "des": order_id}
    )
    text = (
        f"Đơn #{order_id}\nGói: {name}\nTài khoản: @{d['target']}\n\n"
        f"Quét QR hoặc chuyển khoản:\nNgân hàng: {BANK_CODE}\nSTK: {BANK_ACC}\n"
        + (f"Chủ TK: {BANK_HOLDER}\n" if BANK_HOLDER else "")
        + f"Số tiền: {price:,}đ\nNội dung: {order_id}\n\n"
        "⚠️ Giữ nguyên số tiền và nội dung. Chuyển xong hệ thống tự nâng cấp trong vài giây."
    )
    try:
        await ctx.bot.send_photo(q.message.chat_id, qr, caption=text)
    except Exception:
        log.exception("send QR fail")
        await ctx.bot.send_message(q.message.chat_id, text)


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Dự phòng: khách gửi biên lai thì chuyển cho admin duyệt tay."""
    order_id = ctx.user_data.get("order_id")
    if ctx.user_data.get("awaiting") != "proof" or not order_id:
        return
    order = get_order(order_id)
    if order[6] == "pending_payment":
        set_status(order_id, "paid_waiting")
    await update.message.reply_text(
        "Đã nhận biên lai. Nếu tiền đã vào hệ thống sẽ tự nâng cấp, còn không admin sẽ kiểm tra giúp bạn."
    )
    _, tg_id, user, dname, pkg, price, _s = order
    await ctx.bot.send_photo(
        ADMIN_ID,
        update.message.photo[-1].file_id,
        caption=(f"Biên lai đơn #{order_id}\nKhách: {update.effective_user.full_name} ({tg_id})\n"
                 f"Locket: @{user} ({dname})\nGói: {PACKAGES[pkg][0]} - {price:,}đ\n"
                 "(Chỉ bấm Duyệt sau khi đã thấy tiền trong app ngân hàng)"),
        reply_markup=Markup(
            [[Btn("✅ Duyệt & cấp Gold", callback_data=f"ok:{order_id}"),
              Btn("❌ Từ chối", callback_data=f"no:{order_id}")]]
        ),
    )


async def dns_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    order_id = q.data.split(":", 1)[1]
    order = get_order(order_id)
    if not order or order[1] != q.from_user.id:
        await q.answer("Không tìm thấy đơn của bạn.", show_alert=True)
        return
    status = order[6]
    if status == "done":
        await edit(q, "✅ Đơn này đã được kích hoạt Gold rồi.")
        return
    if status not in ("paid_waiting_installation", "install_confirmed", "failed"):
        await q.answer("Đơn chưa sẵn sàng hoặc đang được xử lý.", show_alert=True)
        return
    if status != "install_confirmed":
        set_status(order_id, "install_confirmed")
    await edit(
        q,
        "✅ <b>ĐÃ XÁC NHẬN CÀI DNS</b>\n\n"
        "Kiểm tra DNS đã được cài và không bị tắt. Khi sẵn sàng, bấm nút bên dưới để gọi hệ thống kích hoạt Locket Gold.",
        activate_markup(order_id),
    )


async def dns_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    order_id = q.data.split(":", 1)[1]
    order = get_order(order_id)
    if not order or order[1] != q.from_user.id:
        await q.answer("Không tìm thấy đơn của bạn.", show_alert=True)
        return
    await q.message.reply_text(
        dns_instructions(order_id, order[2], PACKAGES[order[4]][0]),
        reply_markup=dns_markup(order_id), parse_mode="HTML", disable_web_page_preview=True
    )


async def activate_gold(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    order_id = q.data.split(":", 1)[1]
    order = get_order(order_id)
    if not order or order[1] != q.from_user.id:
        await q.answer("Không tìm thấy đơn của bạn.", show_alert=True)
        return
    if order[6] == "done":
        await q.answer("Gold của đơn này đã được kích hoạt.", show_alert=True)
        return
    if order[6] not in ("install_confirmed", "failed"):
        await q.answer("Vui lòng xác nhận đã cài DNS trước.", show_alert=True)
        return
    await q.answer("Đang gửi yêu cầu kích hoạt...")
    await edit(q, "⏳ <b>ĐANG KÍCH HOẠT LOCKET GOLD</b>\nHệ thống đang xử lý, vui lòng đợi một chút…")
    ok, msg = await fulfill(order_id, ctx.bot)
    if ok:
        await edit(q, "🎉 <b>YÊU CẦU KÍCH HOẠT THÀNH CÔNG!</b>\nMở ứng dụng Locket để kiểm tra Gold nhé.")
    elif msg == "already":
        current = get_order(order_id)
        if current and current[6] == "done":
            await edit(q, "✅ Đơn này đã kích hoạt Gold rồi.")
        else:
            await edit(q, "⏳ Đơn đang được xử lý. Vui lòng đợi và không bấm liên tục.")
    else:
        await edit(
            q,
            "⚠️ <b>CHƯA KÍCH HOẠT ĐƯỢC</b>\nTiền của bạn vẫn được ghi nhận. Vui lòng thử lại sau hoặc chờ admin hỗ trợ.\n\n"
            f"Mã đơn: <code>{order_id}</code>",
            Markup([[Btn("🔁 Thử kích hoạt lại", callback_data=f"activate:{order_id}")]])
        )
        await notify_admin(ctx.bot, f"⚠️ Đơn #{order_id} đã thanh toán, khách xác nhận DNS nhưng cấp Gold lỗi: {msg}", retry_markup(order_id))


async def change_package(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    order_id = q.data.split(":", 1)[1]
    order = get_order(order_id)
    if not order or order[1] != q.from_user.id:
        await q.answer("Không tìm thấy đơn của bạn.", show_alert=True)
        return
    if order[6] == "done":
        await edit(q, "Đơn này đã kích hoạt Gold. Nếu cần đổi gói, vui lòng liên hệ admin.")
        return
    set_status(order_id, "change_requested")
    await notify_admin(
        ctx.bot,
        f"↩️ Khách yêu cầu đổi gói sau khi thanh toán. Đơn #{order_id}, khách {order[1]}, "
        f"gói {PACKAGES[order[4]][0]} ({order[5]:,}đ). Cần đối soát khoản đã nhận trước khi tạo đơn mới."
    )
    await edit(
        q,
        "↩️ <b>ĐÃ GHI NHẬN YÊU CẦU ĐỔI GÓI</b>\n\n"
        "Vì đơn này đã thanh toán, hệ thống giữ nguyên thông tin để tránh mất tiền hoặc cấp nhầm gói. "
        "Admin sẽ kiểm tra khoản thanh toán trước khi bạn tạo đơn mới.\n\n"
        "Bạn có thể quay lại menu gói, nhưng vui lòng không thanh toán đơn mới cho đến khi được xác nhận.",
        Markup([[Btn("📦 Quay lại menu gói", callback_data="restart")]])
    )


async def restart_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx.user_data.clear()
    rows = [[Btn(f"{name} - {price:,}đ", callback_data=f"pkg:{pid}")] for pid, (name, price) in PACKAGES.items()]
    await edit(q, "💎 <b>LOCKET GOLD STORE</b>\n━━━━━━━━━━━━━━━━━━\n📦 Chọn gói Gold muốn mua:", Markup(rows))


async def admin_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        await q.answer("Không có quyền", show_alert=True)
        return
    await q.answer()
    action, order_id = q.data.split(":")
    order = get_order(order_id)
    if not order:
        return
    tg_id, status = order[1], order[6]
    if action == "ok" and status not in ("install_confirmed", "failed"):
        await edit(q, f"Đơn #{order_id} chưa được khách xác nhận cài DNS. Chưa thể cấp Gold.")
        return
    if status == "done":
        await edit(q, f"Đơn #{order_id} đã cấp rồi.")
        return

    if action == "no":
        set_status(order_id, "rejected")
        await edit(q, f"Đơn #{order_id}: ĐÃ TỪ CHỐI")
        await ctx.bot.send_message(tg_id, f"Đơn #{order_id} bị từ chối. Liên hệ admin nếu có nhầm lẫn.")
        return

    ok, msg = await fulfill(order_id, ctx.bot)
    if ok:
        await edit(q, f"Đơn #{order_id}: ĐÃ CẤP ✅")
    elif msg == "already":
        await edit(q, f"Đơn #{order_id} đang được xử lý hoặc đã xong.")
    else:
        await edit(q, f"Đơn #{order_id}: LỖI cấp Gold\n{msg}", retry_markup(order_id))


async def grant_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /cap <mã đơn> - cấp tay cho đơn đã nhận tiền (vd webhook không khớp)."""
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        await update.message.reply_text("Dùng: /cap <mã đơn>")
        return
    order_id = ctx.args[0].upper()
    if not get_order(order_id):
        await update.message.reply_text("Không thấy đơn này.")
        return
    ok, msg = await fulfill(order_id, ctx.bot)
    await update.message.reply_text(
        "Đã cấp ✅" if ok else ("Đơn đang xử lý hoặc đã xong." if msg == "already" else f"Lỗi: {msg}"),
        reply_markup=None if ok or msg == "already" else retry_markup(order_id),
    )


async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin: /huy <username|link|uid> - thu hồi Gold (hoàn lượt nếu trong 72h)."""
    if update.effective_user.id != ADMIN_ID:
        return
    if not ctx.args:
        await update.message.reply_text("Dùng: /huy <username|link|uid>")
        return
    res = await locket_post("/api/v1/cancel", {"user": ctx.args[0]})
    code = error_code(res)
    await update.message.reply_text(
        res.get("message") or ADMIN_HINTS.get(code) or str(res)[:300]
    )


# ---------- Webhook SePay ----------
def verify_sepay(request: web.Request, raw: bytes) -> bool:
    """Xác thực request thật sự từ SePay. Không cấu hình gì -> từ chối hết (fail closed)."""
    if SEPAY_SECRET:
        sig = request.headers.get("X-SePay-Signature", "")
        try:
            ts = int(request.headers.get("X-SePay-Timestamp", ""))
        except ValueError:
            return False
        if abs(time.time() - ts) > 300:  # chống replay
            return False
        mac = hmac.new(SEPAY_SECRET.encode(), f"{ts}.".encode() + raw, hashlib.sha256)
        return hmac.compare_digest("sha256=" + mac.hexdigest(), sig)
    auth = request.headers.get("Authorization", "")
    return hmac.compare_digest(auth, f"Apikey {SEPAY_APIKEY}")


def find_order(data: dict) -> str | None:
    """Tìm mã đơn trong field `code` (SePay tự tách) hoặc trong nội dung chuyển khoản."""
    for text in (data.get("code"), data.get("content")):
        if not text:
            continue
        for m in CODE_RE.findall(str(text).upper().replace(" ", "")):
            if get_order(m):
                return m
    return None


async def notify_admin(bot, text: str, markup=None) -> None:
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=markup)
    except Exception:
        log.exception("notify admin fail")


async def handle_payment(bot, data: dict) -> None:
    try:
        amount = int(data.get("transferAmount") or 0)
        order_id = find_order(data)
        if not order_id:
            await notify_admin(
                bot,
                f"💸 Tiền vào không khớp đơn nào: {amount:,}đ\nNội dung: {data.get('content')}\n"
                f"Mã GD SePay: {data.get('id')}\nNếu là của khách, dùng /cap <mã đơn>.",
            )
            return
        _, tg_id, user, _dname, pkg, price, status = get_order(order_id)
        if status == "done":
            return
        if status == "rejected":
            await notify_admin(bot, f"⚠️ Đơn #{order_id} đã bị từ chối nhưng vừa nhận {amount:,}đ. Kiểm tra/hoàn tiền.")
            return
        if amount < price:
            await notify_admin(bot, f"⚠️ Đơn #{order_id} nhận {amount:,}đ, thiếu so với giá {price:,}đ. Chưa cấp.")
            await bot.send_message(
                tg_id, f"Đơn #{order_id}: mình nhận được {amount:,}đ, chưa đủ {price:,}đ. "
                       "Bạn chuyển bù phần thiếu hoặc nhắn admin nhé."
            )
            return
        # Đánh dấu đã thanh toán và yêu cầu khách hoàn tất bước DNS trước khi gọi /grant.
        if status in ("pending_payment", "paid_waiting", "failed"):
            set_status(order_id, "paid_waiting_installation")
            try:
                await bot.send_message(
                    tg_id, dns_instructions(order_id, user, PACKAGES[pkg][0]),
                    reply_markup=dns_markup(order_id), parse_mode="HTML", disable_web_page_preview=True
                )
            except Exception:
                log.exception("send DNS instructions fail for %s", order_id)
                await notify_admin(bot, f"⚠️ Đã nhận tiền đơn #{order_id} nhưng không gửi được hướng dẫn DNS cho khách {tg_id}.")
            await notify_admin(
                bot,
                f"💰 <b>ĐÃ NHẬN THANH TOÁN</b>\nĐơn: <code>{order_id}</code>\n"
                f"Gói: {PACKAGES[pkg][0]} — {amount:,}đ\nTài khoản: @{user}\n"
                "⏳ Đang chờ khách xác nhận cài DNS; Gold chưa được cấp.",
            )
        elif status == "install_confirmed":
            # Webhook trùng giao dịch không được tự kích hoạt Gold.
            return
        elif status == "paid_waiting_installation":
            return
        elif status == "change_requested":
            await notify_admin(bot, f"⚠️ Đơn #{order_id} đã thanh toán nhưng khách yêu cầu đổi gói. Cần đối soát trước khi xử lý.")
    except Exception:
        log.exception("handle_payment fail")
        await notify_admin(bot, f"⚠️ Lỗi xử lý webhook: {data}")


_tasks: set = set()


async def sepay_webhook(request: web.Request) -> web.Response:
    raw = await request.read()
    if not verify_sepay(request, raw):
        return web.json_response({"success": False}, status=401)
    try:
        data = json.loads(raw)
    except ValueError:
        return web.json_response({"success": False}, status=400)

    # Chống trùng: SePay có thể retry cùng một giao dịch.
    cur = db.execute(
        "INSERT OR IGNORE INTO webhook_logs VALUES (?,?,?)",
        (data.get("id"), raw.decode(errors="replace"), int(time.time())),
    )
    db.commit()
    if cur.rowcount == 0 or data.get("transferType") != "in":
        return web.json_response({"success": True})

    # Trả 200 ngay, xử lý (gọi Locket API) chạy nền.
    task = asyncio.create_task(handle_payment(request.app["tg"].bot, data))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return web.json_response({"success": True})


# ---------- Khởi chạy: bot (polling) + web server nhận webhook ----------
async def run() -> None:
    tg = Application.builder().token(BOT_TOKEN).build()
    tg.add_handler(CommandHandler("start", start))
    tg.add_handler(CommandHandler("cap", grant_cmd))
    tg.add_handler(CommandHandler("huy", cancel_cmd))
    tg.add_handler(CallbackQueryHandler(choose_package, pattern=r"^pkg:"))
    tg.add_handler(CallbackQueryHandler(confirm, pattern=r"^confirm$"))
    tg.add_handler(CallbackQueryHandler(redo, pattern=r"^redo$"))
    tg.add_handler(CallbackQueryHandler(dns_done, pattern=r"^dnsdone:"))
    tg.add_handler(CallbackQueryHandler(dns_help, pattern=r"^dnshelp:"))
    tg.add_handler(CallbackQueryHandler(activate_gold, pattern=r"^activate:"))
    tg.add_handler(CallbackQueryHandler(change_package, pattern=r"^change:"))
    tg.add_handler(CallbackQueryHandler(restart_menu, pattern=r"^restart$"))
    tg.add_handler(CallbackQueryHandler(admin_action, pattern=r"^(ok|no):"))
    tg.add_handler(MessageHandler(filters.PHOTO, on_photo))
    tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    web_app = web.Application()
    web_app["tg"] = tg
    web_app.router.add_post("/webhook/sepay", sepay_webhook)
    web_app.router.add_get("/", lambda r: web.Response(text="ok"))  # health check
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()

    await tg.initialize()
    await tg.start()
    await tg.updater.start_polling()
    log.info("Bot + webhook đang chạy")
    try:
        await asyncio.Event().wait()
    finally:
        await tg.updater.stop()
        await tg.stop()
        await tg.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run())
