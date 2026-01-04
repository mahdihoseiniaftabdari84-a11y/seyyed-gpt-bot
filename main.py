import io
import os
import pandas as pd
import asyncpg

from aiogram.filters import Command
from aiogram.types import BufferedInputFile

import re
import asyncio
import random
import string
from datetime import datetime, timedelta, timezone

import aiosqlite
import jdatetime
from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import CommandStart, Command

from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext


# -------------------- ENV --------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()          # مثل: -1001234567890
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "").strip()      # مثل: https://t.me/YourChannel
if not CHANNEL_LINK:
    CHANNEL_LINK = "https://t.me/SEYEDGPT"

CARD_NUMBER = os.getenv("CARD_NUMBER", "").strip()
CARD_NAME = os.getenv("CARD_NAME", "SEYED GPT").strip()


# مسیرها
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "db.sqlite3")
EXCEL_PATH = os.path.join(BASE_DIR, "data.xlsx")

# قیمت پلن
PLAN_TITLE = "ChatGPT Plus — 1 Month (Single User)"
PLAN_PRICE = 369_000  # تومان

TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))


# -------------------- Postgres Excel (/excel) --------------------
async def fetch_users():
    if not DATABASE_URL:
        return []
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch("SELECT user_id, username, full_name FROM users ORDER BY user_id DESC")
        return [dict(r) for r in rows]
    finally:
        await conn.close()

async def send_excel_to_admin(bot, rows: list[dict], filename: str = "report.xlsx"):
    df = pd.DataFrame(rows)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="users")

    buffer.seek(0)
    file = BufferedInputFile(buffer.read(), filename=filename)

    await bot.send_document(ADMIN_ID, file, caption="📊 گزارش اکسل آماده شد.")


# -------------------- Helpers --------------------
def now_utc_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def to_jalali_str(dt: datetime) -> str:
    dt_teh = dt.astimezone(TEHRAN_TZ)
    jdt = jdatetime.datetime.fromgregorian(datetime=dt_teh.replace(tzinfo=None))
    return jdt.strftime("%Y/%m/%d %H:%M")

def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default

def is_admin(user_id: int) -> bool:
    return ADMIN_ID and user_id == ADMIN_ID

def random_discount_code(length: int = 5) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))

def random_discount_percent() -> int:
    return random.randint(20, 40)

def calc_discounted_price(price: int, percent: int) -> int:
    return int(price * (100 - percent) / 100)

def clamp_text(s: str, max_len: int = 800) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
    if len(s) > max_len:
        s = s[:max_len]
    return s

async def safe_answer(message: Message, text: str, **kwargs):
    try:
        return await message.answer(text, **kwargs)
    except Exception as e:
        if "can't parse entities" in str(e):
            kwargs.pop("parse_mode", None)
            return await message.answer(text, parse_mode=None, **kwargs)
        raise

async def safe_send(bot_obj: Bot, chat_id: int, text: str, **kwargs):
    try:
        return await bot_obj.send_message(chat_id, text, **kwargs)
    except Exception as e:
        if "can't parse entities" in str(e):
            kwargs.pop("parse_mode", None)
            return await bot_obj.send_message(chat_id, text, parse_mode=None, **kwargs)
        raise


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
PHONE_RE = re.compile(r"^(?:\+98|0)?9\d{9}$")


# -------------------- DB Migration (orders columns) --------------------
async def ensure_orders_columns(db: aiosqlite.Connection):
    needed = {
        "plan_title": "TEXT",
        "base_amount": "INTEGER",
        "discount_code": "TEXT",
        "discount_percent": "INTEGER",
        "final_amount": "INTEGER",
        "pay_method": "TEXT",
        "status": "TEXT",
        "stage": "INTEGER",
        "receipt_file_id": "TEXT",
        "approved_at": "TEXT",
        "expires_at": "TEXT",
        "email": "TEXT",
        "phone": "TEXT",
        "reward_code": "TEXT",
        "reward_percent": "INTEGER",
        "reward_issued_at": "TEXT",

        "gpt_username": "TEXT",
        "gpt_password": "TEXT",
        "gpt_sent_at": "TEXT",
    }

    cur = await db.execute("PRAGMA table_info(orders)")
    cols = await cur.fetchall()
    existing = {c[1] for c in cols}

    for col, coltype in needed.items():
        if col not in existing:
            await db.execute(f"ALTER TABLE orders ADD COLUMN {col} {coltype}")


# -------------------- DB Schema & Excel --------------------
async def ensure_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            created_at TEXT,
            last_member_check_at TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            created_at TEXT
        )
        """)
        await ensure_orders_columns(db)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS discount_codes (
            code TEXT PRIMARY KEY,
            percent INTEGER,
            issued_to_user INTEGER,
            issued_at TEXT,
            used_by_order INTEGER,
            used_at TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            text TEXT,
            created_at TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS support_threads (
            user_id INTEGER PRIMARY KEY,
            is_open INTEGER,
            opened_at TEXT,
            closed_at TEXT
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS support_links (
            admin_message_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            created_at TEXT
        )
        """)

        await db.commit()

async def upsert_user(user_id: int, username: str, full_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if row is None:
            await db.execute("""
            INSERT INTO users(user_id, username, full_name, created_at, last_member_check_at)
            VALUES (?, ?, ?, ?, ?)
            """, (user_id, username, full_name, now_utc_iso(), None))
        else:
            await db.execute("""
            UPDATE users
            SET username=?, full_name=?
            WHERE user_id=?
            """, (username, full_name, user_id))
        await db.commit()

def ensure_excel():
    if os.path.exists(EXCEL_PATH):
        return
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Orders"
    ws1.append([
        "OrderID", "UserID", "Username", "FullName",
        "Email", "Phone", "PlanTitle",
        "BaseAmount", "DiscountCode", "DiscountPercent", "FinalAmount",
        "PayMethod", "Status", "Stage",
        "CreatedAtJalali", "ApprovedAtJalali", "ExpiresAtJalali",
        "ReceiptFileID",
        "RewardCode", "RewardPercent", "RewardIssuedAtJalali",
        "GPT_Username", "GPT_Password", "GPT_SentAtJalali"
    ])
    ws2 = wb.create_sheet("Feedback")
    ws2.append(["UserID", "Username", "FullName", "Text", "CreatedAtJalali"])
    wb.save(EXCEL_PATH)

def excel_append_order(row: list):
    ensure_excel()
    try:
        wb = load_workbook(EXCEL_PATH)
        ws = wb["Orders"]
        ws.append(row)
        wb.save(EXCEL_PATH)
        wb.close()
    except PermissionError:
        fallback = EXCEL_PATH.replace(".xlsx", "_NEW.xlsx")
        print("EXCEL LOCKED. Writing to:", fallback)
    except Exception as e:
        print("EXCEL WRITE ERROR:", e)

def excel_update_order(order_id: int, **updates):
    ensure_excel()
    try:
        wb = load_workbook(EXCEL_PATH)
        ws = wb["Orders"]

        target_row = None
        for r in range(2, ws.max_row + 1):
            cell_val = ws.cell(row=r, column=1).value
            if str(cell_val) == str(order_id):
                target_row = r
                break

        if not target_row:
            wb.close()
            return

        col_map = {
            "discount_code": 9,
            "discount_percent": 10,
            "final_amount": 11,
            "pay_method": 12,
            "status": 13,
            "stage": 14,
            "approved_at_jalali": 16,
            "expires_at_jalali": 17,
            "receipt_file_id": 18,
            "reward_code": 19,
            "reward_percent": 20,
            "reward_issued_at_jalali": 21,
            "gpt_username": 22,
            "gpt_password": 23,
            "gpt_sent_at_jalali": 24,
        }

        for k, v in updates.items():
            if k in col_map:
                ws.cell(row=target_row, column=col_map[k]).value = v

        wb.save(EXCEL_PATH)
        wb.close()
    except PermissionError:
        print("EXCEL UPDATE ERROR: file is locked (close Excel).")
    except Exception as e:
        print("EXCEL UPDATE ERROR:", e)

def excel_append_feedback(row: list):
    ensure_excel()
    try:
        wb = load_workbook(EXCEL_PATH)
        ws = wb["Feedback"]
        ws.append(row)
        wb.save(EXCEL_PATH)
        wb.close()
    except PermissionError:
        print("EXCEL LOCKED. Close Excel file.")
    except Exception as e:
        print("EXCEL WRITE ERROR:", e)


# -------------------- Channel membership check --------------------
async def is_member(bot: Bot, user_id: int) -> bool:
    if not CHANNEL_ID:
        return False
    try:
        member = await bot.get_chat_member(chat_id=int(CHANNEL_ID), user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False


# -------------------- UI --------------------
def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 خرید اشتراک"), KeyboardButton(text="💎 پلن و قیمت")],
            [KeyboardButton(text="🎟 کد تخفیف"), KeyboardButton(text="💬 نظر / پیشنهاد")],
            [KeyboardButton(text="🆘 پشتیبانی (چت)"), KeyboardButton(text="❓ سوالات متداول")]
        ],
        resize_keyboard=True
    )

def cancel_only_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ لغو عملیات")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def discount_step_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⏭ بدون کد تخفیف")],
            [KeyboardButton(text="❌ لغو عملیات")]
        ],
        resize_keyboard=True
    )

def payment_methods_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💳 کارت‌به‌کارت"), KeyboardButton(text="🟦 پرداخت آنلاین (به‌زودی)")],
            [KeyboardButton(text="❌ لغو عملیات")]
        ],
        resize_keyboard=True
    )

def join_channel_inline_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ عضویت در کانال", url=CHANNEL_LINK)
    kb.button(text="🔄 بررسی عضویت", callback_data="check_join")
    kb.adjust(1)
    return kb

def stage_text(stage: int) -> str:
    return {
        1: "1️⃣ دریافت اطلاعات",
        2: "2️⃣ آماده‌سازی",
        3: "3️⃣ ارسال اطلاعات"
    }.get(stage, "—")


def admin_order_kb(order_id: int) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ تایید پرداخت", callback_data=f"admin:approve:{order_id}")
    kb.button(text="❌ رد پرداخت", callback_data=f"admin:reject:{order_id}")
    kb.button(text="🚚 مرحله 1", callback_data=f"admin:stage:{order_id}:1")
    kb.button(text="🛠 مرحله 2", callback_data=f"admin:stage:{order_id}:2")
    kb.button(text="📤 مرحله 3", callback_data=f"admin:stage:{order_id}:3")
    kb.button(text="📩 ارسال اکانت GPT", callback_data=f"admin:sendacc:{order_id}")
    kb.adjust(2, 3, 1)
    return kb


# -------------------- FSM --------------------
class Flow(StatesGroup):
    waiting_email = State()
    waiting_phone = State()
    waiting_discount = State()
    waiting_payment_choice = State()
    waiting_receipt = State()
    waiting_feedback = State()
    waiting_support = State()

class AdminFlow(StatesGroup):
    waiting_gpt_credentials = State()


# -------------------- Bot init --------------------
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Put it in .env or ENV variables.")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()


# -------------------- Access control (Channel gate) --------------------
async def require_access(msg_or_cb, user_id: int) -> bool:
    if is_admin(user_id):
        return True

    if not CHANNEL_ID:
        text = (
            "⚠️ برای استفاده از ربات، ابتدا باید عضو کانال شوید.\n\n"
            f"لینک کانال:\n{CHANNEL_LINK}\n\n"
            "بعد از عضویت، روی «🔄 بررسی عضویت» بزن ✅"
        )
        if isinstance(msg_or_cb, Message):
            await safe_answer(msg_or_cb, text, reply_markup=join_channel_inline_kb().as_markup())
        else:
            await safe_answer(msg_or_cb.message, text, reply_markup=join_channel_inline_kb().as_markup())
            await msg_or_cb.answer()
        return False

    ok = await is_member(bot, user_id)
    if not ok:
        text = "⚠️ برای استفاده از ربات، ابتدا باید عضو کانال شوید.\nبعد از عضویت، روی «🔄 بررسی عضویت» بزن ✅"
        if isinstance(msg_or_cb, Message):
            await safe_answer(msg_or_cb, text, reply_markup=join_channel_inline_kb().as_markup())
        else:
            await safe_answer(msg_or_cb.message, text, reply_markup=join_channel_inline_kb().as_markup())
            await msg_or_cb.answer()
        return False

    return True


# -------------------- Start / Home --------------------
@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()

    username = msg.from_user.username or ""
    full_name = (msg.from_user.full_name or msg.from_user.first_name or "").strip()
    await upsert_user(msg.from_user.id, username, full_name)

    if not await require_access(msg, msg.from_user.id):
        return

    await safe_answer(
        msg,
        "🌟 به *SEYED GPT* خوش اومدی!\n\n"
        "از منوی پایین انتخاب کن 👇",
        reply_markup=main_menu_kb()
    )


# ✅ Handler دستور /excel (طبق چیزی که خواستی)
@dp.message(Command("excel"))
async def cmd_excel(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ فقط ادمین اجازه دارد.")

    rows = await fetch_users()
    await send_excel_to_admin(message.bot, rows, filename="users.xlsx")
    await message.answer("✅ فایل اکسل ارسال شد.")


# -------------------- Main --------------------
async def main():
    ensure_excel()
    await ensure_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
