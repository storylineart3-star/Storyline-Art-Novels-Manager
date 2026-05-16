import logging
import os
import re
import shlex
import sqlite3
from datetime import datetime
from typing import Optional, List, Tuple
from urllib.parse import urljoin

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = list(map(int, os.environ.get("ADMIN_IDS", "").split(",")))
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID"))
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
PORT = int(os.environ.get("PORT", 10000))

DB_PATH = "novels.db"

# Conversation states for manual add (IMAGE state now uses inline skip)
(NAME, AUTHOR, PLATFORM, CHANNEL, IMAGE) = range(5)

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Database helpers (unchanged)
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS novels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            normalized_name TEXT NOT NULL,
            original_name TEXT NOT NULL,
            author TEXT,
            platform TEXT,
            channel TEXT,
            file_id TEXT,
            chat_id INTEGER,
            message_id INTEGER,
            sender_id INTEGER,
            sender_name TEXT,
            date TEXT,
            added_by_index INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            novel_id INTEGER,
            word TEXT,
            FOREIGN KEY(novel_id) REFERENCES novels(id)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_words_word ON words(word)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_novels_normalized ON novels(normalized_name)")
    conn.commit()
    conn.close()

def add_novel(
    normalized_name: str,
    original_name: str,
    author: Optional[str],
    platform: Optional[str],
    channel: Optional[str],
    file_id: Optional[str],
    chat_id: int,
    message_id: int,
    sender_id: int,
    sender_name: str,
    added_by_index: int = 0,
):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    c.execute(
        """
        INSERT INTO novels (normalized_name, original_name, author, platform, channel,
                            file_id, chat_id, message_id, sender_id, sender_name, date, added_by_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (normalized_name, original_name, author, platform, channel, file_id, chat_id, message_id,
         sender_id, sender_name, now, added_by_index),
    )
    novel_id = c.lastrowid
    words = set(normalized_name.split())
    for w in words:
        if w.strip():
            c.execute("INSERT INTO words (novel_id, word) VALUES (?, ?)", (novel_id, w.strip()))
    conn.commit()
    conn.close()
    return novel_id

def get_exact_match(normalized_name: str) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM novels WHERE normalized_name = ? LIMIT 1", (normalized_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return dict(zip([col[0] for col in c.description], row))
    return None

def get_partial_matches(words: List[str]) -> List[dict]:
    if not words:
        return []
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    placeholders = ",".join(["?"] * len(words))
    query = f"""
        SELECT DISTINCT n.* FROM novels n
        JOIN words w ON n.id = w.novel_id
        WHERE w.word IN ({placeholders})
        ORDER BY n.date DESC
        LIMIT 5
    """
    c.execute(query, words)
    rows = c.fetchall()
    conn.close()
    return [dict(zip([col[0] for col in c.description], r)) for r in rows]

def search_novels(query: str, offset: int = 0, limit: int = 5) -> List[dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    param = f"%{query}%"
    c.execute(
        """
        SELECT * FROM novels
        WHERE original_name LIKE ? OR author LIKE ? OR channel LIKE ?
        ORDER BY date DESC LIMIT ? OFFSET ?
        """,
        (param, param, param, limit, offset),
    )
    rows = c.fetchall()
    conn.close()
    return [dict(zip([col[0] for col in c.description], r)) for r in rows]

def count_search(query: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    param = f"%{query}%"
    c.execute(
        "SELECT COUNT(*) FROM novels WHERE original_name LIKE ? OR author LIKE ? OR channel LIKE ?",
        (param, param, param),
    )
    total = c.fetchone()[0]
    conn.close()
    return total

def get_novels_filtered(
    author: Optional[str] = None,
    platform: Optional[str] = None,
    channel: Optional[str] = None,
    offset: int = 0,
    limit: int = 5,
) -> Tuple[List[dict], int]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    where = []
    params = []
    if author:
        where.append("author LIKE ?")
        params.append(f"%{author}%")
    if platform:
        where.append("platform = ?")
        params.append(platform)
    if channel:
        where.append("channel LIKE ?")
        params.append(f"%{channel}%")
    where_clause = " AND ".join(where) if where else "1=1"
    c.execute(f"SELECT COUNT(*) FROM novels WHERE {where_clause}", params)
    total = c.fetchone()[0]
    c.execute(
        f"SELECT * FROM novels WHERE {where_clause} ORDER BY date DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    )
    rows = c.fetchall()
    conn.close()
    return [dict(zip([col[0] for col in c.description], r)) for r in rows], total

def get_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM novels")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT author) FROM novels WHERE author IS NOT NULL")
    authors = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT platform) FROM novels WHERE platform IS NOT NULL")
    platforms = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT channel) FROM novels WHERE channel IS NOT NULL")
    channels = c.fetchone()[0]
    conn.close()
    return {"total": total, "authors": authors, "platforms": platforms, "channels": channels}

def delete_novel(novel_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM words WHERE novel_id = ?", (novel_id,))
    c.execute("DELETE FROM novels WHERE id = ?", (novel_id,))
    conn.commit()
    conn.close()

# ----------------------------------------------------------------------
# Utility functions
# ----------------------------------------------------------------------
def extract_author(full_caption: str) -> Tuple[str, Optional[str]]:
    patterns = [
        r'^(.*?)\s+[bB][yY]\s+(.+)$',
        r'^(.*?)\s+-\s+(.+)$',
        r'^(.*?)\s+\((.+)\)$',
    ]
    for pat in patterns:
        match = re.match(pat, full_caption.strip())
        if match:
            title = match.group(1).strip()
            author = match.group(2).strip()
            return title, author
    return full_caption.strip(), None

def normalize(name: str) -> str:
    return name.strip().lower()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def build_pagination_keyboard(current_page: int, total_pages: int, prefix: str) -> InlineKeyboardMarkup:
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"{prefix}_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_{current_page+1}"))
    return InlineKeyboardMarkup([buttons]) if buttons else None

# ----------------------------------------------------------------------
# Group automatic duplicate detection (unchanged)
# ----------------------------------------------------------------------
async def handle_group_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return
    if update.message.chat_id != GROUP_CHAT_ID:
        return

    caption = update.message.caption
    if not caption:
        await update.message.reply_text("⚠️ Please add the novel name in the caption!")
        return

    novel_name, author = extract_author(caption)
    norm_name = normalize(novel_name)

    photo = update.message.photo[-1]
    file_id = photo.file_id
    sender = update.message.from_user
    sender_name = sender.full_name
    sender_id = sender.id
    chat_id = update.message.chat_id
    message_id = update.message.message_id

    # 1. Exact match
    exact = get_exact_match(norm_name)
    if exact:
        await update.message.reply_text(
            f"❌ **This novel was already posted!**\n"
            f"Original by: {exact['sender_name']}\n"
            f"Date: {exact['date'][:10]}\n"
            f"Original post:",
            parse_mode=ParseMode.MARKDOWN,
        )
        try:
            await context.bot.forward_message(
                chat_id=GROUP_CHAT_ID,
                from_chat_id=exact["chat_id"],
                message_id=exact["message_id"],
            )
        except Exception as e:
            logger.error(f"Could not forward original: {e}")
        return

    # 2. Partial word match
    words = norm_name.split()
    partials = get_partial_matches(words)
    if partials:
        try:
            await context.bot.send_message(
                chat_id=sender_id,
                text="🔎 Similar novels found. I’ll send you the originals one by one.\n"
                     "Please tell me if any of them is the same.",
            )
            for p in partials:
                await context.bot.forward_message(
                    chat_id=sender_id,
                    from_chat_id=p["chat_id"],
                    message_id=p["message_id"],
                )
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("✅ Yes (same)", callback_data=f"same_{p['id']}_{message_id}"),
                            InlineKeyboardButton("❌ No", callback_data=f"diff_{p['id']}_{message_id}"),
                        ]
                    ]
                )
                await context.bot.send_message(
                    chat_id=sender_id,
                    text="Is this the same novel?",
                    reply_markup=keyboard,
                )
            await update.message.reply_text(
                "🔍 I found similar novels. I’ve DMed you the details.",
                reply_to_message_id=message_id,
            )
        except Exception as e:
            logger.warning(f"Could not DM user {sender_id}: {e}")
            await update.message.reply_text(
                "⚠️ Please start a private chat with me so I can DM you about similar novels.",
                reply_to_message_id=message_id,
            )
        # Still add the novel (not exact duplicate)
        add_novel(norm_name, novel_name, author, None, None, file_id, chat_id, message_id, sender_id, sender_name)
        return

    # 3. Safe
    add_novel(norm_name, novel_name, author, None, None, file_id, chat_id, message_id, sender_id, sender_name)
    await update.message.reply_text("✅ Novel saved. No repost detected.", reply_to_message_id=message_id)

# ----------------------------------------------------------------------
# Duplicate confirmation (enhanced – handles group and manual)
# ----------------------------------------------------------------------
async def button_same_diff_enhanced(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("same_") or data.startswith("diff_"):
        parts = data.split("_")
        action = parts[0]
        novel_id = int(parts[1])
        suffix = parts[2] if len(parts) > 2 else ""
        is_manual = (suffix == "manual")

        if action == "same":
            if is_manual:
                pending = context.user_data.get("pending_add")
                if pending:
                    await query.edit_message_text(
                        f"❌ Duplicate confirmed. The novel '{pending['original_name']}' will NOT be added."
                    )
                    context.user_data.pop("pending_add", None)
                else:
                    await query.edit_message_text("❌ Duplicate confirmed.")
            else:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT * FROM novels WHERE id = ?", (novel_id,))
                row = c.fetchone()
                conn.close()
                if row:
                    orig = dict(zip([col[0] for col in c.description], row))
                    try:
                        await context.bot.send_message(
                            chat_id=GROUP_CHAT_ID,
                            text=f"🚨 Duplicate confirmed by @{query.from_user.username}!\n"
                                 f"Original: {orig['original_name']} by {orig['sender_name']}",
                            reply_to_message_id=int(parts[2]),
                        )
                    except:
                        pass
                await query.edit_message_text("✅ Marked as duplicate. Warning sent to group.")
        else:  # diff_
            if is_manual:
                pending = context.user_data.get("pending_add")
                if pending:
                    if "partial_ids" in pending and novel_id in pending["partial_ids"]:
                        pending["partial_ids"].remove(novel_id)
                    if not pending.get("partial_ids"):
                        add_novel(
                            pending["norm_name"],
                            pending["original_name"],
                            pending["author"],
                            pending["platform"],
                            pending["channel"],
                            pending["file_id"],
                            pending["chat_id"],
                            pending["message_id"],
                            pending["sender_id"],
                            pending["sender_name"],
                        )
                        await query.edit_message_text(
                            f"✅ Novel added!\n"
                            f"• Name: {pending['original_name']}\n"
                            f"• Author: {pending['author'] or '—'}\n"
                            f"• Platform: {pending['platform'] or '—'}\n"
                            f"• Channel: {pending['channel']}\n"
                            f"• Date: {datetime.utcnow().strftime('%Y-%m-%d')}"
                        )
                        context.user_data.pop("pending_add", None)
                    else:
                        await query.edit_message_text("✅ Noted – not the same. Waiting for other confirmations...")
                else:
                    await query.edit_message_text("✅ Noted – this is a different novel.")
            else:
                await query.edit_message_text("✅ Noted – this is a different novel.")

# ----------------------------------------------------------------------
# Private chat – manual add (conversation) with inline skip button
# ----------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Novel", callback_data="add_novel")],
        [InlineKeyboardButton("🔍 Search", callback_data="search_menu")],
        [InlineKeyboardButton("❓ Help", callback_data="help_menu")],
    ])
    await update.message.reply_text("Welcome! Choose an option below:", reply_markup=keyboard)

async def help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = (
        "📚 **Novel Repost Guard**\n\n"
        "**Group:** Post a photo with the novel name. Bot checks for duplicates.\n"
        "**Private chat:** Use buttons to add novels or search.\n\n"
        "Admins: /admin for control panel."
    )
    await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)

# Entry point to add novel
async def add_novel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("📖 Send me the **novel name**:", parse_mode=ParseMode.MARKDOWN)
    return NAME

async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["novel_name"] = update.message.text
    await update.message.reply_text("✍️ Now send the **author name** (or /skip to leave blank):")
    return AUTHOR

async def add_author(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() != "/skip":
        context.user_data["author"] = update.message.text
    else:
        context.user_data["author"] = None
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Pratilipi", callback_data="plat_Pratilipi")],
        [InlineKeyboardButton("Pocket Novel", callback_data="plat_Pocket Novel")],
        [InlineKeyboardButton("Other", callback_data="plat_Other")],
        [InlineKeyboardButton("Skip", callback_data="plat_Skip")],
    ])
    await update.message.reply_text("🌐 Choose **platform**:", reply_markup=keyboard)
    return PLATFORM

async def add_platform_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plat = query.data.split("_", 1)[1]
    if plat == "Skip":
        context.user_data["platform"] = None
    else:
        context.user_data["platform"] = plat
    await query.edit_message_text("📺 Now send the **channel name** (YouTube channel):")
    return CHANNEL

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel = update.message.text.strip()
    context.user_data["channel"] = channel
    # New: inline buttons for skip/cancel
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data="skip_image"),
         InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await update.message.reply_text("🖼️ Send a **photo**, or use the buttons below:", reply_markup=keyboard)
    return IMAGE

async def add_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Photo received
    photo = update.message.photo[-1]
    context.user_data["file_id"] = photo.file_id
    await update.message.reply_text("✅ Photo saved. Now checking for duplicates...")
    return await finalize_add(update, context)

async def add_image_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Skip button pressed
    query = update.callback_query
    await query.answer()
    context.user_data["file_id"] = None
    await query.edit_message_text("Skipped photo. Checking for duplicates...")
    return await finalize_add(update, context)

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Cancel button or /cancel command
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("❌ Add cancelled.")
    else:
        await update.message.reply_text("❌ Add cancelled.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

async def finalize_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = context.user_data
    name = user_data["novel_name"]
    author = user_data.get("author")
    platform = user_data.get("platform")
    channel = user_data["channel"]
    file_id = user_data.get("file_id")
    norm_name = normalize(name)
    sender = update.effective_user
    sender_name = sender.full_name
    sender_id = sender.id
    chat_id = update.effective_chat.id
    # use the message that triggered the step (photo or skip message)
    message_id = update.effective_message.message_id

    # Duplicate check: exact match first
    exact = get_exact_match(norm_name)
    if exact:
        await update.effective_message.reply_text(
            f"❌ This novel **already exists** in the database!\n"
            f"Name: {exact['original_name']}\n"
            f"Added by: {exact['sender_name']} on {exact['date'][:10]}",
            parse_mode=ParseMode.MARKDOWN,
        )
        if exact.get("file_id"):
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=exact["file_id"],
                    caption=f"Original post (ID {exact['id']})"
                )
            except:
                pass
        return ConversationHandler.END

    # Partial word match
    words = norm_name.split()
    partials = get_partial_matches(words)
    if partials:
        for p in partials:
            try:
                if p.get("file_id"):
                    await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=p["file_id"],
                        caption=f"Similar novel (ID {p['id']}): {p['original_name']}"
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Similar novel (ID {p['id']}): {p['original_name']} (no image)"
                    )
            except:
                pass
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("✅ Yes (same)", callback_data=f"same_{p['id']}_manual"),
                        InlineKeyboardButton("❌ No (different)", callback_data=f"diff_{p['id']}_manual"),
                    ]
                ]
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text="Is this the same novel?",
                reply_markup=keyboard,
            )
        await update.effective_message.reply_text(
            "🔎 I found similar novels above. Please use the buttons to confirm if any are the same.\n"
            "If you click **No** for all, the novel will be added automatically after the last confirmation.\n"
            "You can also /cancel to abort."
        )
        context.user_data["pending_add"] = {
            "norm_name": norm_name,
            "original_name": name,
            "author": author,
            "platform": platform,
            "channel": channel,
            "file_id": file_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "partial_ids": [p["id"] for p in partials],
        }
        return ConversationHandler.END

    # No matches at all, safe to add
    add_novel(norm_name, name, author, platform, channel, file_id, chat_id, message_id, sender_id, sender_name)
    await update.effective_message.reply_text(
        f"✅ Novel added!\n"
        f"• Name: {name}\n"
        f"• Author: {author or '—'}\n"
        f"• Platform: {platform or '—'}\n"
        f"• Channel: {channel}\n"
        f"• Date: {datetime.utcnow().strftime('%Y-%m-%d')}"
    )
    return ConversationHandler.END

# ----------------------------------------------------------------------
# Search functionality (unchanged)
# ----------------------------------------------------------------------
async def search_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔎 Send me a search query (novel name, author, or channel):")
    context.user_data["awaiting_search"] = True

async def handle_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_search"):
        return
    query_text = update.message.text.strip()
    context.user_data["search_query"] = query_text
    context.user_data["search_offset"] = 0
    await show_search_results(update, context, query_text, 0)

async def show_search_results(update: Update, context, query: str, offset: int):
    limit = 5
    novels = search_novels(query, offset, limit)
    total = count_search(query)
    if not novels:
        await update.message.reply_text("No novels found.")
        context.user_data["awaiting_search"] = False
        return
    text = f"🔍 Results for *{query}* ({offset+1}-{min(offset+limit, total)} of {total}):\n\n"
    for i, n in enumerate(novels, start=1):
        text += (
            f"*{i+offset}.* {n['original_name']}\n"
            f"   Author: {n['author'] or '—'} | Platform: {n['platform'] or '—'}\n"
            f"   Channel: {n['channel'] or '—'}\n"
            f"   Date: {n['date'][:10]}\n\n"
        )
    total_pages = (total + limit - 1) // limit
    current_page = offset // limit
    keyboard = build_pagination_keyboard(current_page, total_pages, "search_page")
    if keyboard:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    context.user_data["awaiting_search"] = False

async def search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])
    query_text = context.user_data.get("search_query", "")
    offset = page * 5
    context.user_data["search_offset"] = offset
    await query.edit_message_text("Loading...")
    await show_search_results(update, context, query_text, offset)

# ----------------------------------------------------------------------
# Admin commands (unchanged)
# ----------------------------------------------------------------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 List novels", callback_data="admin_list")],
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔍 Filtered search", callback_data="admin_filter")],
        [InlineKeyboardButton("🗑 Delete (by ID)", callback_data="admin_delete_prompt")],
    ])
    await update.message.reply_text("🔧 Admin Panel:", reply_markup=keyboard)

async def admin_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["admin_list_offset"] = 0
    context.user_data["admin_list_filters"] = {}
    await show_admin_list(update, context)

async def show_admin_list(update: Update, context):
    offset = context.user_data.get("admin_list_offset", 0)
    filters = context.user_data.get("admin_list_filters", {})
    novels, total = get_novels_filtered(
        author=filters.get("author"),
        platform=filters.get("platform"),
        channel=filters.get("channel"),
        offset=offset,
        limit=5,
    )
    if not novels:
        await update.callback_query.edit_message_text("No novels found.")
        return
    text = f"📋 Novels (filtered) – page {offset//5 + 1}\n\n"
    for i, n in enumerate(novels, start=1):
        text += (
            f"`{n['id']}`: {n['original_name']}\n"
            f"   Author: {n['author'] or '—'} | Platform: {n['platform'] or '—'}\n"
            f"   Channel: {n['channel'] or '—'} | {n['date'][:10]}\n\n"
        )
    total_pages = (total + 4) // 5
    current_page = offset // 5
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_page_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_page_{current_page+1}"))
    buttons.append(InlineKeyboardButton("🔧 Set filter", callback_data="admin_filter"))
    keyboard = InlineKeyboardMarkup([buttons])
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

async def admin_list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])
    context.user_data["admin_list_offset"] = page * 5
    await show_admin_list(update, context)

async def admin_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "Send filter in format:\n"
        "`author:Name`\n`platform:Pratilipi`\n`channel:Channel`\n"
        "You can combine them with spaces (e.g., `author:John platform:Pratilipi`)\n"
        "Or `/skip` to clear all filters.",
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data["awaiting_filter"] = True

async def handle_filter_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_filter"):
        return
    text = update.message.text.strip()
    if text.lower() == "/skip":
        context.user_data["admin_list_filters"] = {}
        await update.message.reply_text("Filters cleared.")
        context.user_data["awaiting_filter"] = False
        await admin_panel(update, context)
        return
    filters = {}
    for part in shlex.split(text):
        if ":" in part:
            key, value = part.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key in ("author", "platform", "channel"):
                filters[key] = value
    context.user_data["admin_list_filters"] = filters
    context.user_data["admin_list_offset"] = 0
    context.user_data["awaiting_filter"] = False
    await update.message.reply_text("Filters applied. Showing list:")
    await show_admin_list(update, context)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s = get_stats()
    text = (
        f"📊 *Statistics*\n"
        f"Total novels: {s['total']}\n"
        f"Distinct authors: {s['authors']}\n"
        f"Platforms used: {s['platforms']}\n"
        f"Channels: {s['channels']}"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)

async def admin_delete_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Send the ID of the novel to delete:")
    context.user_data["awaiting_delete_id"] = True

async def handle_delete_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_delete_id"):
        return
    try:
        novel_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("Invalid ID.")
        context.user_data["awaiting_delete_id"] = False
        return
    delete_novel(novel_id)
    await update.message.reply_text(f"✅ Novel {novel_id} deleted.")
    context.user_data["awaiting_delete_id"] = False

# ----------------------------------------------------------------------
# Indexing old posts (unchanged)
# ----------------------------------------------------------------------
async def index_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    context.user_data["index_mode"] = True
    await update.message.reply_text(
        "📥 Index mode ON. Forward old group posts (with photo) to me privately.\n"
        "Send /stopindex when done."
    )

async def stop_index(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["index_mode"] = False
    await update.message.reply_text("Index mode OFF.")

async def handle_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("index_mode", False):
        return
    msg = update.message
    if not msg.forward_origin:
        await msg.reply_text("Not a forwarded message.")
        return
    if hasattr(msg.forward_origin, "chat"):
        if msg.forward_origin.chat.id != GROUP_CHAT_ID:
            await msg.reply_text("This forward is not from our group.")
            return
    else:
        await msg.reply_text("Cannot determine original chat.")
        return
    if not msg.photo:
        await msg.reply_text("Please forward only photo messages.")
        return
    caption = msg.caption or ""
    novel_name, author = extract_author(caption)
    norm_name = normalize(novel_name)
    file_id = msg.photo[-1].file_id
    add_novel(
        normalized_name=norm_name,
        original_name=novel_name,
        author=author,
        platform=None,
        channel=None,
        file_id=file_id,
        chat_id=GROUP_CHAT_ID,
        message_id=msg.message_id,
        sender_id=update.effective_user.id,
        sender_name="indexed",
        added_by_index=1,
    )
    await msg.reply_text(f"✅ Indexed: {novel_name}")

# ----------------------------------------------------------------------
# Broadcast (unchanged)
# ----------------------------------------------------------------------
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    text = " ".join(context.args)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT sender_id FROM novels WHERE sender_id IS NOT NULL")
    rows = c.fetchall()
    conn.close()
    sent = 0
    for (uid,) in rows:
        try:
            await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast:\n{text}")
            sent += 1
        except:
            pass
    await update.message.reply_text(f"Broadcast sent to {sent} users.")

# ----------------------------------------------------------------------
# Main application
# ----------------------------------------------------------------------
def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Conversation handler for manual add (now with inline skip button)
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_novel_start, pattern="^add_novel$")],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            AUTHOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_author)],
            PLATFORM: [CallbackQueryHandler(add_platform_callback, pattern="^plat_")],
            CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_channel)],
            IMAGE: [
                MessageHandler(filters.PHOTO, add_image),
                CallbackQueryHandler(add_image_skip, pattern="^skip_image$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_add)],
    )

    # Register all handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(help_menu, pattern="^help_menu$"))
    app.add_handler(CallbackQueryHandler(search_menu, pattern="^search_menu$"))
    app.add_handler(CallbackQueryHandler(search_page_callback, pattern="^search_page_"))
    # Duplicate confirmation
    app.add_handler(CallbackQueryHandler(button_same_diff_enhanced, pattern="^(same_|diff_)"))
    # Admin panel
    app.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin$"))
    app.add_handler(CallbackQueryHandler(admin_list_callback, pattern="^admin_list$"))
    app.add_handler(CallbackQueryHandler(admin_list_page_callback, pattern="^admin_list_page_"))
    app.add_handler(CallbackQueryHandler(admin_filter_callback, pattern="^admin_filter$"))
    app.add_handler(CallbackQueryHandler(admin_stats, pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(admin_delete_prompt, pattern="^admin_delete_prompt$"))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("index", index_cmd))
    app.add_handler(CommandHandler("stopindex", stop_index))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(MessageHandler(filters.PHOTO & filters.Chat(GROUP_CHAT_ID), handle_group_photo))
    app.add_handler(MessageHandler(filters.FORWARDED & filters.ChatType.PRIVATE, handle_forward))
    # Text handlers for search, filter, delete ID (private chat)
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_search_text))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_filter_text))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_delete_id))

    # Start webhook
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=urljoin(WEBHOOK_URL, "webhook"),
    )

if __name__ == "__main__":
    main()