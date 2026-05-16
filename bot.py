import asyncio
import logging
import os
import re
import shlex
import sqlite3
import csv
import io
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any
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
from telegram.helpers import escape_markdown

import motor.motor_asyncio
from bson import ObjectId

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = list(map(int, os.environ.get("ADMIN_IDS", "").split(","))) if os.environ.get("ADMIN_IDS") else []
SUPER_ADMIN_ID = int(os.environ.get("SUPER_ADMIN_ID", 0))
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID"))
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
PORT = int(os.environ.get("PORT", 10000))
STORAGE_GROUP_ID = int(os.environ.get("STORAGE_GROUP_ID")) if os.environ.get("STORAGE_GROUP_ID") else None

MONGO_URI = os.environ.get("MONGO_URI")
DB_PATH = "novels.db"

# Conversation states (Reordered per request)
(NAME, AUTHOR, IMAGE, PLATFORM, CHANNEL, STORY_NAME) = range(6)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Database abstraction layer
# ----------------------------------------------------------------------
class Database:
    def __init__(self):
        self.mongo = None
        self.db = None
        self.fallback = False
        self.sqlite_path = DB_PATH

    async def initialize(self):
        if MONGO_URI:
            try:
                self.mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
                await self.mongo.admin.command("ping")
                self.db = self.mongo.novelbot
                logger.info("Connected to MongoDB Atlas")
                await self.db.novels.create_index("normalized_name", unique=True)
                await self.db.novels.create_index([("words", 1)])
                await self.db.novels.create_index([("date", -1)])
                await self.db.admins.create_index("user_id", unique=True)
                return
            except Exception as e:
                logger.warning(f"MongoDB failed: {e}. Falling back to SQLite.")
        self.fallback = True
        self._init_sqlite()
        logger.info("Using SQLite fallback")

    def _init_sqlite(self):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS novels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                normalized_name TEXT NOT NULL,
                original_name TEXT NOT NULL,
                author TEXT,
                platform TEXT,
                channel TEXT,
                story_name TEXT,
                file_id TEXT,
                chat_id INTEGER,
                message_id INTEGER,
                storage_chat_id INTEGER,
                storage_message_id INTEGER,
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
        c.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            )
        """)
        conn.commit()
        conn.close()

    async def is_admin(self, user_id: int) -> bool:
        if user_id in ADMIN_IDS or user_id == SUPER_ADMIN_ID:
            return True
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_is_admin, user_id)
        else:
            doc = await self.db.admins.find_one({"user_id": user_id})
            return doc is not None

    def _sqlite_is_admin(self, user_id: int) -> bool:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
        res = c.fetchone()
        conn.close()
        return res is not None

    async def add_admin(self, user_id: int):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_add_admin, user_id)
        else:
            await self.db.admins.update_one({"user_id": user_id}, {"$set": {"user_id": user_id}}, upsert=True)

    def _sqlite_add_admin(self, user_id: int):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()

    async def remove_admin(self, user_id: int):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_remove_admin, user_id)
        else:
            await self.db.admins.delete_one({"user_id": user_id})

    def _sqlite_remove_admin(self, user_id: int):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()

    async def add_novel(self, normalized_name, original_name, author, platform, channel, story_name,
                        file_id, chat_id, message_id, storage_chat_id, storage_message_id,
                        sender_id, sender_name, added_by_index=0):
        now = datetime.utcnow().isoformat()
        words = normalized_name.split()
        if self.fallback:
            await asyncio.to_thread(
                self._sqlite_add_novel,
                normalized_name, original_name, author, platform, channel, story_name,
                file_id, chat_id, message_id, storage_chat_id, storage_message_id,
                sender_id, sender_name, now, added_by_index, words
            )
        else:
            doc = {
                "normalized_name": normalized_name,
                "original_name": original_name,
                "author": author,
                "platform": platform,
                "channel": channel,
                "story_name": story_name,
                "file_id": file_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "storage_chat_id": storage_chat_id,
                "storage_message_id": storage_message_id,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "date": now,
                "added_by_index": added_by_index,
                "words": words
            }
            await self.db.novels.insert_one(doc)

    def _sqlite_add_novel(self, normalized_name, original_name, author, platform, channel, story_name,
                          file_id, chat_id, message_id, storage_chat_id, storage_message_id,
                          sender_id, sender_name, date, added_by_index, words):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO novels (normalized_name, original_name, author, platform, channel, story_name,
                                file_id, chat_id, message_id, storage_chat_id, storage_message_id,
                                sender_id, sender_name, date, added_by_index)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (normalized_name, original_name, author, platform, channel, story_name,
              file_id, chat_id, message_id, storage_chat_id, storage_message_id,
              sender_id, sender_name, date, added_by_index))
        novel_id = c.lastrowid
        for w in words:
            if w.strip():
                c.execute("INSERT INTO words (novel_id, word) VALUES (?, ?)", (novel_id, w.strip()))
        conn.commit()
        conn.close()

    async def get_exact_match(self, normalized_name: str) -> Optional[Dict]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_exact_match, normalized_name)
        else:
            doc = await self.db.novels.find_one({"normalized_name": normalized_name})
            if doc:
                doc["id"] = str(doc["_id"])
            return doc

    def _sqlite_get_exact_match(self, normalized_name: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT * FROM novels WHERE normalized_name = ? LIMIT 1", (normalized_name,))
        row = c.fetchone()
        conn.close()
        if row:
            cols = [col[0] for col in c.description]
            return dict(zip(cols, row))
        return None

    async def get_partial_matches(self, words: List[str]) -> List[Dict]:
        if not words:
            return []
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_partial_matches, words)
        else:
            cursor = self.db.novels.find({"words": {"$in": words}}).sort("date", -1).limit(5)
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels

    def _sqlite_get_partial_matches(self, words: List[str]) -> List[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
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
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows]

    async def search_novels(self, query: str, offset=0, limit=5) -> List[Dict]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_search_novels, query, offset, limit)
        else:
            regex = re.compile(re.escape(query), re.IGNORECASE)
            cursor = self.db.novels.find(
                {"$or": [{"original_name": regex}, {"author": regex}, {"channel": regex}]}
            ).sort("date", -1).skip(offset).limit(limit)
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels

    def _sqlite_search_novels(self, query: str, offset: int, limit: int) -> List[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        param = f"%{query}%"
        c.execute("""
            SELECT * FROM novels
            WHERE original_name LIKE ? OR author LIKE ? OR channel LIKE ?
            ORDER BY date DESC LIMIT ? OFFSET ?
        """, (param, param, param, limit, offset))
        rows = c.fetchall()
        conn.close()
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows]

    async def count_search(self, query: str) -> int:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_count_search, query)
        else:
            regex = re.compile(re.escape(query), re.IGNORECASE)
            return await self.db.novels.count_documents({
                "$or": [{"original_name": regex}, {"author": regex}, {"channel": regex}]
            })

    def _sqlite_count_search(self, query: str) -> int:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        param = f"%{query}%"
        c.execute("SELECT COUNT(*) FROM novels WHERE original_name LIKE ? OR author LIKE ? OR channel LIKE ?",
                  (param, param, param))
        total = c.fetchone()[0]
        conn.close()
        return total

    async def get_novels_filtered(self, author=None, platform=None, channel=None, offset=0, limit=5) -> Tuple[List[Dict], int]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_novels_filtered, author, platform, channel, offset, limit)
        else:
            filt = {}
            if author:
                filt["author"] = {"$regex": re.escape(author), "$options": "i"}
            if platform:
                filt["platform"] = platform
            if channel:
                filt["channel"] = {"$regex": re.escape(channel), "$options": "i"}
            total = await self.db.novels.count_documents(filt)
            cursor = self.db.novels.find(filt).sort("date", -1).skip(offset).limit(limit)
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels, total

    def _sqlite_get_novels_filtered(self, author, platform, channel, offset, limit) -> Tuple[List[Dict], int]:
        conn = sqlite3.connect(self.sqlite_path)
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
        c.execute(f"SELECT * FROM novels WHERE {where_clause} ORDER BY date DESC LIMIT ? OFFSET ?",
                  params + [limit, offset])
        rows = c.fetchall()
        conn.close()
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows], total

    async def get_stats(self) -> Dict[str, int]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_stats)
        else:
            total = await self.db.novels.count_documents({})
            authors = len(await self.db.novels.distinct("author"))
            platforms = len(await self.db.novels.distinct("platform"))
            channels = len(await self.db.novels.distinct("channel"))
            return {"total": total, "authors": authors, "platforms": platforms, "channels": channels}

    def _sqlite_get_stats(self) -> Dict[str, int]:
        conn = sqlite3.connect(self.sqlite_path)
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

    async def delete_novel(self, novel_id: str):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_delete_novel, int(novel_id))
        else:
            await self.db.novels.delete_one({"_id": ObjectId(novel_id)})

    def _sqlite_delete_novel(self, novel_id: int):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("DELETE FROM words WHERE novel_id = ?", (novel_id,))
        c.execute("DELETE FROM novels WHERE id = ?", (novel_id,))
        conn.commit()
        conn.close()

db = Database()
# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def extract_author(full_caption: str) -> Tuple[str, Optional[str]]:
    if not full_caption:
        return "", None
    first_line = full_caption.strip().split('\n')[0]
    # Catches: Title by Author, Title written by Author, Title - Author, Title (Author)
    patterns = [
        r'^(.*?)\s+(?:written\s+by|by|-)\s+(.+)$',
        r'^(.*?)\s+\((.+)\)$',
    ]
    for pat in patterns:
        match = re.match(pat, first_line, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    return first_line.strip(), None

def normalize(name: str) -> str:
    return name.strip().lower()

async def is_admin(user_id: int) -> bool:
    return await db.is_admin(user_id)

async def is_super_admin(user_id: int) -> bool:
    return user_id == SUPER_ADMIN_ID

def escape_mdv2(text: str) -> str:
    return escape_markdown(text, version=2)

def build_pagination_keyboard(current_page: int, total_pages: int, prefix: str) -> InlineKeyboardMarkup:
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"{prefix}_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_{current_page+1}"))
    return InlineKeyboardMarkup([buttons]) if buttons else None

async def forward_to_storage(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    if not STORAGE_GROUP_ID:
        return None, None
    try:
        msg = await context.bot.forward_message(
            chat_id=STORAGE_GROUP_ID,
            from_chat_id=chat_id,
            message_id=message_id
        )
        return msg.chat_id, msg.message_id
    except Exception as e:
        logger.error(f"Failed to forward to storage: {e}")
        return None, None

async def send_rich_duplicate_check(context: ContextTypes.DEFAULT_TYPE, chat_id: int, p: dict, msg_id_suffix: str):
    text = f"📚 *Title:* {escape_mdv2(p['original_name'])}\n"
    text += f"👤 *Author:* {escape_mdv2(p.get('author') or 'Unknown')}\n"
    text += f"🌐 *Platform:* {escape_mdv2(p.get('platform') or 'Unknown')}"
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes (same)", callback_data=f"same_{p['id']}_{msg_id_suffix}"),
        InlineKeyboardButton("❌ No", callback_data=f"diff_{p['id']}_{msg_id_suffix}")
    ]])
    
    if p.get('file_id'):
        await context.bot.send_photo(chat_id=chat_id, photo=p['file_id'], caption=text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
    else:
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

# ----------------------------------------------------------------------
# Group automatic detection
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

    exact = await db.get_exact_match(norm_name)
    if exact:
        orig_name_esc = escape_mdv2(exact['original_name'])
        sender_esc = escape_mdv2(exact['sender_name'])
        await update.message.reply_text(
            f"❌ *This novel was already posted!*\nOriginal by: {sender_esc}\nDate: {exact['date'][:10]}\nOriginal post:",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        fwd_chat_id = exact.get('storage_chat_id') or exact.get('chat_id')
        fwd_msg_id = exact.get('storage_message_id') or exact.get('message_id')
        if fwd_chat_id and fwd_msg_id:
            try:
                await context.bot.forward_message(chat_id=GROUP_CHAT_ID, from_chat_id=fwd_chat_id, message_id=fwd_msg_id)
            except Exception as e:
                logger.error(f"Could not forward original: {e}")
        return

    words = norm_name.split()
    partials = await db.get_partial_matches(words)
    if partials:
        try:
            await context.bot.send_message(chat_id=sender_id,
                                           text="🔎 Similar novels found. Please check if any of these match what you just posted:")
            for p in partials:
                await send_rich_duplicate_check(context, sender_id, p, str(message_id))
            await update.message.reply_text("🔍 I found similar novels. I’ve DMed you the details to confirm.",
                                            reply_to_message_id=message_id)
        except Exception as e:
            logger.warning(f"Could not DM user {sender_id}: {e}")
            await update.message.reply_text("⚠️ Please start a private chat with me so I can DM you about similar novels.",
                                            reply_to_message_id=message_id)
        
        storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
        await db.add_novel(norm_name, novel_name, author, None, None, None,
                           file_id, chat_id, message_id, storage_chat_id, storage_msg_id,
                           sender_id, sender_name)
        return

    storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
    await db.add_novel(norm_name, novel_name, author, None, None, None,
                       file_id, chat_id, message_id, storage_chat_id, storage_msg_id,
                       sender_id, sender_name)
    try:
        await update.message.set_reaction(reaction="👍")
    except Exception:
        pass
    await update.message.reply_text("✅ Novel saved. No repost detected.", reply_to_message_id=message_id, disable_notification=True)

# ----------------------------------------------------------------------
# Duplicate confirmation callback
# ----------------------------------------------------------------------
async def button_same_diff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not (data.startswith("same_") or data.startswith("diff_")):
        return

    parts = data.split("_")
    action = parts[0]
    novel_id = parts[1]
    suffix = parts[2] if len(parts) > 2 else ""
    is_manual = (suffix == "manual")

    if action == "same":
        if is_manual:
            pending = context.user_data.get("pending_add")
            if pending:
                await query.edit_message_caption(f"❌ Duplicate confirmed. The novel '{escape_mdv2(pending['original_name'])}' will NOT be added.") if query.message.photo else await query.edit_message_text(f"❌ Duplicate confirmed. The novel '{escape_mdv2(pending['original_name'])}' will NOT be added.")
                context.user_data.pop("pending_add", None)
            else:
                await query.edit_message_caption("❌ Duplicate confirmed.") if query.message.photo else await query.edit_message_text("❌ Duplicate confirmed.")
        else:
            exact = await db.get_exact_match(novel_id) if not novel_id.isdigit() else None
            if exact:
                orig_name_esc = escape_mdv2(exact['original_name'])
                sender_esc = escape_mdv2(exact['sender_name'])
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=f"🚨 Duplicate confirmed by @{query.from_user.username}!\nOriginal: {orig_name_esc} by {sender_esc}",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_to_message_id=int(suffix) if suffix.isdigit() else None
                )
            await query.edit_message_caption("✅ Marked as duplicate. Warning sent to group.") if query.message.photo else await query.edit_message_text("✅ Marked as duplicate. Warning sent to group.")
    else:  # diff
        if is_manual:
            pending = context.user_data.get("pending_add")
            if pending:
                if "partial_ids" in pending and novel_id in pending["partial_ids"]:
                    pending["partial_ids"].remove(novel_id)
                if not pending.get("partial_ids"):
                    await db.add_novel(
                        pending["norm_name"], pending["original_name"], pending["author"],
                        pending["platform"], pending["channel"], pending["story_name"],
                        pending.get("file_id"), pending["chat_id"], pending["message_id"],
                        pending.get("storage_chat_id"), pending.get("storage_message_id"),
                        pending["sender_id"], pending["sender_name"]
                    )
                    success_msg = (f"✅ Novel added!\n"
                                   f"• Name: {escape_mdv2(pending['original_name'])}\n"
                                   f"• Author: {escape_mdv2(pending['author'] or '—')}\n"
                                   f"• Platform: {escape_mdv2(pending['platform'] or '—')}\n"
                                   f"• Channel: {escape_mdv2(pending['channel'] or '—')}\n"
                                   f"• Story: {escape_mdv2(pending['story_name'] or '—')}\n"
                                   f"• Date: {datetime.utcnow().strftime('%Y-%m-%d')}")
                    await query.edit_message_caption(success_msg, parse_mode=ParseMode.MARKDOWN_V2) if query.message.photo else await query.edit_message_text(success_msg, parse_mode=ParseMode.MARKDOWN_V2)
                    context.user_data.pop("pending_add", None)
                else:
                    await query.edit_message_caption("✅ Noted – not the same. Waiting for other confirmations...") if query.message.photo else await query.edit_message_text("✅ Noted – not the same. Waiting for other confirmations...")
            else:
                await query.edit_message_caption("✅ Noted – this is a different novel.") if query.message.photo else await query.edit_message_text("✅ Noted – this is a different novel.")
        else:
            await query.edit_message_caption("✅ Noted – this is a different novel.") if query.message.photo else await query.edit_message_text("✅ Noted – this is a different novel.")

# ----------------------------------------------------------------------
# Private chat – start & help
# ----------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Novel", callback_data="add_novel")],
        [InlineKeyboardButton("🔍 Search", callback_data="search_menu")],
        [InlineKeyboardButton("❓ Help", callback_data="help_menu")],
    ])
    await update.message.reply_text("Welcome to Storyline Art Novels Manager! Choose an option below:", reply_markup=keyboard)

async def help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = (
        "📚 *Storyline Art Novels Manager*\n\n"
        "*Group:* Post a photo with the novel name. Bot checks for duplicates.\n"
        "*Private chat:* Use buttons to add novels or search.\n\n"
        "Admins: /admin for control panel.\n"
        "Super admin: /promote & /demote.\n"
        "Indexing: /index & /stopindex.\n\n"
        "Need Support? Contact: @GamingHommie"
    )
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start_menu")]])
    await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

async def start_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Novel", callback_data="add_novel")],
        [InlineKeyboardButton("🔍 Search", callback_data="search_menu")],
        [InlineKeyboardButton("❓ Help", callback_data="help_menu")],
    ])
    await query.edit_message_text("Welcome to Storyline Art Novels Manager! Choose an option below:", reply_markup=keyboard)

    # ----------------------------------------------------------------------
# Manual Add Conversation Flow
# ----------------------------------------------------------------------
async def add_novel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    await query.edit_message_text("📖 Please send me the *Novel Name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return NAME

async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["novel_name"] = update.message.text
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data="skip_author")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await update.message.reply_text("✍️ Great! Now send the *Author Name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return AUTHOR

async def process_author(update: Update, context: ContextTypes.DEFAULT_TYPE, author_val: Optional[str]):
    context.user_data["author"] = author_val
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip Image", callback_data="skip_image")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    msg = update.message or update.callback_query.message
    await msg.reply_text("🖼️ Now send a *Photo* for the novel:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return IMAGE

async def add_author_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_author(update, context, update.message.text)

async def skip_author_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await process_author(update, context, None)

async def process_image(update: Update, context: ContextTypes.DEFAULT_TYPE, file_id, chat_id, msg_id):
    context.user_data["file_id"] = file_id
    context.user_data["image_chat_id"] = chat_id
    context.user_data["image_message_id"] = msg_id
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Pratilipi", callback_data="plat_Pratilipi"), InlineKeyboardButton("Pocket Novel", callback_data="plat_Pocket Novel")],
        [InlineKeyboardButton("Webnovel", callback_data="plat_Webnovel"), InlineKeyboardButton("Other", callback_data="plat_Other")],
        [InlineKeyboardButton("⏭ Skip", callback_data="plat_Skip")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    msg = update.message or update.callback_query.message
    await msg.reply_text("🌐 Choose the *Platform*:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    return PLATFORM

async def add_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo = update.message.photo[-1]
    return await process_image(update, context, photo.file_id, update.message.chat_id, update.message.message_id)

async def skip_image_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await process_image(update, context, None, None, None)

async def add_platform_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plat = query.data.split("_", 1)[1]
    context.user_data["platform"] = plat if plat != "Skip" else None
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data="skip_channel")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await query.edit_message_text("📺 Send the *YouTube Channel Name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return CHANNEL

async def process_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_val: Optional[str]):
    context.user_data["channel"] = channel_val
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data="skip_story")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    msg = update.message or update.callback_query.message
    await msg.reply_text("📝 Finally, send the *Story Name* on that channel:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return STORY_NAME

async def add_channel_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_channel(update, context, update.message.text.strip())

async def skip_channel_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await process_channel(update, context, None)

async def process_story(update: Update, context: ContextTypes.DEFAULT_TYPE, story_val: Optional[str]):
    context.user_data["story_name"] = story_val
    msg = update.message or update.callback_query.message
    await msg.reply_text("🔍 Checking for duplicates...")
    return await finalize_add(update, context, msg)

async def add_story_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await process_story(update, context, update.message.text.strip())

async def skip_story_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await process_story(update, context, None)

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("❌ Adding process cancelled.")
    else:
        await update.message.reply_text("❌ Adding process cancelled.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

async def finalize_add(update: Update, context: ContextTypes.DEFAULT_TYPE, msg_obj):
    user_data = context.user_data
    name = user_data["novel_name"]
    author = user_data.get("author")
    platform = user_data.get("platform")
    channel = user_data.get("channel")
    story_name = user_data.get("story_name")
    file_id = user_data.get("file_id")
    norm_name = normalize(name)
    sender = update.effective_user
    sender_name = sender.full_name
    sender_id = sender.id
    chat_id = update.effective_chat.id
    message_id = msg_obj.message_id

    storage_chat_id, storage_msg_id = None, None
    if user_data.get("image_chat_id") and user_data.get("image_message_id"):
        storage_chat_id, storage_msg_id = await forward_to_storage(
            context, user_data["image_chat_id"], user_data["image_message_id"]
        )

    exact = await db.get_exact_match(norm_name)
    if exact:
        orig_name_esc = escape_mdv2(exact['original_name'])
        sender_esc = escape_mdv2(exact['sender_name'])
        await msg_obj.reply_text(
            f"❌ This novel *already exists* in the database!\n"
            f"Name: {orig_name_esc}\nAdded by: {sender_esc} on {exact['date'][:10]}",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        if exact.get("file_id"):
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=exact["file_id"],
                                             caption=f"Original post (ID {exact['id']})")
            except:
                pass
        return ConversationHandler.END

    words = norm_name.split()
    partials = await db.get_partial_matches(words)
    if partials:
        for p in partials:
            await send_rich_duplicate_check(context, chat_id, p, "manual")
            
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Add", callback_data="cancel_add")]])
        await msg_obj.reply_text(
            "🔎 I found similar novels above. Please confirm if any are the same.\n"
            "If you click *No* for all, the novel will be added automatically.",
            parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb
        )
        context.user_data["pending_add"] = {
            "norm_name": norm_name,
            "original_name": name,
            "author": author,
            "platform": platform,
            "channel": channel,
            "story_name": story_name,
            "file_id": file_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "storage_chat_id": storage_chat_id,
            "storage_message_id": storage_msg_id,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "partial_ids": [p["id"] for p in partials],
        }
        return ConversationHandler.END

    await db.add_novel(norm_name, name, author, platform, channel, story_name,
                       file_id, chat_id, message_id, storage_chat_id, storage_msg_id,
                       sender_id, sender_name)
    await msg_obj.reply_text(
        f"✅ Novel added!\n"
        f"• Name: {escape_mdv2(name)}\n"
        f"• Author: {escape_mdv2(author or '—')}\n"
        f"• Platform: {escape_mdv2(platform or '—')}\n"
        f"• Channel: {escape_mdv2(channel or '—')}\n"
        f"• Story: {escape_mdv2(story_name or '—')}\n"
        f"• Date: {datetime.utcnow().strftime('%Y-%m-%d')}",
        parse_mode=ParseMode.MARKDOWN_V2
    )
    return ConversationHandler.END

# ----------------------------------------------------------------------
# Central Private Message Router & Search/Filter functions
# ----------------------------------------------------------------------
async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ud = context.user_data
    msg = update.message

    # --- 1. Index Mode Routing ---
    if ud.get("index_mode"):
        pending = ud.get("pending_index")
        if msg.photo:
            caption = msg.caption or ""
            first_line = caption.split('\n')[0] if caption else ""
            if first_line:
                await process_indexed_novel(update, context, first_line, msg)
            else:
                if pending and pending["type"] == "text":
                    await process_indexed_novel(update, context, pending["caption"], msg)
                    ud["pending_index"] = None
                else:
                    ud["pending_index"] = {"type": "photo", "message": msg}
                    await msg.reply_text("📸 Photo received! Now type or forward the novel name.")
            return
        elif msg.text:
            if pending and pending["type"] == "photo":
                await process_indexed_novel(update, context, msg.text, pending["message"])
                ud["pending_index"] = None
            else:
                ud["pending_index"] = {"type": "text", "caption": msg.text}
                await msg.reply_text("📝 Novel name noted! Now forward the photo.")
            return

    # --- 2. Standard State Routing ---
    if not msg.text:
        return

    if ud.get("awaiting_search"):
        await handle_search_text(update, context)
    elif ud.get("awaiting_filter"):
        await handle_filter_text(update, context)
    elif ud.get("awaiting_delete_id"):
        await handle_delete_id(update, context)

async def search_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start_menu")]])
    await query.edit_message_text("🔎 Send me a search query (novel name, author, or channel):", reply_markup=kb)
    context.user_data["awaiting_search"] = True

async def handle_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.strip()
    context.user_data["search_query"] = query_text
    context.user_data["search_offset"] = 0
    await show_search_results(update, context, query_text, 0)

async def show_search_results(update: Update, context, query: str, offset: int):
    limit = 5
    novels = await db.search_novels(query, offset, limit)
    total = await db.count_search(query)
    user_id = update.effective_user.id
    admin = await is_admin(user_id)

    if not novels:
        await update.message.reply_text("No novels found.")
        context.user_data["awaiting_search"] = False
        return

    text = f"🔍 Results for *{escape_mdv2(query)}* ({offset+1}\\-{min(offset+limit, total)} of {total}):\n\n"
    for i, n in enumerate(novels, start=1):
        name_esc = escape_mdv2(n['original_name'])
        author_esc = escape_mdv2(n['author'] or '—')
        plat_esc = escape_mdv2(n['platform'] or '—')
        date_esc = n['date'][:10] if n.get('date') else '—'
        text += f"*{i+offset}.* {name_esc}\n   Author: {author_esc} \\| Platform: {plat_esc}\n"
        if admin:
            chan_esc = escape_mdv2(n.get('channel') or '—')
            story_esc = escape_mdv2(n.get('story_name') or '—')
            text += f"   Channel: {chan_esc} \\| Story: {story_esc}\n"
        text += f"   Date: {date_esc}\n\n"

    total_pages = (total + limit - 1) // limit
    current_page = offset // limit
    keyboard = build_pagination_keyboard(current_page, total_pages, "search_page")
    if keyboard:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
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
# Admin panel & listing
# ----------------------------------------------------------------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        if update.message: await update.message.reply_text("⛔ Admin only.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 List novels", callback_data="admin_list")],
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔍 Filtered search", callback_data="admin_filter")],
        [InlineKeyboardButton("🗑 Delete (by ID)", callback_data="admin_delete_prompt")],
        [InlineKeyboardButton("🔙 Close Admin", callback_data="start_menu")]
    ])
    text = "🔧 Admin Panel:\n(Pro tip: Type /export to download a database CSV)"
    if update.message:
        await update.message.reply_text(text, reply_markup=keyboard)
    else:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard)

async def admin_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["admin_list_offset"] = 0
    context.user_data["admin_list_filters"] = {}
    await show_admin_list(update, context)

async def show_admin_list(update: Update, context):
    offset = context.user_data.get("admin_list_offset", 0)
    filters = context.user_data.get("admin_list_filters", {})
    novels, total = await db.get_novels_filtered(
        author=filters.get("author"),
        platform=filters.get("platform"),
        channel=filters.get("channel"),
        offset=offset, limit=5
    )
    if not novels:
        await update.callback_query.edit_message_text("No novels found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]]))
        return
    text = f"📋 Novels (filtered) – page {offset//5 + 1}\n\n"
    for n in novels:
        id_str = n.get('id') or str(n.get('_id'))
        name_esc = escape_mdv2(n['original_name'])
        author_esc = escape_mdv2(n['author'] or '—')
        plat_esc = escape_mdv2(n['platform'] or '—')
        chan_esc = escape_mdv2(n.get('channel') or '—')
        story_esc = escape_mdv2(n.get('story_name') or '—')
        date_esc = n['date'][:10] if n.get('date') else '—'
        text += (
            f"`{id_str}`: {name_esc}\n"
            f"   Author: {author_esc} \\| Platform: {plat_esc}\n"
            f"   Channel: {chan_esc} \\| Story: {story_esc}\n"
            f"   Date: {date_esc}\n\n"
        )
    total_pages = (total + 4) // 5
    current_page = offset // 5
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_page_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_page_{current_page+1}"))
    
    keyboard = [[InlineKeyboardButton("🔧 Set filter", callback_data="admin_filter")], buttons, [InlineKeyboardButton("🔙 Back", callback_data="admin")]]
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])
    context.user_data["admin_list_offset"] = page * 5
    await show_admin_list(update, context)

async def admin_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="admin")]])
    await query.edit_message_text(
        "Send filter in format:\n`author:Name`\n`platform:Pratilipi`\n`channel:Channel`\n"
        "Combine with spaces (e.g. `author:John platform:Pratilipi`)\n"
        "Or type /skip to clear all filters.",
        parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb
    )
    context.user_data["awaiting_filter"] = True

async def handle_filter_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    # We trigger the list view manually since we don't have a callback query here
    class FakeQuery:
         async def edit_message_text(self, *args, **kwargs):
             await update.message.reply_text(*args, **kwargs)
    update.callback_query = FakeQuery()
    await show_admin_list(update, context)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    s = await db.get_stats()
    text = f"📊 *Statistics*\nTotal novels: {s['total']}\nDistinct authors: {s['authors']}\nPlatforms used: {s['platforms']}\nChannels: {s['channels']}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]])
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)

async def admin_delete_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="admin")]])
    await query.edit_message_text("Send the ID of the novel to delete:", reply_markup=kb)
    context.user_data["awaiting_delete_id"] = True

async def handle_delete_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    novel_id = update.message.text.strip()
    await db.delete_novel(novel_id)
    await update.message.reply_text(f"✅ Novel {novel_id} deleted.")
    context.user_data["awaiting_delete_id"] = False
    await admin_panel(update, context)

# ----------------------------------------------------------------------
# Indexing (accepts any forwarded message)
# ----------------------------------------------------------------------
async def index_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    context.user_data["index_mode"] = True
    context.user_data["indexed_count"] = 0
    context.user_data["pending_index"] = None
    await update.message.reply_text(
        "📥 Index mode ON. Forward any novel post (photo with caption, or text + photo) to me.\n"
        "Send /stopindex when done."
    )

async def stop_index(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = context.user_data.get("indexed_count", 0)
    context.user_data["index_mode"] = False
    context.user_data.pop("pending_index", None)
    msg = f"Index mode OFF. {count} novel(s) added." if count else "Index mode OFF. No novels were added."
    await update.message.reply_text(msg)

async def process_indexed_novel(update: Update, context, caption_text: str, photo_msg):
    novel_name, author = extract_author(caption_text)
    norm_name = normalize(novel_name)
    file_id = photo_msg.photo[-1].file_id if photo_msg.photo else None
    chat_id = photo_msg.chat_id
    message_id = photo_msg.message_id

    storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)

    await db.add_novel(
        normalized_name=norm_name,
        original_name=novel_name,
        author=author,
        platform=None,
        channel=None,
        story_name=None,
        file_id=file_id,
        chat_id=chat_id,
        message_id=message_id,
        storage_chat_id=storage_chat_id,
        storage_message_id=storage_msg_id,
        sender_id=update.effective_user.id,
        sender_name="indexed",
        added_by_index=1
    )
    context.user_data["indexed_count"] = context.user_data.get("indexed_count", 0) + 1
    await update.message.reply_text(f"✅ Indexed: {escape_mdv2(novel_name)}", parse_mode=ParseMode.MARKDOWN_V2)

# ----------------------------------------------------------------------
# Export Database Feature (CSV)
# ----------------------------------------------------------------------
async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Generating database export...")

    if db.fallback:
        conn = sqlite3.connect(db.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT * FROM novels ORDER BY date DESC")
        rows = c.fetchall()
        cols = [col[0] for col in c.description]
        novels = [dict(zip(cols, row)) for row in rows]
        conn.close()
    else:
        cursor = db.db.novels.find({}).sort("date", -1)
        novels = []
        async for doc in cursor:
            doc["id"] = str(doc["_id"])
            novels.append(doc)

    if not novels:
        await update.message.reply_text("Database is empty.")
        return

    output = io.StringIO()
    writer = csv.writer(output)
    keys = ["id", "original_name", "author", "platform", "channel", "story_name", "date", "sender_name"]
    writer.writerow(keys)
    for n in novels:
        writer.writerow([n.get(k, "") for k in keys])

    output.seek(0)
    bio = io.BytesIO(output.read().encode('utf-8'))
    bio.name = f"novels_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"

    await update.message.reply_document(document=bio, caption="📊 Here is your database backup.")

# ----------------------------------------------------------------------
# Admin promotion / demotion / broadcast
# ----------------------------------------------------------------------
async def promote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_super_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Only the super admin can promote users.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /promote <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    await db.add_admin(user_id)
    await update.message.reply_text(f"✅ User {user_id} is now an admin.")

async def demote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_super_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Only the super admin can demote users.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /demote <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    if user_id == SUPER_ADMIN_ID:
        await update.message.reply_text("Cannot demote the super admin.")
        return
    await db.remove_admin(user_id)
    await update.message.reply_text(f"✅ User {user_id} is no longer an admin.")

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    text = " ".join(context.args)
    if db.fallback:
        conn = sqlite3.connect(db.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT DISTINCT sender_id FROM novels WHERE sender_id IS NOT NULL")
        rows = c.fetchall()
        conn.close()
    else:
        cursor = db.db.novels.distinct("sender_id")
        rows = [(uid,) for uid in await cursor]
    sent = 0
    for (uid,) in rows:
        try:
            await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast:\n{text}")
            sent += 1
        except:
            pass
    await update.message.reply_text(f"Broadcast sent to {sent} users.")

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
async def post_init(app: Application):
    await db.initialize()

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_novel_start, pattern="^add_novel$")],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            AUTHOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_author_text),
                CallbackQueryHandler(skip_author_btn, pattern="^skip_author$")
            ],
            IMAGE: [
                MessageHandler(filters.PHOTO, add_image),
                CallbackQueryHandler(skip_image_btn, pattern="^skip_image$")
            ],
            PLATFORM: [CallbackQueryHandler(add_platform_callback, pattern="^plat_")],
            CHANNEL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_channel_text),
                CallbackQueryHandler(skip_channel_btn, pattern="^skip_channel$")
            ],
            STORY_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_story_text),
                CallbackQueryHandler(skip_story_btn, pattern="^skip_story$")
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_add),
            CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(help_menu, pattern="^help_menu$"))
    app.add_handler(CallbackQueryHandler(start_menu_callback, pattern="^start_menu$"))
    app.add_handler(CallbackQueryHandler(search_menu, pattern="^search_menu$"))
    app.add_handler(CallbackQueryHandler(search_page_callback, pattern="^search_page_"))
    app.add_handler(CallbackQueryHandler(button_same_diff, pattern="^(same_|diff_)"))
    
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
    app.add_handler(CommandHandler("promote", promote_cmd))
    app.add_handler(CommandHandler("demote", demote_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    
    app.add_handler(MessageHandler(filters.PHOTO & filters.Chat(GROUP_CHAT_ID), handle_group_photo))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & filters.ChatType.PRIVATE & ~filters.COMMAND, handle_private_message))

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=urljoin(WEBHOOK_URL, "webhook"),
    )

if __name__ == "__main__":
    main()
