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

# Conversation states
(NAME, AUTHOR, IMAGE, PLATFORM, CHANNEL, STORY_NAME) = range(6)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Robust MarkdownV2 Escaper (Fixes your BadRequest errors)
# ----------------------------------------------------------------------
def escape_mdv2(text: Any) -> str:
    if text is None:
        return ""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", str(text))

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

    # Upgraded Fuzzier / Multi-word Search
    async def search_novels(self, query: str, offset=0, limit=5, user_id=None) -> Tuple[List[Dict], int]:
        words = query.strip().split()
        if not words and not user_id:
            return [], 0
            
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_search_novels, words, offset, limit, user_id)
        else:
            filt = {}
            if user_id:
                filt["sender_id"] = user_id
            elif words:
                or_conditions = []
                for w in words:
                    regex = re.compile(re.escape(w), re.IGNORECASE)
                    or_conditions.append({"original_name": regex})
                    or_conditions.append({"author": regex})
                    or_conditions.append({"channel": regex})
                filt["$or"] = or_conditions
                
            total = await self.db.novels.count_documents(filt)
            cursor = self.db.novels.find(filt).sort("date", -1).skip(offset).limit(limit)
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels, total

    def _sqlite_search_novels(self, words: List[str], offset: int, limit: int, user_id: int) -> Tuple[List[Dict], int]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        
        where_clauses = []
        params = []
        
        if user_id:
            where_clauses.append("sender_id = ?")
            params.append(user_id)
        elif words:
            word_clauses = []
            for w in words:
                word_clauses.append("(original_name LIKE ? OR author LIKE ? OR channel LIKE ?)")
                params.extend([f"%{w}%", f"%{w}%", f"%{w}%"])
            where_clauses.append("(" + " OR ".join(word_clauses) + ")")
            
        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        c.execute(f"SELECT COUNT(*) FROM novels WHERE {where_str}", params)
        total = c.fetchone()[0]
        
        c.execute(f"""
            SELECT * FROM novels
            WHERE {where_str}
            ORDER BY date DESC LIMIT ? OFFSET ?
        """, params + [limit, offset])
        
        rows = c.fetchall()
        conn.close()
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows], total

    async def get_novels_filtered(self, author=None, platform=None, channel=None, offset=0, limit=5) -> Tuple[List[Dict], int]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_novels_filtered, author, platform, channel, offset, limit)
        else:
            filt = {}
            if author: filt["author"] = {"$regex": re.escape(author), "$options": "i"}
            if platform: filt["platform"] = platform
            if channel: filt["channel"] = {"$regex": re.escape(channel), "$options": "i"}
            
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
        c.execute(f"SELECT * FROM novels WHERE {where_clause} ORDER BY date DESC LIMIT ? OFFSET ?", params + [limit, offset])
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
            users = len(await self.db.novels.distinct("sender_id"))
            return {"total": total, "authors": authors, "platforms": platforms, "channels": channels, "users": users}

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
        c.execute("SELECT COUNT(DISTINCT sender_id) FROM novels WHERE sender_id IS NOT NULL")
        users = c.fetchone()[0]
        conn.close()
        return {"total": total, "authors": authors, "platforms": platforms, "channels": channels, "users": users}

    async def delete_novel(self, novel_id: str):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_delete_novel, novel_id)
        else:
            try:
                await self.db.novels.delete_one({"_id": ObjectId(novel_id)})
            except:
                pass

    def _sqlite_delete_novel(self, novel_id: str):
        if not novel_id.isdigit(): return
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("DELETE FROM words WHERE novel_id = ?", (int(novel_id),))
        c.execute("DELETE FROM novels WHERE id = ?", (int(novel_id),))
        conn.commit()
        conn.close()

    async def update_novel(self, novel_id: str, field: str, value: str) -> bool:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_update_novel, novel_id, field, value)
        else:
            try:
                res = await self.db.novels.update_one({"_id": ObjectId(novel_id)}, {"$set": {field: value}})
                return res.modified_count > 0
            except:
                return False

    def _sqlite_update_novel(self, novel_id: str, field: str, value: str) -> bool:
        if not novel_id.isdigit() or field not in ['author', 'platform', 'channel', 'story_name']: 
            return False
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute(f"UPDATE novels SET {field} = ? WHERE id = ?", (value, int(novel_id)))
        mods = c.rowcount
        conn.commit()
        conn.close()
        return mods > 0

db = Database()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def extract_author(full_caption: str) -> Tuple[str, Optional[str]]:
    if not full_caption:
        return "", None
    first_line = full_caption.strip().split('\n')[0]
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

async def forward_to_storage(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    if not STORAGE_GROUP_ID:
        return None, None
    try:
        msg = await context.bot.forward_message(chat_id=STORAGE_GROUP_ID, from_chat_id=chat_id, message_id=message_id)
        return msg.chat_id, msg.message_id
    except:
        return None, None

def build_pagination_keyboard(current_page: int, total_pages: int, prefix: str) -> InlineKeyboardMarkup:
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"{prefix}_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_{current_page+1}"))
    return InlineKeyboardMarkup([buttons]) if buttons else None

async def send_rich_duplicate_check(context: ContextTypes.DEFAULT_TYPE, chat_id: int, p: dict, msg_id_suffix: str):
    text = f"📚 *Title:* {escape_mdv2(p['original_name'])}\n"
    text += f"👤 *Author:* {escape_mdv2(p.get('author') or '—')}\n"
    text += f"🌐 *Platform:* {escape_mdv2(p.get('platform') or '—')}"
    
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
    if not update.message.photo or update.message.chat_id != GROUP_CHAT_ID:
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
    chat_id = update.message.chat_id
    message_id = update.message.message_id

    exact = await db.get_exact_match(norm_name)
    if exact:
        orig_name_esc = escape_mdv2(exact['original_name'])
        sender_esc = escape_mdv2(exact['sender_name'])
        await update.message.reply_text(
            f"❌ *This novel was already posted\\!*\nOriginal by: {sender_esc}\nDate: {escape_mdv2(exact['date'][:10])}\nOriginal post:",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        fwd_chat_id = exact.get('storage_chat_id') or exact.get('chat_id')
        fwd_msg_id = exact.get('storage_message_id') or exact.get('message_id')
        if fwd_chat_id and fwd_msg_id:
            try:
                await context.bot.forward_message(chat_id=GROUP_CHAT_ID, from_chat_id=fwd_chat_id, message_id=fwd_msg_id)
            except: pass
        return

    words = norm_name.split()
    partials = await db.get_partial_matches(words)
    if partials:
        try:
            await context.bot.send_message(chat_id=sender.id, text="🔎 Similar novels found. Please check if any of these match what you just posted:")
            for p in partials:
                await send_rich_duplicate_check(context, sender.id, p, str(message_id))
            await update.message.reply_text("🔍 I found similar novels. I’ve DMed you the details to confirm.", reply_to_message_id=message_id)
        except Exception:
            await update.message.reply_text("⚠️ Please start a private chat with me so I can DM you about similar novels.", reply_to_message_id=message_id)
        
        storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
        await db.add_novel(norm_name, novel_name, author, None, None, None, file_id, chat_id, message_id, storage_chat_id, storage_msg_id, sender.id, sender.full_name)
        return

    storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
    await db.add_novel(norm_name, novel_name, author, None, None, None, file_id, chat_id, message_id, storage_chat_id, storage_msg_id, sender.id, sender.full_name)
    try: await update.message.set_reaction(reaction="👍")
    except: pass
    await update.message.reply_text("✅ Novel saved. No repost detected.", reply_to_message_id=message_id, disable_notification=True)

# ----------------------------------------------------------------------
# Duplicate confirmation callback
# ----------------------------------------------------------------------
async def button_same_diff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split("_")
    action, novel_id = parts[0], parts[1]
    suffix = parts[2] if len(parts) > 2 else ""
    is_manual = (suffix == "manual")

    if action == "same":
        if is_manual:
            pending = context.user_data.get("pending_add")
            if pending:
                msg_str = f"❌ Duplicate confirmed\\. The novel '{escape_mdv2(pending['original_name'])}' will NOT be added\\."
                if query.message.photo: await query.edit_message_caption(msg_str, parse_mode=ParseMode.MARKDOWN_V2)
                else: await query.edit_message_text(msg_str, parse_mode=ParseMode.MARKDOWN_V2)
                context.user_data.pop("pending_add", None)
            else:
                if query.message.photo: await query.edit_message_caption("❌ Duplicate confirmed.")
                else: await query.edit_message_text("❌ Duplicate confirmed.")
        else:
            exact = await db.get_exact_match(novel_id) if not novel_id.isdigit() else None
            if exact:
                await context.bot.send_message(
                    chat_id=GROUP_CHAT_ID,
                    text=f"🚨 Duplicate confirmed by @{escape_mdv2(query.from_user.username)}!\nOriginal: {escape_mdv2(exact['original_name'])} by {escape_mdv2(exact['sender_name'])}",
                    parse_mode=ParseMode.MARKDOWN_V2, reply_to_message_id=int(suffix) if suffix.isdigit() else None
                )
            if query.message.photo: await query.edit_message_caption("✅ Marked as duplicate. Warning sent to group.")
            else: await query.edit_message_text("✅ Marked as duplicate. Warning sent to group.")
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
                    success_msg = (f"✅ Novel added\\!\n• Name: {escape_mdv2(pending['original_name'])}\n"
                                   f"• Author: {escape_mdv2(pending['author'] or '—')}\n"
                                   f"• Platform: {escape_mdv2(pending['platform'] or '—')}\n"
                                   f"• Channel: {escape_mdv2(pending['channel'] or '—')}")
                    if query.message.photo: await query.edit_message_caption(success_msg, parse_mode=ParseMode.MARKDOWN_V2)
                    else: await query.edit_message_text(success_msg, parse_mode=ParseMode.MARKDOWN_V2)
                    context.user_data.pop("pending_add", None)
                else:
                    msg_wait = "✅ Noted – not the same. Waiting for other confirmations..."
                    if query.message.photo: await query.edit_message_caption(msg_wait)
                    else: await query.edit_message_text(msg_wait)
            else:
                if query.message.photo: await query.edit_message_caption("✅ Noted – this is a different novel.")
                else: await query.edit_message_text("✅ Noted – this is a different novel.")
        else:
            if query.message.photo: await query.edit_message_caption("✅ Noted – this is a different novel.")
            else: await query.edit_message_text("✅ Noted – this is a different novel.")

# ----------------------------------------------------------------------
# Private chat – menus and /mynovels
# ----------------------------------------------------------------------
def get_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Novel", callback_data="add_novel")],
        [InlineKeyboardButton("🔍 Search", callback_data="search_menu")],
        [InlineKeyboardButton("❓ Help", callback_data="help_menu")],
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Welcome to Storyline Art Novels Manager! Choose an option below:", reply_markup=get_main_keyboard())

async def start_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Welcome to Storyline Art Novels Manager! Choose an option below:", reply_markup=get_main_keyboard())

async def help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = (
        "📚 *Storyline Art Novels Manager*\n\n"
        "*Group:* Post a photo with the novel name\\. Bot checks for duplicates\\.\n"
        "*Private chat:* Use buttons to add novels or search\\.\n\n"
        "Need to fix a blank detail? Use `/edit <ID> <field> <value>`\n"
        "Example: `/edit 12 author John Doe`\n\n"
        "Check your novels: `/mynovels`\n"
        "Admins: `/admin` for control panel\\.\n"
        "Indexing: `/index` & `/stopindex`\n\n"
        "Need Support? Contact: @GamingHommie"
    )
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start_menu")]])
    await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

async def mynovels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data["search_query"] = ""
    context.user_data["search_offset"] = 0
    context.user_data["search_user_id"] = user_id
    await show_search_results(update, context, "", 0, user_id=user_id)

async def edit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /edit <novel_id> <author|platform|channel|story_name> <new_value>")
        return
    novel_id = context.args[0]
    field = context.args[1].lower()
    value = " ".join(context.args[2:])
    
    # Very basic auth: let them edit if they are admin or if they own it. 
    # For now, allowing all users to edit to keep it simple as requested.
    success = await db.update_novel(novel_id, field, value)
    if success:
        await update.message.reply_text(f"✅ Novel {novel_id} updated successfully!")
    else:
        await update.message.reply_text("❌ Failed to update. Make sure the ID is correct and field is valid (author, platform, channel, story_name).")
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
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_author")], [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    await update.message.reply_text("✍️ Great! Now send the *Author Name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return AUTHOR

async def process_author(update: Update, context: ContextTypes.DEFAULT_TYPE, author_val: Optional[str]):
    context.user_data["author"] = author_val
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip Image", callback_data="skip_image")], [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    msg = update.message or update.callback_query.message
    await msg.reply_text("🖼️ Now send a *Photo* for the novel:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return IMAGE

async def add_author_text(update: Update, context: ContextTypes.DEFAULT_TYPE): return await process_author(update, context, update.message.text)
async def skip_author_btn(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.callback_query.answer(); return await process_author(update, context, None)

async def process_image(update: Update, context: ContextTypes.DEFAULT_TYPE, file_id, chat_id, msg_id):
    context.user_data["file_id"], context.user_data["image_chat_id"], context.user_data["image_message_id"] = file_id, chat_id, msg_id
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Pratilipi", callback_data="plat_Pratilipi"), InlineKeyboardButton("Pocket Novel", callback_data="plat_Pocket Novel")],
        [InlineKeyboardButton("Webnovel", callback_data="plat_Webnovel"), InlineKeyboardButton("Other", callback_data="plat_Other")],
        [InlineKeyboardButton("⏭ Skip", callback_data="plat_Skip"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    msg = update.message or update.callback_query.message
    await msg.reply_text("🌐 Choose the *Platform*:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    return PLATFORM

async def add_image(update: Update, context: ContextTypes.DEFAULT_TYPE): return await process_image(update, context, update.message.photo[-1].file_id, update.message.chat_id, update.message.message_id)
async def skip_image_btn(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.callback_query.answer(); return await process_image(update, context, None, None, None)

async def add_platform_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plat = query.data.split("_", 1)[1]
    context.user_data["platform"] = plat if plat != "Skip" else None
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_channel")], [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    await query.edit_message_text("📺 Send the *YouTube Channel Name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return CHANNEL

async def process_channel(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_val: Optional[str]):
    context.user_data["channel"] = channel_val
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_story")], [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    msg = update.message or update.callback_query.message
    await msg.reply_text("📝 Finally, send the *Story Name* on that channel:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return STORY_NAME

async def add_channel_text(update: Update, context: ContextTypes.DEFAULT_TYPE): return await process_channel(update, context, update.message.text.strip())
async def skip_channel_btn(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.callback_query.answer(); return await process_channel(update, context, None)

async def process_story(update: Update, context: ContextTypes.DEFAULT_TYPE, story_val: Optional[str]):
    context.user_data["story_name"] = story_val
    msg = update.message or update.callback_query.message
    await msg.reply_text("🔍 Checking for duplicates...")
    return await finalize_add(update, context, msg)

async def add_story_text(update: Update, context: ContextTypes.DEFAULT_TYPE): return await process_story(update, context, update.message.text.strip())
async def skip_story_btn(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.callback_query.answer(); return await process_story(update, context, None)

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("❌ Adding process cancelled.", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text("❌ Adding process cancelled.", reply_markup=get_main_keyboard())
    return ConversationHandler.END

async def finalize_add(update: Update, context: ContextTypes.DEFAULT_TYPE, msg_obj):
    ud = context.user_data
    norm_name = normalize(ud["novel_name"])
    
    storage_chat_id, storage_msg_id = None, None
    if ud.get("image_chat_id") and ud.get("image_message_id"):
        storage_chat_id, storage_msg_id = await forward_to_storage(context, ud["image_chat_id"], ud["image_message_id"])

    exact = await db.get_exact_match(norm_name)
    if exact:
        await msg_obj.reply_text(
            f"❌ This novel *already exists* in the database\\!\nName: {escape_mdv2(exact['original_name'])}\nAdded by: {escape_mdv2(exact['sender_name'])}",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END

    partials = await db.get_partial_matches(norm_name.split())
    if partials:
        for p in partials: await send_rich_duplicate_check(context, update.effective_chat.id, p, "manual")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Add", callback_data="cancel_add")]])
        await msg_obj.reply_text("🔎 I found similar novels. Confirm if any are the same above.\nIf *No* for all, it will be added.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
        context.user_data["pending_add"] = {
            "norm_name": norm_name, "original_name": ud["novel_name"], "author": ud.get("author"), "platform": ud.get("platform"),
            "channel": ud.get("channel"), "story_name": ud.get("story_name"), "file_id": ud.get("file_id"), "chat_id": update.effective_chat.id,
            "message_id": msg_obj.message_id, "storage_chat_id": storage_chat_id, "storage_message_id": storage_msg_id,
            "sender_id": update.effective_user.id, "sender_name": update.effective_user.full_name, "partial_ids": [p["id"] for p in partials],
        }
        return ConversationHandler.END

    await db.add_novel(norm_name, ud["novel_name"], ud.get("author"), ud.get("platform"), ud.get("channel"), ud.get("story_name"),
                       ud.get("file_id"), update.effective_chat.id, msg_obj.message_id, storage_chat_id, storage_msg_id,
                       update.effective_user.id, update.effective_user.full_name)
    await msg_obj.reply_text(f"✅ Novel added\\!\n• Name: {escape_mdv2(ud['novel_name'])}\n• Author: {escape_mdv2(ud.get('author') or '—')}", parse_mode=ParseMode.MARKDOWN_V2)
    return ConversationHandler.END

# ----------------------------------------------------------------------
# Central Private Message Router & Search
# ----------------------------------------------------------------------
async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ud = context.user_data
    msg = update.message

    if ud.get("index_mode"):
        pending = ud.get("pending_index")
        if msg.photo:
            caption = msg.caption or ""
            if caption: await process_indexed_novel(update, context, caption.split('\n')[0], msg)
            elif pending and pending["type"] == "text":
                await process_indexed_novel(update, context, pending["caption"], msg)
                ud["pending_index"] = None
            else:
                ud["pending_index"] = {"type": "photo", "message": msg}
                await msg.reply_text("📸 Photo received! Now type or forward the novel name.")
        elif msg.text:
            if pending and pending["type"] == "photo":
                await process_indexed_novel(update, context, msg.text, pending["message"])
                ud["pending_index"] = None
            else:
                ud["pending_index"] = {"type": "text", "caption": msg.text}
                await msg.reply_text("📝 Novel name noted! Now forward the photo.")
        return

    # If user drops a random photo not in index mode and not mid-conversation:
    if msg.photo:
        await msg.reply_text("📸 I see a photo! To add this as a novel properly, please click the button below.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add Novel", callback_data="add_novel")]]))
        return

    if not msg.text: return
    if ud.get("awaiting_search"): await handle_search_text(update, context)
    elif ud.get("awaiting_filter"): await handle_filter_text(update, context)
    elif ud.get("awaiting_delete_id"):
        await db.delete_novel(msg.text.strip())
        await msg.reply_text(f"✅ Novel {msg.text.strip()} deleted.")
        ud["awaiting_delete_id"] = False

async def search_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start_menu")]])
    await query.edit_message_text("🔎 Send me a search query (single word works!):", reply_markup=kb)
    context.user_data["awaiting_search"] = True

async def handle_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.strip()
    context.user_data["search_query"] = query_text
    context.user_data["search_offset"] = 0
    context.user_data["search_user_id"] = None
    await show_search_results(update, context, query_text, 0)

async def show_search_results(update: Update, context, query: str, offset: int, user_id=None):
    limit = 5
    novels, total = await db.search_novels(query, offset, limit, user_id)
    admin = await is_admin(update.effective_user.id)

    if not novels:
        await update.message.reply_text("No novels found.")
        context.user_data["awaiting_search"] = False
        return

    title_header = "Your Novels" if user_id else f"Results for *{escape_mdv2(query)}*"
    text = f"🔍 {title_header} ({offset+1}\\-{min(offset+limit, total)} of {total}):\n\n"
    for i, n in enumerate(novels, start=1):
        id_str = escape_mdv2(n.get('id') or str(n.get('_id')))
        text += f"*{i+offset}\\.* `[ID: {id_str}]` {escape_mdv2(n['original_name'])}\n   Author: {escape_mdv2(n.get('author') or '—')} \\| Platform: {escape_mdv2(n.get('platform') or '—')}\n"
        if admin or user_id:
            text += f"   Channel: {escape_mdv2(n.get('channel') or '—')} \\| Story: {escape_mdv2(n.get('story_name') or '—')}\n"
        text += f"   Date: {escape_mdv2(n.get('date', '—')[:10])}\n\n"

    total_pages = (total + limit - 1) // limit
    current_page = offset // limit
    keyboard = build_pagination_keyboard(current_page, total_pages, "search_page")
    if keyboard: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
    else: await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    context.user_data["awaiting_search"] = False

async def search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])
    offset = page * 5
    context.user_data["search_offset"] = offset
    await query.edit_message_text("Loading...")
    # Emulate message reply format for pagination 
    class FakeQuery:
         async def reply_text(self, *args, **kwargs):
             await update.callback_query.edit_message_text(*args, **kwargs)
    fake_update = Update(update.update_id, message=FakeQuery(), effective_user=update.effective_user)
    await show_search_results(fake_update, context, context.user_data.get("search_query", ""), offset, context.user_data.get("search_user_id"))

# ----------------------------------------------------------------------
# Admin panel & Indexing & Export
# ----------------------------------------------------------------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 List novels", callback_data="admin_list")],
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔍 Filtered search", callback_data="admin_filter")],
        [InlineKeyboardButton("🗑 Delete (by ID)", callback_data="admin_delete_prompt")],
        [InlineKeyboardButton("🔙 Close Admin", callback_data="start_menu")]
    ])
    text = "🔧 Admin Panel:\n(Pro tip: Type /export to download a database CSV)"
    if update.message: await update.message.reply_text(text, reply_markup=kb)
    else: await update.callback_query.edit_message_text(text, reply_markup=kb)

async def admin_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data["admin_list_offset"] = 0
    context.user_data["admin_list_filters"] = {}
    await show_admin_list(update, context)

async def show_admin_list(update: Update, context):
    offset = context.user_data.get("admin_list_offset", 0)
    filters = context.user_data.get("admin_list_filters", {})
    novels, total = await db.get_novels_filtered(filters.get("author"), filters.get("platform"), filters.get("channel"), offset, 5)
    
    if not novels:
        await update.callback_query.edit_message_text("No novels found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]]))
        return
    text = f"📋 Novels \\(filtered\\) – page {offset//5 + 1}\n\n"
    for n in novels:
        id_str = escape_mdv2(n.get('id') or str(n.get('_id')))
        text += (f"`{id_str}`: {escape_mdv2(n['original_name'])}\n"
                 f"   Author: {escape_mdv2(n.get('author') or '—')} \\| Plat: {escape_mdv2(n.get('platform') or '—')}\n"
                 f"   Chan: {escape_mdv2(n.get('channel') or '—')} \\| Date: {escape_mdv2(n.get('date', '—')[:10])}\n\n")
    
    total_pages = (total + 4) // 5
    current_page = offset // 5
    buttons = []
    if current_page > 0: buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_page_{current_page-1}"))
    if current_page < total_pages - 1: buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_page_{current_page+1}"))
    keyboard = [[InlineKeyboardButton("🔧 Set filter", callback_data="admin_filter")], buttons, [InlineKeyboardButton("🔙 Back", callback_data="admin")]]
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data["admin_list_offset"] = int(update.callback_query.data.split("_")[-1]) * 5
    await show_admin_list(update, context)

async def admin_filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="admin")]])
    await update.callback_query.edit_message_text("Send filter:\n`author:Name`\n`platform:Pratilipi`\nCombine with spaces\nOr type /skip to clear.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    context.user_data["awaiting_filter"] = True

async def handle_filter_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.lower() == "/skip":
        context.user_data["admin_list_filters"] = {}
        await update.message.reply_text("Filters cleared.")
        context.user_data["awaiting_filter"] = False
        return await admin_panel(update, context)
    
    filters = {}
    for part in shlex.split(text):
        if ":" in part:
            k, v = part.split(":", 1)
            if k.strip().lower() in ("author", "platform", "channel"): filters[k.strip().lower()] = v.strip()
    context.user_data["admin_list_filters"] = filters
    context.user_data["admin_list_offset"] = 0
    context.user_data["awaiting_filter"] = False
    await update.message.reply_text("Filters applied. Check list in /admin")

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    s = await db.get_stats()
    text = f"📊 *Statistics*\nTotal novels: {s['total']}\nDistinct authors: {s['authors']}\nPlatforms used: {s['platforms']}\nChannels: {s['channels']}\nUsers contributed: {s['users']}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]])
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)

async def admin_delete_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="admin")]])
    await update.callback_query.edit_message_text("Send the ID of the novel to delete:", reply_markup=kb)
    context.user_data["awaiting_delete_id"] = True

async def index_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    context.user_data.update({"index_mode": True, "indexed_count": 0, "pending_index": None})
    await update.message.reply_text("📥 Index mode ON. Forward any novel post to me. Send /stopindex when done.")

async def stop_index(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = context.user_data.get("indexed_count", 0)
    context.user_data["index_mode"] = False
    context.user_data.pop("pending_index", None)
    await update.message.reply_text(f"Index mode OFF. {count} novel(s) added.")

async def process_indexed_novel(update: Update, context, caption_text: str, photo_msg):
    novel_name, author = extract_author(caption_text)
    norm_name = normalize(novel_name)
    file_id = photo_msg.photo[-1].file_id if photo_msg.photo else None
    storage_chat_id, storage_msg_id = await forward_to_storage(context, photo_msg.chat_id, photo_msg.message_id)
    await db.add_novel(norm_name, novel_name, author, None, None, None, file_id, photo_msg.chat_id, photo_msg.message_id, storage_chat_id, storage_msg_id, update.effective_user.id, "indexed", 1)
    context.user_data["indexed_count"] = context.user_data.get("indexed_count", 0) + 1
    await update.message.reply_text(f"✅ Indexed: {escape_mdv2(novel_name)}", parse_mode=ParseMode.MARKDOWN_V2)

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    await update.message.reply_text("⏳ Generating database export...")
    if db.fallback:
        conn = sqlite3.connect(db.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT * FROM novels ORDER BY date DESC")
        novels = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
        conn.close()
    else:
        cursor = db.db.novels.find({}).sort("date", -1)
        novels = [dict(doc, id=str(doc["_id"])) async for doc in cursor]

    if not novels: return await update.message.reply_text("Database is empty.")
    output = io.StringIO()
    writer = csv.writer(output)
    keys = ["id", "original_name", "author", "platform", "channel", "story_name", "date", "sender_name"]
    writer.writerow(keys)
    for n in novels: writer.writerow([n.get(k, "") for k in keys])
    bio = io.BytesIO(output.getvalue().encode('utf-8'))
    bio.name = f"novels_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    await update.message.reply_document(document=bio, caption="📊 Here is your database backup.")

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(lambda app: db.initialize()).build()

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_novel_start, pattern="^add_novel$")],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            AUTHOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_author_text), CallbackQueryHandler(skip_author_btn, pattern="^skip_author$")],
            IMAGE: [MessageHandler(filters.PHOTO, add_image), CallbackQueryHandler(skip_image_btn, pattern="^skip_image$")],
            PLATFORM: [CallbackQueryHandler(add_platform_callback, pattern="^plat_")],
            CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_channel_text), CallbackQueryHandler(skip_channel_btn, pattern="^skip_channel$")],
            STORY_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_story_text), CallbackQueryHandler(skip_story_btn, pattern="^skip_story$")],
        },
        fallbacks=[CommandHandler("cancel", cancel_add), CallbackQueryHandler(cancel_add, pattern="^cancel_add$")],
    )

    app.add_handler(CommandHandler(["start", "help"], start))
    app.add_handler(CommandHandler("mynovels", mynovels_cmd))
    app.add_handler(CommandHandler("edit", edit_cmd))
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
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(MessageHandler(filters.PHOTO & filters.Chat(GROUP_CHAT_ID), handle_group_photo))
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & filters.ChatType.PRIVATE & ~filters.COMMAND, handle_private_message))

    app.run_webhook(listen="0.0.0.0", port=PORT, url_path="webhook", webhook_url=urljoin(WEBHOOK_URL, "webhook"))

if __name__ == "__main__": main()
