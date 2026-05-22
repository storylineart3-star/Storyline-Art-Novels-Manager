import asyncio
import logging
import os
import re
import shlex
import sqlite3
import csv
import io
import difflib
from datetime import datetime, timezone
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
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID", 0))
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
PORT = int(os.environ.get("PORT", 10000))
STORAGE_GROUP_ID = int(os.environ.get("STORAGE_GROUP_ID", 0))

MONGO_URI = os.environ.get("MONGO_URI")
DB_PATH = "novels.db"

# Conversation states
(NAME, AUTHOR, PLATFORM, CHANNEL, STORY_NAME, IMAGE) = range(6)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Database abstraction layer (MongoDB primary, SQLite fallback)
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
        now = datetime.now(timezone.utc).isoformat()
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
            try:
                await self.db.novels.insert_one(doc)
            except Exception as e:
                logger.error(f"Mongo Insert Error: {e}")

    def _sqlite_add_novel(self, normalized_name, original_name, author, platform, channel, story_name,
                          file_id, chat_id, message_id, storage_chat_id, storage_message_id,
                          sender_id, sender_name, date, added_by_index, words):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        try:
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
        except Exception as e:
            logger.error(f"SQLite Insert Error: {e}")
        finally:
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

    async def get_novel_by_id(self, novel_id: str) -> Optional[Dict]:
        if self.fallback:
            try:
                nid = int(novel_id)
            except ValueError:
                return None
            return await asyncio.to_thread(self._sqlite_get_novel_by_id, nid)
        else:
            try:
                obj_id = ObjectId(novel_id)
            except:
                return None
            doc = await self.db.novels.find_one({"_id": obj_id})
            if doc:
                doc["id"] = str(doc["_id"])
            return doc

    def _sqlite_get_novel_by_id(self, novel_id: int) -> Optional[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT * FROM novels WHERE id = ? LIMIT 1", (novel_id,))
        row = c.fetchone()
        conn.close()
        if row:
            cols = [col[0] for col in c.description]
            return dict(zip(cols, row))
        return None

    async def get_all_normalized_names(self) -> List[Tuple[str, str]]:
        if self.fallback:
            conn = sqlite3.connect(self.sqlite_path)
            c = conn.cursor()
            c.execute("SELECT id, normalized_name FROM novels")
            rows = c.fetchall()
            conn.close()
            return [(str(row[0]), row[1]) for row in rows]
        else:
            cursor = self.db.novels.find({}, {"_id": 1, "normalized_name": 1})
            return [(str(doc["_id"]), doc["normalized_name"]) async for doc in cursor]

    async def search_novels(self, query: str) -> List[Dict]:
        query_words = [w for w in normalize(query).split() if len(w) > 2]
        if not query_words:
            query_words = [normalize(query)]
            
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_search_novels_unpaginated, query_words)
        else:
            regexes = [re.compile(re.escape(w), re.IGNORECASE) for w in query_words]
            or_cond = []
            for r in regexes:
                or_cond.extend([
                    {"original_name": {"$regex": r}},
                    {"author": {"$regex": r}},
                    {"channel": {"$regex": r}}
                ])
            if not or_cond: 
                return []
            cursor = self.db.novels.find({"$or": or_cond})
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels

    def _sqlite_search_novels_unpaginated(self, query_words: List[str]) -> List[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        query_conditions = []
        params = []
        for w in query_words:
            param = f"%{w}%"
            query_conditions.append("(original_name LIKE ? OR author LIKE ? OR channel LIKE ?)")
            params.extend([param, param, param])
        where_clause = " OR ".join(query_conditions)
        if not where_clause:
            return []
        c.execute(f"SELECT * FROM novels WHERE {where_clause} ORDER BY date DESC")
        rows = c.fetchall()
        conn.close()
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows]

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

    async def get_all_novels(self) -> List[Dict]:
        if self.fallback:
            return await asyncio.to_thread(self._sqlite_get_all_novels)
        else:
            cursor = self.db.novels.find({}).sort("date", -1)
            novels = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                novels.append(doc)
            return novels

    def _sqlite_get_all_novels(self) -> List[Dict]:
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT * FROM novels ORDER BY date DESC")
        rows = c.fetchall()
        conn.close()
        cols = [col[0] for col in c.description]
        return [dict(zip(cols, row)) for row in rows]

    async def delete_novel(self, novel_id: str):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_delete_novel, novel_id)
        else:
            try:
                await self.db.novels.delete_one({"_id": ObjectId(novel_id)})
            except:
                pass

    def _sqlite_delete_novel(self, novel_id: str):
        try:
            nid = int(novel_id)
        except ValueError:
            return
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("DELETE FROM words WHERE novel_id = ?", (nid,))
        c.execute("DELETE FROM novels WHERE id = ?", (nid,))
        conn.commit()
        conn.close()

    async def delete_novel_by_msg(self, chat_id: int, message_id: int):
        if self.fallback:
            await asyncio.to_thread(self._sqlite_delete_novel_by_msg, chat_id, message_id)
        else:
            await self.db.novels.delete_one({"chat_id": chat_id, "message_id": message_id})

    def _sqlite_delete_novel_by_msg(self, chat_id: int, message_id: int):
        conn = sqlite3.connect(self.sqlite_path)
        c = conn.cursor()
        c.execute("SELECT id FROM novels WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        row = c.fetchone()
        if row:
            nid = row[0]
            c.execute("DELETE FROM words WHERE novel_id = ?", (nid,))
            c.execute("DELETE FROM novels WHERE id = ?", (nid,))
        conn.commit()
        conn.close()

db = Database()

# ----------------------------------------------------------------------
# Helpers & Smart Ranking Methods
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
            return match.group(1).strip(), match.group(2).strip()
    return full_caption.strip(), None

def normalize(name: Any) -> str:
    """Safely normalizes strings, handling None or missing data gracefully."""
    if name is None:
        return ""
    if not isinstance(name, str):
        name = str(name)
        
    cleaned = name.strip().lower()
    cleaned = re.sub(r'[^\w\s]', '', cleaned, flags=re.UNICODE)
    return " ".join(cleaned.split())

def score_match(query: str, novel: dict) -> float:
    """Intelligently score search results using token overlaps and similarity."""
    q_norm = normalize(query)
    q_tokens = set([w for w in q_norm.split() if len(w) > 2] or q_norm.split())
    
    n_norm = normalize(novel.get('original_name', ''))
    n_tokens = set([w for w in n_norm.split() if len(w) > 2] or n_norm.split())
    
    overlap = len(q_tokens.intersection(n_tokens))
    ratio = difflib.SequenceMatcher(None, q_norm, n_norm).ratio()
    
    author_norm = normalize(novel.get('author', ''))
    author_ratio = difflib.SequenceMatcher(None, q_norm, author_norm).ratio() if author_norm else 0
    
    return (overlap * 0.3) + (ratio * 0.5) + (author_ratio * 0.2)

def fuzzy_match(query: str, candidates: List[Tuple[str, str]], cutoff: float = 0.6) -> List[Tuple[str, str, float]]:
    results = []
    query_norm = normalize(query)
    for cid, cname in candidates:
        cname_norm = normalize(cname)
        ratio = difflib.SequenceMatcher(None, query_norm, cname_norm).ratio()
        if ratio >= cutoff:
            results.append((cid, cname, ratio))
    results.sort(key=lambda x: x[2], reverse=True)
    return results

async def find_similar_novels(novel_name: str, cutoff: float = 0.6) -> List[Dict]:
    candidates = await db.get_all_normalized_names()
    matches = fuzzy_match(novel_name, candidates, cutoff)
    similar = []
    for cid, _, _ in matches[:5]: 
        novel = await db.get_novel_by_id(cid)
        if novel:
            similar.append(novel)
    return similar

async def is_admin(user_id: int) -> bool:
    return await db.is_admin(user_id)

async def is_super_admin(user_id: int) -> bool:
    return user_id == SUPER_ADMIN_ID

def escape_mdv2(text: Any) -> str:
    if not text:
        return "—"
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", str(text))

def build_pagination_keyboard(current_page: int, total_pages: int, prefix: str) -> Optional[InlineKeyboardMarkup]:
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

async def send_photo_by_novel(context, chat_id: int, novel: Dict):
    file_id = novel.get("file_id")
    if file_id:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=file_id, caption=f"📸 Novel: {escape_mdv2(novel['original_name'])}", parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Could not send photo: {e}")
    else:
        storage_chat = novel.get("storage_chat_id")
        storage_msg = novel.get("storage_message_id")
        if storage_chat and storage_msg:
            try:
                await context.bot.forward_message(chat_id=chat_id, from_chat_id=storage_chat, message_id=storage_msg)
            except:
                await context.bot.send_message(chat_id=chat_id, text="❌ No photo available for this novel.")
        else:
            await context.bot.send_message(chat_id=chat_id, text="❌ No photo available for this novel.")

# ----------------------------------------------------------------------
# Group Submission Handlers
# ----------------------------------------------------------------------
async def process_group_submission(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, photo_data: Optional[dict] = None):
    novel_name, author = extract_author(text)
    norm_name = normalize(novel_name)
    if not norm_name:
        return

    if photo_data:
        file_id = photo_data["file_id"]
        sender_id = photo_data["sender_id"]
        sender_name = photo_data["sender_name"]
        chat_id = photo_data["chat_id"]
        message_id = photo_data["photo_msg_id"]
        reply_message_id = update.message.message_id
    else:
        photo = update.message.photo[-1]
        file_id = photo.file_id
        sender = update.message.from_user
        sender_name = sender.full_name if sender else "Unknown"
        sender_id = sender.id if sender else 0
        chat_id = update.message.chat_id
        message_id = update.message.message_id
        reply_message_id = message_id

    # 1. Exact match
    exact = await db.get_exact_match(norm_name)
    if exact:
        orig_name_esc = escape_mdv2(exact['original_name'])
        sender_esc = escape_mdv2(exact['sender_name'])
        date_esc = escape_mdv2(exact['date'][:10]) if exact.get('date') else escape_mdv2('—')
        await update.message.reply_text(
            rf"❌ *This novel was already posted\!*\nOriginal by: {sender_esc}\nDate: {date_esc}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_to_message_id=reply_message_id
        )
        fwd_chat_id = exact.get('storage_chat_id') or exact.get('chat_id')
        fwd_msg_id = exact.get('storage_message_id') or exact.get('message_id')
        if fwd_chat_id and fwd_msg_id:
            try:
                await context.bot.forward_message(chat_id=chat_id, from_chat_id=fwd_chat_id, message_id=fwd_msg_id)
            except Exception:
                pass
        return

    # 2. Fuzzy matches (ask user in DM)
    fuzzy_similar = await find_similar_novels(novel_name, cutoff=0.3)
    if fuzzy_similar:
        try:
            await context.bot.send_message(
                chat_id=sender_id,
                text="🔎 **Potential duplicate found!** I found novels with similar names from your recent group post.\nPlease confirm if any is the same.\nIf you click *No* for all, the novel will be added automatically."
            )
            for p in fuzzy_similar:
                await send_photo_by_novel(context, sender_id, p)
                detail_text = (
                    f"📖 *{escape_mdv2(p['original_name'])}*\n"
                    f"Author: {escape_mdv2(p.get('author') or '—')}\n"
                    f"Platform: {escape_mdv2(p.get('platform') or '—')}\n"
                    f"Channel: {escape_mdv2(p.get('channel') or '—')}\n"
                    f"Story: {escape_mdv2(p.get('story_name') or '—')}\n"
                    f"Date: {escape_mdv2(p.get('date', '')[:10])}"
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes (same)", callback_data=f"same_{p['id']}_{message_id}"),
                    InlineKeyboardButton("❌ No", callback_data=f"diff_{p['id']}_{message_id}")
                ]])
                await context.bot.send_message(chat_id=sender_id, text=detail_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
            
            context.user_data["pending_group_add"] = {
                "norm_name": norm_name,
                "original_name": novel_name,
                "author": author,
                "file_id": file_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "partial_ids": [p["id"] for p in fuzzy_similar],
            }
            storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
            context.user_data["pending_group_add"].update({
                "storage_chat_id": storage_chat_id,
                "storage_message_id": storage_msg_id
            })
            await update.message.reply_text("🔍 I found similar novels. I’ve DMed you the details. Please check and confirm.", reply_to_message_id=reply_message_id)
        except Exception as e:
            logger.warning(f"Could not DM user {sender_id}: {e}")
            await update.message.reply_text("⚠️ Please start a private chat with me so I can DM you about similar novels.", reply_to_message_id=reply_message_id)
        return

    # 3. Save novel
    storage_chat_id, storage_msg_id = await forward_to_storage(context, chat_id, message_id)
    await db.add_novel(norm_name, novel_name, author, None, None, None,
                       file_id, chat_id, message_id, storage_chat_id, storage_msg_id,
                       sender_id, sender_name)
    try:
        await update.message.set_reaction(reaction="👍")
    except Exception:
        await update.message.reply_text(f"✅ Novel '{novel_name}' successfully saved. No duplicates detected.", reply_to_message_id=reply_message_id)

async def handle_group_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if GROUP_CHAT_ID != 0 and update.message.chat_id != GROUP_CHAT_ID:
        return
        
    caption = update.message.caption
    if not caption:
        prompt_msg = await update.message.reply_text(
            "📸 Image received! Please **reply directly to this message** with the novel title (e.g. 'Title by Author').",
            reply_to_message_id=update.message.message_id
        )
        if "group_photo_queue" not in context.chat_data:
            context.chat_data["group_photo_queue"] = {}
            
        context.chat_data["group_photo_queue"][prompt_msg.message_id] = {
            "photo_msg_id": update.message.message_id,
            "file_id": update.message.photo[-1].file_id,
            "sender_id": update.message.from_user.id,
            "sender_name": update.message.from_user.full_name,
            "chat_id": update.message.chat_id,
        }
        return
        
    await process_group_submission(update, context, caption, None)

async def handle_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if GROUP_CHAT_ID != 0 and update.message.chat_id != GROUP_CHAT_ID:
        return
    if not update.message.reply_to_message:
        return
        
    reply_id = update.message.reply_to_message.message_id
    queue = context.chat_data.get("group_photo_queue", {})
    
    if reply_id in queue:
        photo_data = queue.pop(reply_id)
        await process_group_submission(update, context, update.message.text, photo_data)

# ----------------------------------------------------------------------
# Duplicate confirmation callback (group + manual)
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
    is_group_pending = (suffix and suffix.isdigit()) 

    if action == "same":
        if is_manual:
            pending = context.user_data.get("pending_add")
            if pending:
                await query.edit_message_text(f"❌ Duplicate confirmed. The novel '{pending['original_name']}' will NOT be added.")
                context.user_data.pop("pending_add", None)
            else:
                await query.edit_message_text("❌ Duplicate confirmed.")
        elif is_group_pending:
            pending = context.user_data.get("pending_group_add")
            if pending:
                await query.edit_message_text(f"❌ Duplicate confirmed. The novel '{pending['original_name']}' will NOT be added.")
                context.user_data.pop("pending_group_add", None)
            else:
                await query.edit_message_text("❌ Duplicate confirmed.")
        else:
            exact = await db.get_novel_by_id(novel_id)
            if exact and GROUP_CHAT_ID != 0:
                orig_name_esc = escape_mdv2(exact['original_name'])
                sender_esc = escape_mdv2(exact['sender_name'])
                user_esc = escape_mdv2(query.from_user.username or query.from_user.full_name)
                try:
                    await context.bot.send_message(
                        chat_id=GROUP_CHAT_ID,
                        text=rf"🚨 *Duplicate confirmed* by @{user_esc}\!\nOriginal: {orig_name_esc} by {sender_esc}",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_to_message_id=int(suffix) if suffix.isdigit() else None
                    )
                except: pass
            if suffix.isdigit():
                await db.delete_novel_by_msg(GROUP_CHAT_ID, int(suffix))
            await query.edit_message_text("✅ Marked as duplicate. Warning sent to group and record dropped.")
    else:  # diff
        if is_manual:
            pending = context.user_data.get("pending_add")
            if pending:
                if novel_id in pending.get("partial_ids", []):
                    pending["partial_ids"].remove(novel_id)
                if not pending.get("partial_ids"):
                    await db.add_novel(
                        pending["norm_name"], pending["original_name"], pending["author"],
                        pending["platform"], pending["channel"], pending["story_name"],
                        pending.get("file_id"), pending["chat_id"], pending["message_id"],
                        pending.get("storage_chat_id"), pending.get("storage_message_id"),
                        pending["sender_id"], pending["sender_name"]
                    )
                    await query.edit_message_text(
                        rf"✅ *Novel added\!\n"
                        f"• Name: {escape_mdv2(pending['original_name'])}\n"
                        f"• Author: {escape_mdv2(pending['author'])}\n"
                        f"• Platform: {escape_mdv2(pending['platform'])}\n"
                        f"• Channel: {escape_mdv2(pending['channel'])}\n"
                        f"• Story: {escape_mdv2(pending['story_name'])}\n"
                        f"• Date: {escape_mdv2(datetime.now(timezone.utc).strftime('%Y-%m-%d'))}",
                        parse_mode=ParseMode.MARKDOWN_V2
                    )
                    context.user_data.pop("pending_add", None)
                else:
                    await query.edit_message_text("✅ Noted – not the same. Waiting for other confirmations...")
            else:
                await query.edit_message_text("✅ Noted – this is a different novel.")
        elif is_group_pending:
            pending = context.user_data.get("pending_group_add")
            if pending:
                if novel_id in pending.get("partial_ids", []):
                    pending["partial_ids"].remove(novel_id)
                if not pending.get("partial_ids"):
                    await db.add_novel(
                        pending["norm_name"], pending["original_name"], pending["author"],
                        None, None, None,
                        pending.get("file_id"), pending["chat_id"], pending["message_id"],
                        pending.get("storage_chat_id"), pending.get("storage_message_id"),
                        pending["sender_id"], pending["sender_name"]
                    )
                    await query.edit_message_text(
                        rf"✅ *Novel added\!\n"
                        f"• Name: {escape_mdv2(pending['original_name'])}\n"
                        f"• Author: {escape_mdv2(pending['author'])}\n"
                        f"• Date: {escape_mdv2(datetime.now(timezone.utc).strftime('%Y-%m-%d'))}",
                        parse_mode=ParseMode.MARKDOWN_V2
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=pending["chat_id"],
                            text=f"✅ Novel '{pending['original_name']}' successfully saved after manual review.",
                            reply_to_message_id=pending["message_id"]
                        )
                    except:
                        pass
                    context.user_data.pop("pending_group_add", None)
                else:
                    await query.edit_message_text("✅ Noted – not the same. Waiting for other confirmations...")
            else:
                await query.edit_message_text("✅ Noted – this is a different novel.")
        else:
            await query.edit_message_text("✅ Noted – this is a different novel.")

# ----------------------------------------------------------------------
# Private chat – start & help
# ----------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
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
        r"📚 *Novel Repost Guard*\n\n"
        r"*Group:* Post a photo with the novel name\. Bot checks for duplicates\.\n"
        r"*Private chat:* Use buttons to add novels or search\.\n\n"
        r"Admins: /admin for control panel\.\n"
        r"Super admin: /promote & /demote\.\n"
        r"Bulk Import: /bulkimport\.\n"
        r"Indexing: /index & /stopindex\."
    )
    await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN_V2)

# ----------------------------------------------------------------------
# Manual add conversation 
# ----------------------------------------------------------------------
def get_navigation_buttons(back_data: str, skip_data: Optional[str] = None):
    row1 = [InlineKeyboardButton("⬅️ Back", callback_data=back_data)]
    if skip_data:
        row1.append(InlineKeyboardButton("⏭️ Skip", callback_data=skip_data))
    return InlineKeyboardMarkup([row1, [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])

async def add_novel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    if query:
        await query.answer()
        await query.edit_message_text("📖 Send me the *novel name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    else:
        await update.message.reply_text("📖 Send me the *novel name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return NAME

async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["novel_name"] = update.message.text.strip()
    kb = get_navigation_buttons("back_to_name", "skip_author")
    await update.message.reply_text("✍️ Now send the *author name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return AUTHOR

async def back_to_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]])
    await query.edit_message_text("📖 Send me the *novel name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return NAME

async def add_author(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["author"] = update.message.text.strip()
    return await go_to_platform(update.message.reply_text, context)

async def skip_author_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["author"] = None
    return await go_to_platform(query.edit_message_text, context)

async def back_to_author(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = get_navigation_buttons("back_to_name", "skip_author")
    await query.edit_message_text("✍️ Now send the *author name*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return AUTHOR

async def go_to_platform(reply_func, context):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Pratilipi", callback_data="plat_Pratilipi"),
         InlineKeyboardButton("Pocket Novel", callback_data="plat_Pocket Novel")],
        [InlineKeyboardButton("Other", callback_data="plat_Other"),
         InlineKeyboardButton("Skip", callback_data="plat_Skip")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_author"),
         InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await reply_func("🌐 Choose *platform*:", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    return PLATFORM

async def add_platform_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plat = query.data.split("_", 1)[1]
    context.user_data["platform"] = plat if plat != "Skip" else None
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_platform"),
         InlineKeyboardButton("⏭️ Skip", callback_data="skip_channel")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await query.edit_message_text(r"📺 Now send the *channel name* \(YouTube\):", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
    return CHANNEL

async def back_to_platform(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Pratilipi", callback_data="plat_Pratilipi"),
         InlineKeyboardButton("Pocket Novel", callback_data="plat_Pocket Novel")],
        [InlineKeyboardButton("Other", callback_data="plat_Other"),
         InlineKeyboardButton("Skip", callback_data="plat_Skip")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_to_author"),
         InlineKeyboardButton("❌ Cancel", callback_data="cancel_add")]
    ])
    await query.edit_message_text("🌐 Choose *platform*:", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    return PLATFORM

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["channel"] = update.message.text.strip()
    kb = get_navigation_buttons("back_to_channel", "skip_story")
    await update.message.reply_text("📝 Now send the *story name on that channel*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return STORY_NAME

async def skip_channel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["channel"] = None
    kb = get_navigation_buttons("back_to_channel", "skip_story")
    await query.edit_message_text("📝 Now send the *story name on that channel*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return STORY_NAME

async def back_to_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = get_navigation_buttons("back_to_platform", "skip_channel")
    await query.edit_message_text(r"📺 Now send the *channel name* \(YouTube\):", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return CHANNEL

async def add_story_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["story_name"] = update.message.text.strip()
    return await go_to_image(update.message.reply_text, context)

async def skip_story_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["story_name"] = None
    return await go_to_image(query.edit_message_text, context)

async def back_to_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = get_navigation_buttons("back_to_channel", "skip_story")
    await query.edit_message_text("📝 Now send the *story name on that channel*:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=kb)
    return STORY_NAME

async def go_to_image(reply_func, context):
    kb = get_navigation_buttons("back_to_story", "skip_image")
    await reply_func("🖼️ Send a *photo*, or use the buttons below:", reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    return IMAGE

async def add_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo = update.message.photo[-1]
    context.user_data["file_id"] = photo.file_id
    context.user_data["image_chat_id"] = update.message.chat_id
    context.user_data["image_message_id"] = update.message.message_id
    await update.message.reply_text("✅ Photo saved. Checking for duplicates...")
    return await finalize_add(update, context)

async def add_image_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["file_id"] = None
    context.user_data["image_chat_id"] = None
    context.user_data["image_message_id"] = None
    await query.edit_message_text("Skipped photo. Checking for duplicates...")
    return await finalize_add(update, context)

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
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
    channel = user_data.get("channel")
    story_name = user_data.get("story_name")
    file_id = user_data.get("file_id")
    norm_name = normalize(name)
    sender = update.effective_user
    sender_name = sender.full_name
    sender_id = sender.id
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    storage_chat_id, storage_msg_id = None, None
    if user_data.get("image_chat_id") and user_data.get("image_message_id"):
        storage_chat_id, storage_msg_id = await forward_to_storage(
            context, user_data["image_chat_id"], user_data["image_message_id"]
        )

    exact = await db.get_exact_match(norm_name)
    if exact:
        orig_name_esc = escape_mdv2(exact['original_name'])
        sender_esc = escape_mdv2(exact['sender_name'])
        date_esc = escape_mdv2(exact['date'][:10]) if exact.get('date') else escape_mdv2('—')
        await update.effective_message.reply_text(
            rf"❌ This novel *already exists* in the database\!\n"
            f"Name: {orig_name_esc}\nAdded by: {sender_esc} on {date_esc}",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        if exact.get("file_id"):
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=exact["file_id"],
                                             caption=f"Original post (ID {exact['id']})")
            except:
                pass
        context.user_data.clear()
        return ConversationHandler.END

    fuzzy_similar = await find_similar_novels(name, cutoff=0.3)
    if fuzzy_similar:
        for p in fuzzy_similar:
            await send_photo_by_novel(context, chat_id, p)
            detail_text = (
                f"📖 *{escape_mdv2(p['original_name'])}*\n"
                f"Author: {escape_mdv2(p.get('author') or '—')}\n"
                f"Platform: {escape_mdv2(p.get('platform') or '—')}\n"
                f"Channel: {escape_mdv2(p.get('channel') or '—')}\n"
                f"Story: {escape_mdv2(p.get('story_name') or '—')}\n"
                f"Date: {escape_mdv2(p.get('date', '')[:10])}"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes (same)", callback_data=f"same_{p['id']}_manual"),
                InlineKeyboardButton("❌ No", callback_data=f"diff_{p['id']}_manual")
            ]])
            await context.bot.send_message(chat_id=chat_id, text=detail_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
        await update.effective_message.reply_text(
            r"🔎 I found similar novels above\. Please use the buttons to confirm if any are the same\.\n"
            r"If you click *No* for all, the novel will be added automatically after the last confirmation\.\n"
            r"You can also /cancel to abort\.",
            parse_mode=ParseMode.MARKDOWN_V2
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
            "partial_ids": [p["id"] for p in fuzzy_similar],
        }
        return ConversationHandler.END

    await db.add_novel(norm_name, name, author, platform, channel, story_name,
                       file_id, chat_id, message_id, storage_chat_id, storage_msg_id,
                       sender_id, sender_name)
    await update.effective_message.reply_text(
        rf"✅ *Novel added\!\n"
        f"• Name: {escape_mdv2(name)}\n"
        f"• Author: {escape_mdv2(author)}\n"
        f"• Platform: {escape_mdv2(platform)}\n"
        f"• Channel: {escape_mdv2(channel)}\n"
        f"• Story: {escape_mdv2(story_name)}\n"
        f"• Date: {escape_mdv2(datetime.now(timezone.utc).strftime('%Y-%m-%d'))}",
        parse_mode=ParseMode.MARKDOWN_V2
    )
    context.user_data.clear()
    return ConversationHandler.END

# ----------------------------------------------------------------------
# Search (Smart Ranking)
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
    
    novels = await db.search_novels(query_text)
    
    # Score and rank candidates based on similarity
    ranked = []
    for n in novels:
        score = score_match(query_text, n)
        if score > 0.15:  # Low threshold to capture broad, token-based variations
            ranked.append((score, n))
            
    ranked.sort(key=lambda x: x[0], reverse=True)
    results = [r[1] for r in ranked]
    
    context.user_data["search_results"] = results
    context.user_data["search_query"] = query_text
    context.user_data["search_offset"] = 0
    context.user_data["awaiting_search"] = False
    
    await display_search_page(update.message.reply_text, context, update.effective_user.id)

async def display_search_page(reply_func, context, user_id):
    offset = context.user_data.get("search_offset", 0)
    results = context.user_data.get("search_results", [])
    query = context.user_data.get("search_query", "")
    limit = 5
    total = len(results)
    
    if not results:
        await reply_func("❌ No novels found matching your search.")
        return

    page_results = results[offset:offset+limit]
    admin = await is_admin(user_id)

    query_esc = escape_mdv2(query)
    header_esc = escape_mdv2(f"({offset+1}-{min(offset+limit, total)} of {total}):")
    text = f"🔍 Results for *{query_esc}* {header_esc}\n\n"
    
    for i, n in enumerate(page_results, start=1):
        num_esc = escape_mdv2(f"{i+offset}.")
        name_esc = escape_mdv2(n['original_name'])
        author_esc = escape_mdv2(n.get('author') or '—')
        plat_esc = escape_mdv2(n.get('platform') or '—')
        date_esc = escape_mdv2(n.get('date', '')[:10] if n.get('date') else '—')
        
        text += f"*{num_esc}* {name_esc}\n   Author: {author_esc} \\| Platform: {plat_esc}\n"
        if admin:
            chan_esc = escape_mdv2(n.get('channel') or '—')
            story_esc = escape_mdv2(n.get('story_name') or '—')
            text += f"   Channel: {chan_esc} \\| Story: {story_esc}\n"
        text += f"   Date: {date_esc}\n"
        text += f"   [🖼 View Image](callback_data:viewimg_{n['id']})\n\n"

    total_pages = (total + limit - 1) // limit
    current_page = offset // limit
    keyboard = build_pagination_keyboard(current_page, total_pages, "search_page")
    
    if keyboard:
        await reply_func(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard, disable_web_page_preview=True)
    else:
        await reply_func(text, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

async def search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])
    context.user_data["search_offset"] = page * 5
    await display_search_page(query.edit_message_text, context, update.effective_user.id)

async def view_image_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    novel_id = query.data.split("_", 1)[1]
    novel = await db.get_novel_by_id(novel_id)
    if novel:
        await send_photo_by_novel(context, update.effective_chat.id, novel)
    else:
        await query.edit_message_text("❌ Novel not found.")

# ----------------------------------------------------------------------
# Admin panel & listing (with view image)
# ----------------------------------------------------------------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 List novels", callback_data="admin_list")],
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔍 Filtered search", callback_data="admin_filter")],
        [InlineKeyboardButton("🗑 Delete (by ID)", callback_data="admin_delete_prompt")],
        [InlineKeyboardButton("📥 Export DB", callback_data="admin_export")],
    ])
    if update.callback_query:
        await update.callback_query.edit_message_text("🔧 Admin Panel:", reply_markup=keyboard)
    else:
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
    novels, total = await db.get_novels_filtered(
        author=filters.get("author"),
        platform=filters.get("platform"),
        channel=filters.get("channel"),
        offset=offset, limit=5
    )
    if not novels:
        await update.callback_query.edit_message_text("No novels found.")
        return
    
    header_esc = escape_mdv2(f"(filtered) – page {offset//5 + 1}")
    text = f"📋 Novels {header_esc}\n\n"
    
    for n in novels:
        id_str = escape_mdv2(n.get('id') or str(n.get('_id')))
        name_esc = escape_mdv2(n['original_name'])
        author_esc = escape_mdv2(n['author'] or '—')
        plat_esc = escape_mdv2(n['platform'] or '—')
        chan_esc = escape_mdv2(n.get('channel') or '—')
        story_esc = escape_mdv2(n.get('story_name') or '—')
        date_esc = escape_mdv2(n['date'][:10]) if n.get('date') else escape_mdv2('—')
        text += (
            f"`{id_str}`: {name_esc}\n"
            f"   Author: {author_esc} \\| Platform: {plat_esc}\n"
            f"   Channel: {chan_esc} \\| Story: {story_esc}\n"
            f"   Date: {date_esc}\n"
            f"   [🖼 View Image](callback_data:viewimg_{n['id']})\n\n"
        )
    total_pages = (total + 4) // 5
    current_page = offset // 5
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_list_page_{current_page-1}"))
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_page_{current_page+1}"))
    
    keyboard_arr = [buttons] if buttons else []
    keyboard_arr.append([InlineKeyboardButton("🔧 Set filter", callback_data="admin_filter")])
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard_arr), disable_web_page_preview=True)

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
        r"Send filter in format:\n`author:Name`\n`platform:Pratilipi`\n`channel:Channel`\n\n"
        r"Combine with spaces \(e\.g\. `author:John platform:Pratilipi`\)\n"
        r"Or `/skip` to clear all filters\.",
        parse_mode=ParseMode.MARKDOWN_V2
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
    s = await db.get_stats()
    text = f"📊 *Statistics*\nTotal novels: {s['total']}\nDistinct authors: {s['authors']}\nPlatforms used: {s['platforms']}\nChannels: {s['channels']}"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_delete_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Send the ID of the novel to delete:")
    context.user_data["awaiting_delete_id"] = True

async def handle_delete_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_delete_id"):
        return
    novel_id = update.message.text.strip()
    await db.delete_novel(novel_id)
    await update.message.reply_text(f"✅ Novel {novel_id} deleted.")
    context.user_data["awaiting_delete_id"] = False

# ----------------------------------------------------------------------
# Bulk Import Feature
# ----------------------------------------------------------------------
async def bulk_import_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return
    context.user_data["awaiting_bulk_import"] = True
    await update.message.reply_text(
        "Send the text list of novels. Extremely flexible formatting supported.\n\n"
        "Examples:\n"
        "Title = Story Name\n"
        "Title - Story Name\n"
        "Title -; Story Name\n"
        "Title (standalone)"
    )

async def handle_bulk_import_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_bulk_import"):
        return
    
    text = update.message.text
    lines = text.split('\n')
    added = 0
    
    for line in lines:
        line = line.strip()
        if not line: 
            continue
        
        line = re.sub(r'^(?:\d+[\.\)]\s*)?[\(\"\']+|[\)\"\'\✓\s]+$', '', line).strip()
        if not line: 
            continue

        parts = re.split(r'\s*(?:=|[-;]+)\s*', line, maxsplit=1)
        
        novel_name = parts[0].strip()
        story_name = parts[1].strip() if len(parts) > 1 else None

        novel_name = re.sub(r'[\"\'\✓\s]+$', '', novel_name).strip()
        if story_name:
            story_name = re.sub(r'[\"\'\✓\s]+$', '', story_name).strip()

        norm_name = normalize(novel_name)
        if not norm_name:
            continue
            
        exact = await db.get_exact_match(norm_name)
        if not exact:
            await db.add_novel(
                norm_name, novel_name, author=None, platform=None, channel=None, 
                story_name=story_name, file_id=None, chat_id=update.message.chat_id, 
                message_id=update.message.message_id, storage_chat_id=None, 
                storage_message_id=None, sender_id=update.effective_user.id, 
                sender_name=update.effective_user.full_name, added_by_index=1
            )
            added += 1
                
    context.user_data["awaiting_bulk_import"] = False
    await update.message.reply_text(f"✅ Bulk import complete. {added} unique novels processed & added securely.")

# ----------------------------------------------------------------------
# Export Database Feature (CSV)
# ----------------------------------------------------------------------
async def generate_and_send_export(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    novels = await db.get_all_novels()
    if not novels:
        await context.bot.send_message(chat_id=chat_id, text="No novels found to export.")
        return
        
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "ID", "Original Name", "Normalized Name", "Author", 
        "Platform", "Channel", "Story Name", "Sender Name", "Sender ID", "Date"
    ])
    
    for n in novels:
        id_str = n.get('id') or str(n.get('_id'))
        writer.writerow([
            id_str,
            n.get("original_name", ""),
            n.get("normalized_name", ""),
            n.get("author", ""),
            n.get("platform", ""),
            n.get("channel", ""),
            n.get("story_name", ""),
            n.get("sender_name", ""),
            n.get("sender_id", ""),
            n.get("date", "")
        ])
    
    output.seek(0)
    csv_bytes = output.getvalue().encode('utf-8')
    bio = io.BytesIO(csv_bytes)
    bio.name = f"novels_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    
    await context.bot.send_document(
        chat_id=chat_id,
        document=bio,
        caption="📋 Full database export of novels."
    )

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text("⏳ Generating database export file...")
    await generate_and_send_export(update.effective_chat.id, context)

async def admin_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await context.bot.send_message(chat_id=update.effective_chat.id, text="⏳ Generating database export file...")
    await generate_and_send_export(update.effective_chat.id, context)

# ----------------------------------------------------------------------
# Indexing (connects standalone text to standalone photo) 
# ----------------------------------------------------------------------
async def index_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    context.user_data["index_mode"] = True
    context.user_data["indexed_count"] = 0
    context.user_data["pending_index"] = None
    await update.message.reply_text(
        "📥 Index mode ON. Send or forward images and texts to connect them.\n"
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
    if not norm_name:
        return
        
    exact = await db.get_exact_match(norm_name)
    if exact:
        await update.message.reply_text(f"❌ Duplicate: '{novel_name}' already exists. Skipped.")
        return
        
    # User constraint applied: Only skip if matches >= 70%
    fuzzy = await find_similar_novels(novel_name, cutoff=0.70)
    if fuzzy:
        await update.message.reply_text(f"⚠️ Highly similar novels found for '{novel_name}' (>= 70% match). Skipping to avoid duplicates. Use manual add if needed.")
        return
        
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
    await update.message.reply_text(rf"✅ Indexed: {escape_mdv2(novel_name)}", parse_mode=ParseMode.MARKDOWN_V2)

# ----------------------------------------------------------------------
# Primary Message Router
# ----------------------------------------------------------------------
async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ud = context.user_data
    msg = update.message

    if ud.get("index_mode"):
        pending = ud.get("pending_index")
        if msg.photo:
            caption = msg.caption
            if caption:
                await process_indexed_novel(update, context, caption, msg)
                ud.pop("pending_index", None)
            else:
                if pending and pending["type"] == "text":
                    await process_indexed_novel(update, context, pending["text"], msg)
                    ud.pop("pending_index", None)
                else:
                    ud["pending_index"] = {"type": "photo", "msg": msg}
                    await msg.reply_text("📸 Image received. Now send the text/title for it.")
        elif msg.text:
            if pending and pending["type"] == "photo":
                await process_indexed_novel(update, context, msg.text, pending["msg"])
                ud.pop("pending_index", None)
            else:
                ud["pending_index"] = {"type": "text", "text": msg.text}
                await msg.reply_text("📝 Text received. Now send the image for it.")
        return

    if msg.text:
        if ud.get("awaiting_search"):
            await handle_search_text(update, context)
        elif ud.get("awaiting_filter"):
            await handle_filter_text(update, context)
        elif ud.get("awaiting_delete_id"):
            await handle_delete_id(update, context)
        elif ud.get("awaiting_bulk_import"):
            await handle_bulk_import_text(update, context)

# ----------------------------------------------------------------------
# Admin promotion / demotion (super admin only)
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

# ----------------------------------------------------------------------
# Broadcast (admin only)
# ----------------------------------------------------------------------
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
            NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_name),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
            AUTHOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_author),
                CallbackQueryHandler(back_to_name, pattern="^back_to_name$"),
                CallbackQueryHandler(skip_author_callback, pattern="^skip_author$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
            PLATFORM: [
                CallbackQueryHandler(add_platform_callback, pattern="^plat_"),
                CallbackQueryHandler(back_to_author, pattern="^back_to_author$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
            CHANNEL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_channel),
                CallbackQueryHandler(skip_channel_callback, pattern="^skip_channel$"),
                CallbackQueryHandler(back_to_platform, pattern="^back_to_platform$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
            STORY_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_story_name),
                CallbackQueryHandler(skip_story_callback, pattern="^skip_story$"),
                CallbackQueryHandler(back_to_channel, pattern="^back_to_channel$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
            IMAGE: [
                MessageHandler(filters.PHOTO, add_image),
                CallbackQueryHandler(add_image_skip, pattern="^skip_image$"),
                CallbackQueryHandler(back_to_story, pattern="^back_to_story$"),
                CallbackQueryHandler(cancel_add, pattern="^cancel_add$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_add)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(help_menu, pattern="^help_menu$"))
    app.add_handler(CallbackQueryHandler(search_menu, pattern="^search_menu$"))
    app.add_handler(CallbackQueryHandler(search_page_callback, pattern="^search_page_"))
    app.add_handler(CallbackQueryHandler(view_image_callback, pattern="^viewimg_"))
    app.add_handler(CallbackQueryHandler(button_same_diff, pattern="^(same_|diff_)"))
    app.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin$"))
    app.add_handler(CallbackQueryHandler(admin_list_callback, pattern="^admin_list$"))
    app.add_handler(CallbackQueryHandler(admin_list_page_callback, pattern="^admin_list_page_"))
    app.add_handler(CallbackQueryHandler(admin_filter_callback, pattern="^admin_filter$"))
    app.add_handler(CallbackQueryHandler(admin_stats, pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(admin_delete_prompt, pattern="^admin_delete_prompt$"))
    app.add_handler(CallbackQueryHandler(admin_export_callback, pattern="^admin_export$"))
    
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("bulkimport", bulk_import_cmd))
    app.add_handler(CommandHandler("index", index_cmd))
    app.add_handler(CommandHandler("stopindex", stop_index))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("promote", promote_cmd))
    app.add_handler(CommandHandler("demote", demote_cmd))
    
    # Process group photos & text triggers globally
    app.add_handler(MessageHandler(filters.PHOTO & (filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP), handle_group_photo))
    app.add_handler(MessageHandler(filters.TEXT & (filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP) & ~filters.COMMAND, handle_group_text))
    
    # Text and general processing handler allows BOTH Private and Group Chats now
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.FORWARDED) & 
        (filters.ChatType.PRIVATE | filters.ChatType.GROUPS | filters.ChatType.SUPERGROUP) & 
        ~filters.COMMAND, 
        handle_private_message
    ))

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="webhook",
        webhook_url=urljoin(WEBHOOK_URL, "webhook"),
    )

if __name__ == "__main__":
    main()