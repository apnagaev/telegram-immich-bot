import os
import logging
import requests
import asyncio
import re
import shutil
import patoolib
import time
import subprocess
import json
import hashlib
import uuid
import fcntl
from datetime import datetime
from collections import defaultdict
from io import BytesIO
from PIL import Image
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter, TelegramError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters, CommandHandler, CallbackQueryHandler
from telethon import TelegramClient
from telegram.request import HTTPXRequest
import contextlib

# --- TELETHON CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = "telegram_session"

telethon_client = None
TELETHON_SEMAPHORE = asyncio.Semaphore(3)

async def ensure_telethon():
    """Ленивая инициализация Telethon. Вызывается только при необходимости."""
    global telethon_client
    
    if telethon_client is not None:
        if not telethon_client.is_connected():
            try:
                await telethon_client.connect()
            except Exception as e:
                logging.error(f"Не удалось переподключить Telethon: {e}")
                return None
        return telethon_client
    
    if not API_ID or not API_HASH:
        logging.warning("⚠️ TELEGRAM_API_ID или TELEGRAM_API_HASH не заданы")
        return None
    
    session_file = f"{SESSION_NAME}.session"
    if not os.path.exists(session_file):
        logging.error(f"❌ Файл сессии '{session_file}' не найден!")
        return None
    
    try:
        telethon_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await telethon_client.connect()
        if not await telethon_client.is_user_authorized():
            logging.error("❌ Telethon не авторизован! Файл сессии недействителен.")
            await telethon_client.disconnect()
            telethon_client = None
            return None
        logging.info("✅ Telethon авторизован и готов к работе")
        return telethon_client
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации Telethon: {e}", exc_info=True)
        telethon_client = None
        return None

# --- CONFIGURAZIONE ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

TOKEN = os.getenv("TELEGRAM_TOKEN")
IMMICH_URL = os.getenv("IMMICH_URL", "http://immich:2283").rstrip("/")
IMPORT_DIR = os.getenv("IMPORT_DIR", "/import")
HISTORY_FILE = os.path.join(IMPORT_DIR, "bot_history.json")
USER_MAPPING_FILE = os.path.join(IMPORT_DIR, "user_mapping.json")

TEMP_DIR = "/tmp/bot_downloads"
if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

USER_TAGS_MEM = {}
ALBUM_CACHE = {}

REQUIRED_PERMISSIONS = {
    "user.read": "Чтение профиля пользователя (email, имя)",
    "asset": "Базовый доступ к ассетам",
    "asset.read": "Чтение метаданных ассетов (проверка дубликатов)",
    "asset.download": "Скачивание оригиналов файлов",
    "asset.upload": "Загрузка новых фото/видео",
    "asset.update": "Обновление описания ассетов",
    "album": "Базовый доступ к альбомам",
    "album.read": "Поиск существующих альбомов",
    "album.create": "Создание альбомов (Telegram Upload)",
    "albumAsset.create": "Добавление ассетов в альбомы"
}

if not TOKEN:
    logging.error("TELEGRAM_TOKEN не задан!"); exit(1)

# --- ATOMIC JSON ---
def _atomic_json_write(filepath, data):
    temp_path = f"{filepath}.tmp.{os.getpid()}"
    lock_path = f"{filepath}.lock"
    try:
        with open(lock_path, 'a+') as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                with open(temp_path, 'w', encoding='utf-8') as tf:
                    json.dump(data, tf, indent=2, ensure_ascii=False)
                shutil.move(temp_path, filepath)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        logging.error(f"Atomic write error for {filepath}: {e}")
        if os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass

# --- USER MAPPING ---
def load_user_mapping() -> dict:
    if not os.path.exists(USER_MAPPING_FILE):
        try:
            with open(USER_MAPPING_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2)
        except Exception as e:
            logging.error(f"❌ Не удалось создать {USER_MAPPING_FILE}: {e}")
            return {}
    try:
        with open(USER_MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"❌ Ошибка чтения {USER_MAPPING_FILE}: {e}")
        return {}

def save_user_mapping(mapping: dict):
    _atomic_json_write(USER_MAPPING_FILE, mapping)

def get_user_config(telegram_user_id: int) -> dict:
    mapping = load_user_mapping()
    for email, data in mapping.items():
        if str(data.get("telegram_user_id")) == str(telegram_user_id):
            return {
                "email": email,
                "api_key": data.get("immich_api_key"),
                "max_user_id": data.get("max_user_id")
            }
    return None

# --- PERMISSION CHECK ---
def _get_error_detail(resp):
    try:
        data = resp.json()
        if isinstance(data, dict):
            return str(data.get("message") or data.get("error") or "")[:200]
    except: pass
    return ""

async def check_api_key_permissions(api_key: str) -> dict:
    results = {p: {"status": False, "message": "Не проверено"} for p in REQUIRED_PERMISSIONS}
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    try:
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        if resp.status_code == 200:
            for k in ["user.read", "asset", "asset.read"]:
                results[k]["status"] = True; results[k]["message"] = "✅ OK"
    except: pass
    
    try:
        resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers, timeout=10)
        if resp.status_code == 200:
            for k in ["album", "album.read"]:
                results[k]["status"] = True; results[k]["message"] = "✅ OK"
    except: pass
    
    try:
        resp = requests.post(f"{IMMICH_URL}/api/albums", headers=headers, json={"albumName": f"_t_{uuid.uuid4().hex[:8]}"}, timeout=10)
        if resp.status_code in [200, 201]:
            results["album.create"]["status"] = True
            results["album.create"]["message"] = "✅ OK"
            try: requests.delete(f"{IMMICH_URL}/api/albums/{resp.json().get('id')}", headers=headers, timeout=5)
            except: pass
    except: pass
    
    return results

def format_permissions_report(results: dict) -> str:
    lines = []
    has_failure = any(not r["status"] and "ℹ️" not in r["message"] for r in results.values())
    for perm, desc in REQUIRED_PERMISSIONS.items():
        r = results.get(perm, {"status": False, "message": "?"})
        icon = "✅" if r["status"] else "❌"
        lines.append(f"{icon} <code>{perm}</code> — {desc}")
        if not r["status"] and "Не проверено" not in r["message"]:
            lines.append(f"   └─ {r['message']}")
    lines.insert(0, "✅ <b>Все права в порядке!</b>\n" if not has_failure else "⚠️ <b>Есть проблемы с правами:</b>\n")
    return "\n".join(lines)

# --- LOCK MECHANISM ---
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "/shared_storage/bot.lock")
INSTANCE_ID = os.getenv("HOSTNAME", str(uuid.uuid4()))
LOCK_TIMEOUT = int(os.getenv("LOCK_TIMEOUT", "30"))
LOCK_CHECK_INTERVAL = int(os.getenv("LOCK_CHECK_INTERVAL", "10"))

def _validate_lock_dir():
    """Проверяет доступность директории для файла блокировки."""
    lock_dir = os.path.dirname(LOCK_FILE_PATH)
    if not lock_dir:
        return True
    try:
        os.makedirs(lock_dir, exist_ok=True)
        if not os.access(lock_dir, os.W_OK):
            logging.critical(f"❌ Директория '{lock_dir}' НЕ доступна для записи! Проверьте volume в docker-compose.")
            return False
        test_file = os.path.join(lock_dir, ".write_test")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        return True
    except Exception as e:
        logging.critical(f"❌ Ошибка доступа к директории блокировки '{lock_dir}': {e}")
        return False

def try_acquire_lock(lock_path, instance_id, timeout):
    try:
        lock_dir = os.path.dirname(lock_path)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)
        
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
        return True
    except FileExistsError:
        try:
            with open(lock_path, 'r') as f:
                data = json.load(f)
            if time.time() - data.get("last_heartbeat", 0) > timeout:
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            return False
        except (json.JSONDecodeError, KeyError, OSError):
            try:
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            except Exception as e:
                logging.error(f"Не удалось перезаписать повреждённый лок: {e}")
                return False
    except Exception as e:
        logging.error(f"Lock acquisition error: {e}")
        return False

def maintain_lock(lock_path, instance_id):
    try:
        with open(lock_path, 'r') as f:
            data = json.load(f)
        if data.get("instance_id") == instance_id:
            data["last_heartbeat"] = time.time()
            with open(lock_path, 'w') as f:
                json.dump(data, f)
            return True
    except Exception as e:
        logging.warning(f"Lock maintenance error: {e}")
    return False

def release_lock(lock_path, instance_id):
    try:
        if os.path.exists(lock_path):
            with open(lock_path, 'r') as f:
                data = json.load(f)
            if data.get("instance_id") == instance_id:
                os.remove(lock_path)
                logging.info("🔓 Lock released.")
    except Exception as e:
        logging.warning(f"Lock release error: {e}")

# --- UTILS ---
def fix_perms(path):
    try:
        if os.path.isdir(path): os.chmod(path, 0o777)
        else: os.chmod(path, 0o666)
    except: pass

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {"albums": [], "photographers": [], "last_chat_id": None}
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if "last_chat_id" not in data: data["last_chat_id"] = None
            return data
    except:
        return {"albums": [], "photographers": [], "last_chat_id": None}

def save_history(data):
    _atomic_json_write(HISTORY_FILE, data)

def update_history(album, photographer, chat_id=None):
    data = load_history()
    changed = False
    if album:
        if album in data["albums"]: data["albums"].remove(album)
        data["albums"].insert(0, album)
        data["albums"] = data["albums"][:10]
        changed = True
    if photographer:
        if photographer in data["photographers"]: data["photographers"].remove(photographer)
        data["photographers"].insert(0, photographer)
        data["photographers"] = data["photographers"][:10]
        changed = True
    if chat_id:
        data["last_chat_id"] = chat_id
        changed = True
    if changed: save_history(data)

def get_or_create_album(requested_name, api_key):
    if not requested_name or len(str(requested_name).strip()) < 2:
        requested_name = "Telegram Upload"
    
    api_key_hash = hashlib.md5(api_key.encode()).hexdigest()[:12]
    cache_key = (api_key_hash, str(requested_name).lower())
    
    if cache_key in ALBUM_CACHE:
        return ALBUM_CACHE[cache_key]

    headers = {"x-api-key": api_key, "Accept": "application/json"}
    try:
        resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers, timeout=10)
        if resp.status_code == 200:
            for album in resp.json():
                if album["albumName"].lower() == str(requested_name).lower():
                    ALBUM_CACHE[cache_key] = album["id"]
                    return album["id"]
        
        fallback_name = "Telegram Upload"
        fallback_cache_key = (api_key_hash, fallback_name.lower())
        if fallback_cache_key in ALBUM_CACHE:
            return ALBUM_CACHE[fallback_cache_key]

        cr = requests.post(f"{IMMICH_URL}/api/albums", headers=headers, json={"albumName": fallback_name}, timeout=10)
        if cr.status_code in [200, 201]:
            nid = cr.json()["id"]
            ALBUM_CACHE[fallback_cache_key] = nid
            return nid
    except Exception as e:
        logging.error(f"Ошибка работы с альбомами: {e}")
    return None

def parse_tags(text):
    if not text: return None, None
    album, fotografo = None, None
    alb_m = re.search(r"#album\s+([^#\n\/]+)", text, re.IGNORECASE)
    if alb_m: album = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "", alb_m.group(1).strip(), flags=re.IGNORECASE).strip()
    foto_m = re.search(r"#fotografo\s+([^#\n\/]+)", text, re.IGNORECASE)
    if foto_m: fotografo = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "", foto_m.group(1).strip(), flags=re.IGNORECASE).strip()
    return album, fotografo

def clean_caption(text):
    if not text: return None
    cleaned = re.sub(r"#album\s+[^#\n\/]+", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"#fotografo\s+[^#\n\/]+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip() or None

def get_effective_tags(user_id, current_text):
    ta, tf = parse_tags(current_text)
    if ta or tf:
        USER_TAGS_MEM[user_id] = {"tags": [ta, tf], "time": time.time()}
        return ta, tf
    if user_id in USER_TAGS_MEM:
        memo = USER_TAGS_MEM[user_id]
        if time.time() - memo["time"] < 1800: return memo["tags"]
    return None, None

def is_file_stable(filepath, wait_time=5):
    if filepath.endswith(".part"): return False
    if os.path.exists(filepath + ".part"): return False
    try:
        size1 = os.path.getsize(filepath)
        time.sleep(wait_time)
        size2 = os.path.getsize(filepath)
        return size1 == size2
    except: return False

def calculate_checksum(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data: break
            sha1.update(data)
    return sha1.hexdigest()

async def find_asset_by_checksum(checksum, api_key):
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    try:
        resp = requests.post(f"{IMMICH_URL}/api/assets/check", headers=headers, json={"checksums": [checksum]}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("results"):
                return data["results"][0].get("assetId")
    except: pass
    return None

# --- UPLOAD ---
async def upload_file_path_to_immich(file_path, original_name, telegram_date, message_id, user_api_key, override_date=None, extra_album=None, fotografo=None, caption=None):
    try:
        real_date = override_date or telegram_date
        headers = {"x-api-key": user_api_key, "Accept": "application/json"}
        
        with open(file_path, 'rb') as f:
            files = {"assetData": (original_name, f)}
            data = {
                "deviceAssetId": f"tg-{original_name}-{message_id}",
                "deviceId": "telegram-bot",
                "fileCreatedAt": real_date.isoformat(),
                "fileModifiedAt": real_date.isoformat(),
                "isFavorite": "false",
            }
            resp = requests.post(f"{IMMICH_URL}/api/assets", headers=headers, files=files, data=data, timeout=60)
        
        asset_id = None
        status = "error"
        
        if resp.status_code in [200, 201]:
            res_json = resp.json()
            asset_id = res_json.get("id")
            if res_json.get("status") == "duplicate":
                status = "duplicate"
            else:
                status = "success"
        elif resp.status_code == 409: 
             status = "duplicate"
             chk = calculate_checksum(file_path)
             asset_id = await find_asset_by_checksum(chk, user_api_key)
        
        if not asset_id: return status, None

        description_parts = []
        if caption:
            cc = clean_caption(caption)
            if cc: description_parts.append(cc)
        if fotografo:
            description_parts.append(f"Fotografo: {fotografo}")
        
        if description_parts:
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{asset_id}", headers=headers, json={"description": "\n\n".join(description_parts)}, timeout=10)
            except: pass

        target_ids = []
        aid = get_or_create_album(extra_album, user_api_key)
        if aid: target_ids.append(aid)
        if fotografo:
            fid = get_or_create_album(fotografo, user_api_key)
            if fid: target_ids.append(fid)
        
        for alb_id in list(set(target_ids)):
            try:
                requests.put(f"{IMMICH_URL}/api/albums/{alb_id}/assets", headers=headers, json={"ids": [asset_id]}, timeout=10)
            except: pass

        return status, asset_id
    except Exception as e: 
        logging.error(f"Upload error: {e}"); return "error", None

# --- PROCESSORS ---
async def process_directory_content(directory, msg_date, msg_id, user_config: dict, manual_tags=None, bot=None, chat_id=None, caption=None):
    if not user_config or not user_config.get("api_key"):
        return {"success": 0, "duplicate": 0, "error": 0, "unsupported": 0}, []

    user_api_key = user_config["api_key"]
    total_stats = {"success": 0, "duplicate": 0, "error": 0, "unsupported": 0}
    current_job_albums = set()
    
    has_archives = True
    while has_archives:
        has_archives = False
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if d not in ["corrupted", "failed_extraction", "unsupported_files"]]
            for file in files:
                if file.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
                    full_p = os.path.join(root, file)
                    if not is_file_stable(full_p, wait_time=3): continue
                    ext_to = os.path.join(root, "extracted_" + file)
                    os.makedirs(ext_to, exist_ok=True); fix_perms(ext_to)
                    try: 
                        patoolib.extract_archive(full_p, outdir=ext_to)
                        os.remove(full_p)
                        has_archives = True
                        await asyncio.sleep(5)
                    except Exception as e:
                        logging.error(f"Ошибка распаковки {full_p}: {e}")
                        total_stats["error"] += 1
                        
    all_items = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in ["corrupted", "failed_extraction", "unsupported_files"]]
        for file in files: all_items.append((root, file))
    
    total_count = len(all_items); processed_count = 0; progress_msg = None
    if total_count > 5 and bot and chat_id:
        try: progress_msg = await bot.send_message(chat_id=chat_id, text=f"⏳ 0/{total_count}...", parse_mode="HTML")
        except: pass

    for root, file in all_items:
        processed_count += 1; full_p = os.path.join(root, file)
        if not is_file_stable(full_p, wait_time=5): continue
        if file.lower().endswith(('.jpg', '.jpeg', '.png', '.heic', '.mp4', '.mov', '.avi', '.webp',
                                   '.raf', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.rw2', '.orf', '.raw')):
            final_alb = manual_tags[0] if manual_tags and manual_tags[0] else None
            final_foto = manual_tags[1] if manual_tags and len(manual_tags) > 1 and manual_tags[1] else None
            
            rel_p = os.path.relpath(full_p, directory)
            p_alb, p_foto = parse_tags(rel_p)
            if p_alb and not final_alb: final_alb = p_alb
            if p_foto and not final_foto: final_foto = p_foto
            if final_alb: current_job_albums.add(final_alb)
            
            fb_date = msg_date
            if str(msg_id).startswith("watch"):
                try: fb_date = datetime.fromtimestamp(os.path.getmtime(full_p))
                except: pass
            
            status, _ = await upload_file_path_to_immich(
                full_p, file, fb_date, f"{msg_id}-{file}", 
                user_api_key=user_api_key, 
                extra_album=final_alb, 
                fotografo=final_foto,
                caption=caption
            )
            
            if status in ["success", "duplicate"]:
                try: os.remove(full_p)
                except: pass
                total_stats["success" if status == "success" else "duplicate"] += 1
            else: total_stats["error"] += 1
            
            if progress_msg and processed_count % 5 == 0:
                perc = int((processed_count / total_count) * 100)
                try: await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"⏳ {processed_count}/{total_count} ({perc}%)", parse_mode="HTML")
                except: pass
            await asyncio.sleep(1)
        else:
            if root == IMPORT_DIR and file in ["put_files_here.txt", "bot_history.json", "user_mapping.json"]: continue
            unsupported_dir = os.path.join(IMPORT_DIR, "unsupported_files")
            os.makedirs(unsupported_dir, exist_ok=True)
            try: shutil.move(full_p, os.path.join(unsupported_dir, file)); total_stats["unsupported"] += 1
            except: pass

    if progress_msg:
        try: await bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
        except: pass
    
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in dirs:
            d_path = os.path.join(root, name)
            if name in ["corrupted", "failed_extraction", "unsupported_files", "tg-immich-bot"] or d_path == directory: continue
            try: os.rmdir(d_path)
            except: pass
            
    return total_stats, list(current_job_albums)

# --- WATCH FOLDER ---
async def watch_folder_task(app):
    special_dirs = ["corrupted", "failed_extraction", "unsupported_files", "tg-immich-bot"]
    while True:
        try:
            await asyncio.sleep(30)
            if not os.path.exists(IMPORT_DIR): continue
            items = os.listdir(IMPORT_DIR)
            relevant = [i for i in items if i not in special_dirs and i not in ["put_files_here.txt", "bot_history.json", "user_mapping.json"]]
            if not relevant: continue
            
            has_valid_content = False
            for root, dirs, files in os.walk(IMPORT_DIR):
                dirs[:] = [d for d in dirs if d not in special_dirs]
                for file in files:
                    if file.lower().endswith((".jpg", ".jpeg", ".png", ".heic", ".mp4", ".mov", ".avi", ".zip", ".rar", ".7z", ".tar", ".webp",
                                              ".raf", ".cr2", ".cr3", ".nef", ".arw", ".dng", ".rw2", ".orf", ".raw", ".gz")):
                        if is_file_stable(os.path.join(root, file), wait_time=2):
                            has_valid_content = True; break
                if has_valid_content: break
                
            if not has_valid_content: continue
            
            mapping = load_user_mapping()
            if not mapping: continue
            
            first_email = list(mapping.keys())[0]
            default_user_config = {
                "email": first_email,
                "api_key": mapping[first_email]["immich_api_key"],
                "max_user_id": mapping[first_email].get("max_user_id")
            }

            hist = load_history()
            target_chat = hist.get("last_chat_id")
            
            stats, albums = await process_directory_content(
                IMPORT_DIR, datetime.now(), "watchdog", 
                user_config=default_user_config, 
                bot=app.bot, chat_id=target_chat
            )
            
            if target_chat and (sum(stats.values()) > 0):
                report = f"📂 <b>Импорт завершён</b>\n✅ {stats['success']}\n♻️ {stats['duplicate']}"
                if albums: report += f"\n📂 {', '.join(albums)}"
                try: await app.bot.send_message(chat_id=target_chat, text=report, parse_mode="HTML")
                except: pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Watch folder error: {e}")

# --- HANDLERS ---
async def handle_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args
    
    if not args:
        permissions_list = "\n".join([f"• <code>{p}</code> — {d}" for p, d in REQUIRED_PERMISSIONS.items()])
        await update.message.reply_text(
            f"⚙️ <b>Настройка Immich</b>\n\n<code>/setup КЛЮЧ</code>\n\n<b>Права (10):</b>\n{permissions_list}",
            parse_mode="HTML"
        )
        return
    
    api_key = args[0]
    status_msg = await update.message.reply_text("⏳ Проверяю ключ...")
    
    try:
        headers = {"x-api-key": api_key, "Accept": "application/json"}
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        
        if resp.status_code != 200:
            await status_msg.edit_text(f"❌ Неверный ключ. Код: {resp.status_code}")
            return
        
        email = resp.json().get("email")
        if not email:
            await status_msg.edit_text("❌ Не удалось получить email.")
            return
        
        await status_msg.edit_text("⏳ Проверяю права...")
        permissions_results = await check_api_key_permissions(api_key)
        
        mapping = load_user_mapping()
        mapping[email] = {"immich_api_key": api_key, "telegram_user_id": user_id, "max_user_id": None}
        save_user_mapping(mapping)
        
        report = format_permissions_report(permissions_results)
        await status_msg.edit_text(f"✅ <b>Настроено!</b>\n📧 <code>{email}</code>\n\n{report}", parse_mode="HTML")
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {e}")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    
    if not user_config:
        await update.message.reply_text("⚠️ Не настроен.\n<code>/setup КЛЮЧ</code>", parse_mode="HTML")
        return
    
    status_msg = await update.message.reply_text("⏳ Проверяю...")
    
    try:
        permissions_results = await check_api_key_permissions(user_config["api_key"])
        report = format_permissions_report(permissions_results)
        
        text = f"📊 <b>Статус</b>\n📧 <code>{user_config['email']}</code>\n🆔 <code>{user_id}</code>\n\n"
        
        # Статус Telethon
        text += "📱 <b>Telethon:</b> "
        if telethon_client and telethon_client.is_connected():
            text += "✅ Подключен"
        else:
            text += "⚠️ Не активен"
        
        text += f"\n\n{report}"
        await status_msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {e}")

async def send_tags_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user_config(update.effective_user.id):
        await update.message.reply_text("⚠️ Не настроен.")
        return
    update_history(None, None, update.effective_chat.id)
    curr = USER_TAGS_MEM.get(update.effective_user.id, {}).get("tags", [None, None])
    msg_text = "🗂 <b>Теги</b>"
    if curr[0] or curr[1]:
        msg_text += f"\n📂 {curr[0] or '--'}\n👤 {curr[1] or '--'}"
    await update.message.reply_text(msg_text, reply_markup=await get_tags_markup(update.effective_user.id), parse_mode="HTML")

async def get_tags_markup(user_id):
    data = load_history()
    curr = USER_TAGS_MEM.get(user_id, {}).get("tags", [None, None])
    keyboard = []
    if data.get("albums"):
        keyboard.append([InlineKeyboardButton("📂 АЛЬБОМЫ:", callback_data="none")])
        row = []
        for i, alb in enumerate(data["albums"][:6]):
            label = f"✅ {alb}" if curr[0] == alb else alb
            row.append(InlineKeyboardButton(label, callback_data=f"alb_{i}"))
            if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🗑 Сброс", callback_data="reset_tags")])
    return InlineKeyboardMarkup(keyboard)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    if not get_user_config(user_id):
        await query.answer("Не настроен!", show_alert=True); return

    data = query.data; hist = load_history()
    curr = USER_TAGS_MEM.get(user_id, {"tags": [None, None], "time": time.time()})
    if data == "reset_tags": curr["tags"] = [None, None]
    elif data.startswith("alb_"): 
        idx = int(data.split("_")[1])
        if idx < len(hist["albums"]):
            val = hist["albums"][idx]; curr["tags"][0] = None if curr["tags"][0] == val else val
    curr["time"] = time.time(); USER_TAGS_MEM[user_id] = curr
    try: await query.edit_message_text("🗂 Теги", reply_markup=await get_tags_markup(user_id), parse_mode="HTML")
    except: pass

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user_config(update.effective_user.id): return
    text = update.message.text
    if len(text) > 200: return
    ta, tf = parse_tags(text)
    if ta or tf:
        curr_mem = USER_TAGS_MEM.get(update.effective_user.id, {"tags": [None, None], "time": time.time()})
        if ta: curr_mem["tags"][0] = ta
        if tf: curr_mem["tags"][1] = tf
        curr_mem["time"] = time.time(); USER_TAGS_MEM[update.effective_user.id] = curr_mem
        update_history(ta, tf, update.effective_chat.id)
        await update.message.reply_text(f"✅ {curr_mem['tags'][0]} / {curr_mem['tags'][1]}")

async def handle_any_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    if not user_config:
        await update.message.reply_text("⚠️ <b>Не настроен!</b>\n<code>/setup КЛЮЧ</code>", parse_mode="HTML")
        return

    update_history(None, None, update.effective_chat.id)
    msg = update.message
    tags = get_effective_tags(user_id, msg.caption or "")
    raw_caption = msg.caption or None
    file_obj = None; fname = "file.jpg"
    
    if msg.document:
        fname = msg.document.file_name or "file"
        mime = msg.document.mime_type or ""
        file_size = msg.document.file_size or 0
        
        if "image" in mime or "video" in mime or fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
            if file_size > 20 * 1024 * 1024:
                # Пытаемся использовать Telethon для больших файлов
                client = await ensure_telethon()
                if client:
                    job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
                    os.makedirs(job_dir, exist_ok=True)
                    dest_path = os.path.join(job_dir, fname)
                    sm = await msg.reply_text("⏳ <b>Скачивание через Telethon...</b>", parse_mode="HTML")
                    try:
                        async with TELETHON_SEMAPHORE:
                            telethon_msg = await client.get_messages(msg.chat_id, ids=msg.message_id)
                            if not telethon_msg:
                                raise Exception("Telethon не нашёл сообщение")
                            
                            # ВАЖНО: используем возвращаемый путь из download_media
                            result_path = await client.download_media(telethon_msg, file=dest_path)
                            
                            if not result_path:
                                raise Exception("download_media вернул пустой путь")
                            
                            if not os.path.exists(result_path):
                                raise Exception(f"Файл не найден после скачивания: {result_path}")
                            
                            # Если файл скачался без расширения — добавляем .zip
                            if not result_path.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
                                new_path = result_path + ".zip"
                                os.rename(result_path, new_path)
                                result_path = new_path
                                logging.info(f"Переименовано: {result_path}")
                        
                        file_size_actual = os.path.getsize(result_path)
                        logging.info(f"Скачан файл: {result_path} ({file_size_actual} байт)")
                        
                        stats, albums = await process_directory_content(
                            job_dir, msg.date, msg.message_id, 
                            user_config=user_config, manual_tags=tags, 
                            bot=context.bot, chat_id=msg.chat_id, caption=raw_caption
                        )
                        shutil.rmtree(job_dir, ignore_errors=True)
                        
                        rep = f"📦 <b>Обработано!</b>\n✅ {stats['success']}\n♻️ {stats['duplicate']}"
                        if albums: rep += f"\n📂 {', '.join(albums)}"
                        await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
                        return
                    except Exception as e:
                        logging.error(f"Telethon download error: {e}", exc_info=True)
                        await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=f"❌ Ошибка: {e}")
                        shutil.rmtree(job_dir, ignore_errors=True)
                        return
                else:
                    await msg.reply_text(f"⚠️ Файл {file_size / (1024*1024):.1f} МБ слишком большой, а Telethon недоступен.")
                    return
            
            try:
                file_obj = await msg.document.get_file()
            except TelegramError as e:
                if "too big" in str(e).lower():
                    await msg.reply_text(f"❌ Файл слишком большой. Настройте Telethon.")
                    return
                raise
    elif msg.photo:
        file_obj = await msg.photo[-1].get_file(); fname = f"photo_{file_obj.file_unique_id}.jpg"
    elif msg.video:
        file_obj = await msg.video.get_file(); fname = msg.video.file_name or f"video_{file_obj.file_unique_id}.mp4"
                    
    if file_obj:
        if fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
            job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}"); os.makedirs(job_dir, exist_ok=True)
            sm = await msg.reply_text("⏳ <b>Архив...</b>", parse_mode="HTML")
            await file_obj.download_to_drive(os.path.join(job_dir, fname))
            stats, albums = await process_directory_content(
                job_dir, msg.date, msg.message_id, 
                user_config=user_config, manual_tags=tags, 
                bot=context.bot, chat_id=msg.chat_id, caption=raw_caption
            )
            shutil.rmtree(job_dir, ignore_errors=True)
            rep = f"📦 <b>Обработано!</b>\n✅ {stats['success']}\n♻️ {stats['duplicate']}"
            if albums: rep += f"\n📂 {', '.join(albums)}"
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
        else:
            tmp = os.path.join(TEMP_DIR, fname); await file_obj.download_to_drive(tmp)
            status, _ = await upload_file_path_to_immich(
                tmp, fname, msg.date, msg.message_id, 
                user_api_key=user_config["api_key"], 
                extra_album=tags[0] if tags and tags[0] else None, 
                fotografo=tags[1] if tags and len(tags)>1 else None,
                caption=raw_caption
            )
            os.remove(tmp)
            report = "✅ Загружено!" if status == "success" else "♻️ Дубликат."
            await msg.reply_text(report)

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not get_user_config(update.effective_user.id):
        await update.message.reply_text("⚠️ Не настроен."); return

    update_history(None, None, update.effective_chat.id)
    url_m = re.search(r'(https?://\S+)', update.message.text)
    if not url_m: return
    url = url_m.group(1); tags = get_effective_tags(update.effective_user.id, update.message.text)
    
    job_dir = os.path.join(TEMP_DIR, f"job_{update.message.message_id}"); os.makedirs(job_dir, exist_ok=True)
    msg = await update.message.reply_text("⏳ <b>Скачивание...</b>", parse_mode="HTML")
    success = False
    
    try:
        if any(x in url for x in ["youtube", "youtu.be", "instagram", "tiktok"]):
            cmd = ["yt-dlp", "-o", f"{job_dir}/%(title)s.%(ext)s", url]
        else:
            cmd = ["curl", "-L", "-J", "-O", "--output-dir", job_dir, url]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.wait()
        success = (proc.returncode == 0)
    except Exception as e:
        logging.error(f"Download error: {e}")
    
    downloaded_files = [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]
    if not success or not downloaded_files:
        try:
            cmd = ["wget", "--content-disposition", "-P", job_dir, url]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
        except: pass
                    
    stats, albums = await process_directory_content(
        job_dir, update.message.date, update.message.message_id, 
        user_config=user_config, manual_tags=tags, 
        bot=context.bot, chat_id=update.message.chat_id
    )
    shutil.rmtree(job_dir, ignore_errors=True)
    rep = f"✅ {stats['success']}\n♻️ {stats['duplicate']}"
    if albums: rep += f"\n📂 {', '.join(albums)}"
    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id, text=rep, parse_mode="HTML")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, TelegramError):
        logging.warning(f"⚠️ Telegram: {error}")
    else:
        logging.error("❌ Ошибка:", exc_info=error)

# --- MAIN LOOP ---
async def main():
    # Проверяем доступность директории блокировки ДО старта
    if not _validate_lock_dir():
        logging.critical("Продолжение работы без HA-блокировки. Возможен конфликт контейнеров!")
    
    await app.initialize()
    await app.start()
    
    is_active = False
    watch_task = None
    
    async def activate():
        nonlocal is_active, watch_task
        await app.updater.start_polling(drop_pending_updates=False)
        watch_task = asyncio.create_task(watch_folder_task(app))
        is_active = True
        logging.info("🔒 ACTIVE mode. Polling started.")
    
    async def deactivate():
        nonlocal is_active, watch_task
        if watch_task:
            watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watch_task
            watch_task = None
        await app.updater.stop()
        is_active = False
        logging.info("⏳ PASSIVE mode. Polling stopped.")
    
    # Начальная проверка блокировки
    if try_acquire_lock(LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
        await activate()
    else:
        logging.info("⏳ PASSIVE mode (lock held by another instance).")
        
    try:
        while True:
            await asyncio.sleep(LOCK_CHECK_INTERVAL)
            
            if is_active:
                if not maintain_lock(LOCK_FILE_PATH, INSTANCE_ID):
                    logging.warning("🔓 Lock lost. Deactivating.")
                    await deactivate()
                    await asyncio.sleep(4)
            else:
                if try_acquire_lock(LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
                    logging.info("🔒 Lock acquired. Activating.")
                    await asyncio.sleep(3)
                    await activate()
                    
    except asyncio.CancelledError:
        logging.info("Main loop cancelled.")
    finally:
        if is_active:
            await app.updater.stop()
        if watch_task and not watch_task.done():
            watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watch_task
        
        # Отключаем Telethon при завершении
        if telethon_client and telethon_client.is_connected():
            try: await telethon_client.disconnect()
            except: pass
        
        release_lock(LOCK_FILE_PATH, INSTANCE_ID)
        await app.stop()
        await app.shutdown()
        logging.info("🛑 Bot shutdown complete.")

if __name__ == "__main__":
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=30.0,
        pool_timeout=30.0
    )
    
    app = ApplicationBuilder().token(TOKEN).request(request).build()
    
    app.add_error_handler(error_handler)
    app.add_handler(CommandHandler("setup", handle_setup))
    app.add_handler(CommandHandler("status", handle_status))
    app.add_handler(CommandHandler("start", send_tags_menu))
    app.add_handler(CommandHandler("tags", send_tags_menu))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Entity("url") | filters.Entity("text_link"), handle_url))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.VIDEO_NOTE | filters.Document.ALL, handle_any_media))
    
    print("Bot avviato (v60 - Lazy Telethon + Lock Fix)...")
    asyncio.run(main())