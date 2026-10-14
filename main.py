import os
import logging
import requests
import asyncio
import re
import shutil
import patoolib
import time
import subprocess
import myjdapi
import json
import hashlib
import uuid
from datetime import datetime
from collections import defaultdict
from io import BytesIO
from PIL import Image, ExifTags
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter, TelegramError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters, CommandHandler, CallbackQueryHandler
from telethon.tl.types import DocumentAttributeFilename
from telethon import TelegramClient
from telegram.request import HTTPXRequest
import contextlib

# --- TELETHON CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = "telegram_session"

telethon_client = None
if API_ID and API_HASH:
    logging.info(f"🔧 Инициализация Telethon: API_ID={API_ID}, SESSION={SESSION_NAME}")
    session_file = f"{SESSION_NAME}.session"
    if not os.path.exists(session_file):
        logging.error(f"❌ Файл сессии '{session_file}' не найден! Создайте его локально и пробросьте в контейнер.")
    else:
        try:
            telethon_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
            
            async def init_telethon():
                await telethon_client.connect()
                logging.info("✅ Telethon подключен")
                if not await telethon_client.is_user_authorized():
                    logging.error("❌ Telethon не авторизован! Файл сессии недействителен.")
                    await telethon_client.disconnect()
                    return None
                logging.info("✅ Telethon авторизован и готов к работе")
                return telethon_client
            
            loop = asyncio.get_event_loop()
            telethon_client = loop.run_until_complete(init_telethon())
        except Exception as e:
            logging.error(f"❌ Ошибка инициализации Telethon: {e}", exc_info=True)
            telethon_client = None
else:
    logging.warning("⚠️ TELEGRAM_API_ID или TELEGRAM_API_HASH не заданы")

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

# JDownloader Config
MYJD_USER = os.getenv("MYJD_USER")
MYJD_PASSWORD = os.getenv("MYJD_PASSWORD")
MYJD_DEVICE = os.getenv("MYJD_DEVICE", "jdownloader")

TEMP_DIR = "/tmp/bot_downloads"
if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

USER_TAGS_MEM = {}
ALBUM_CACHE = {}

if not TOKEN:
    logging.error("Variabile d'ambiente TELEGRAM_TOKEN mancante!"); exit(1)

# --- USER MAPPING FUNCTIONS ---
def load_user_mapping() -> dict:
    """Загружает или создает файл маппинга пользователей."""
    if not os.path.exists(USER_MAPPING_FILE):
        try:
            with open(USER_MAPPING_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2)
            logging.info(f"✅ Создан новый файл маппинга: {USER_MAPPING_FILE}")
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
    """Сохраняет файл маппинга пользователей."""
    try:
        with open(USER_MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2)
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения {USER_MAPPING_FILE}: {e}")

def get_user_config(telegram_user_id: int) -> dict:
    """Ищет конфигурацию пользователя по Telegram ID. Возвращает None, если не найден."""
    mapping = load_user_mapping()
    for email, data in mapping.items():
        if str(data.get("telegram_user_id")) == str(telegram_user_id):
            return {
                "email": email,
                "api_key": data.get("immich_api_key"),
                "max_user_id": data.get("max_user_id")
            }
    return None

# --- LOCK MECHANISM ---
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "/shared_storage/bot.lock")
INSTANCE_ID = os.getenv("HOSTNAME", str(uuid.uuid4()))
LOCK_TIMEOUT = int(os.getenv("LOCK_TIMEOUT", "30"))
LOCK_CHECK_INTERVAL = int(os.getenv("LOCK_CHECK_INTERVAL", "10"))

def try_acquire_lock(lock_path, instance_id, timeout):
    try:
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
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
        except Exception:
            try:
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            except Exception:
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
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f: json.dump(data, f)
    except Exception as e: logging.error(f"History save error: {e}")

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
    """Ищет альбом по имени. Если не находит или имя пустое, создает 'Telegram Upload'."""
    if not requested_name or len(str(requested_name).strip()) < 2:
        requested_name = "Telegram Upload"
    
    cache_key = (str(requested_name).lower(), api_key[:8])
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
        fallback_cache_key = (fallback_name.lower(), api_key[:8])
        if fallback_cache_key in ALBUM_CACHE:
            return ALBUM_CACHE[fallback_cache_key]

        logging.info(f"Альбом '{requested_name}' не найден. Создаю '{fallback_name}'...")
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

def get_effective_tags(user_id, current_text):
    ta, tf = parse_tags(current_text)
    if ta or tf:
        USER_TAGS_MEM[user_id] = {"tags": [ta, tf], "time": time.time()}
        return ta, tf
    if user_id in USER_TAGS_MEM:
        memo = USER_TAGS_MEM[user_id]
        if time.time() - memo["time"] < 1800: return memo["tags"]
    return None, None

def send_to_jdownloader(url, tags=(None, None)):
    if not MYJD_USER or not MYJD_PASSWORD:
        return False, "Credenziali JDownloader mancanti."
    dest_folder = "/output"
    tag_parts = []
    if tags and len(tags) > 0 and tags[0]: tag_parts.append(f"#album {tags[0]}")
    if tags and len(tags) > 1 and tags[1]: tag_parts.append(f"#fotografo {tags[1]}")
    if tag_parts:
        tag_str = " ".join(tag_parts)
        safe_tag_str = re.sub(r'[\\*?:\"<>|]', "", tag_str).strip()
        dest_folder = f"/output/{safe_tag_str}"
    try:
        jd = myjdapi.Myjdapi()
        jd.connect(MYJD_USER, MYJD_PASSWORD)
        jd.update_devices()
        device = jd.get_device(MYJD_DEVICE)
        if not device: return False, f"Device '{MYJD_DEVICE}' non trovato."
        device.linkgrabber.add_links([{"autostart": True, "links": url, "destinationFolder": dest_folder}])
        return True, "Link inviato a JDownloader!"
    except Exception as e: 
        logging.error(f"JD Error: {e}"); return False, f"Errore: {e}"

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
            if data.get("results") and len(data["results"]) > 0:
                existing = data["results"][0]
                if existing.get("action") == "reject":
                    return existing.get("assetId")
    except Exception as e:
        logging.error(f"Ошибка проверки checksum: {e}")
    return None

# --- UPLOAD ---
async def upload_file_path_to_immich(file_path, original_name, telegram_date, message_id, user_api_key, override_date=None, extra_album=None, fotografo=None):
    try:
        if fotografo: logging.info(f"Uploading {original_name} with Photographer: {fotografo}")
        
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
                if not asset_id:
                    chk = calculate_checksum(file_path)
                    asset_id = await find_asset_by_checksum(chk, user_api_key)
            else:
                status = "success"
        elif resp.status_code == 409: 
             status = "duplicate"
             chk = calculate_checksum(file_path)
             asset_id = await find_asset_by_checksum(chk, user_api_key)
        
        if not asset_id: return status, None

        if fotografo:
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{asset_id}", headers=headers, json={"description": f"Fotografo: {fotografo}"}, timeout=10)
            except Exception as e: logging.error(f"Desc upd error: {e}")

        target_ids = []
        aid = get_or_create_album(extra_album, user_api_key)
        if aid: target_ids.append(aid)
        
        if fotografo:
            fid = get_or_create_album(fotografo, user_api_key)
            if fid: target_ids.append(fid)
        
        for alb_id in list(set(target_ids)):
            try:
                requests.put(f"{IMMICH_URL}/api/albums/{alb_id}/assets", headers=headers, json={"ids": [asset_id]}, timeout=10)
            except Exception as e: 
                logging.warning(f"Не удалось добавить в альбом {alb_id}: {e}")

        return status, asset_id
    except Exception as e: 
        logging.error(f"Upload error: {e}"); return "error", None

# --- PROCESSORS ---
async def process_directory_content(directory, msg_date, msg_id, user_config: dict, manual_tags=None, bot=None, chat_id=None):
    if not user_config or not user_config.get("api_key"):
        logging.error("Попытка обработки без конфигурации пользователя!")
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
                    if not is_file_stable(full_p, wait_time=2): continue
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
        try: progress_msg = await bot.send_message(chat_id=chat_id, text=f"⏳ <b>Avanzamento:</b> 0/{total_count}...", parse_mode="HTML")
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
            
            if p_foto: logging.info(f"Found photographer in path: {p_foto}")

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
                extra_album=final_alb, fotografo=final_foto
            )
            
            if status in ["success", "duplicate"]:
                try: os.remove(full_p)
                except: pass
                if status == "success": total_stats["success"] += 1
                else: total_stats["duplicate"] += 1
            else: total_stats["error"] += 1
            
            if progress_msg and processed_count % 5 == 0:
                perc = int((processed_count / total_count) * 100)
                try: await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"⏳ <b>Avanzamento:</b> {processed_count}/{total_count} ({perc}%)", parse_mode="HTML")
                except: pass
            await asyncio.sleep(1)
        else:
            if root == IMPORT_DIR and (file == "put_files_here.txt" or file == "bot_history.json" or file == "user_mapping.json"): continue
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
            if name in ["corrupted", "failed_extraction", "unsupported_files", "jdownloader", "tg-immich-bot"] or d_path == directory: continue
            try: os.rmdir(d_path)
            except: pass
            
    return total_stats, list(current_job_albums)

# --- WATCH FOLDER TASK ---
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
            
            # Находим первого настроенного пользователя для Watch Folder
            mapping = load_user_mapping()
            if not mapping:
                logging.warning("⚠️ Watch folder: файлы обнаружены, но нет настроенных пользователей. Пропуск.")
                continue
            
            first_email = list(mapping.keys())[0]
            default_user_config = {
                "email": first_email,
                "api_key": mapping[first_email]["immich_api_key"],
                "max_user_id": mapping[first_email].get("max_user_id")
            }

            hist = load_history()
            target_chat = hist.get("last_chat_id")
            
            if target_chat:
                try: await app.bot.send_message(chat_id=target_chat, text=f"📂 <b>Importazione Automatica Iniziata!</b> (User: {default_user_config['email']})", parse_mode="HTML")
                except: pass
                
            stats, albums = await process_directory_content(
                IMPORT_DIR, datetime.now(), "watchdog", 
                user_config=default_user_config, 
                bot=app.bot, chat_id=target_chat
            )
            
            if target_chat and (sum(stats.values()) > 0):
                report = f"📂 <b>Importazione Completata!</b>\n✅ {stats['success']} Caricati\n♻️ {stats['duplicate']} Duplicati"
                if albums: report += f"\n📂 <b>Album:</b> {', '.join(albums)}"
                if stats['unsupported'] > 0: report += f"\n⚠️ {stats['unsupported']} Non supportati"
                if stats['error'] > 0: report += f"\n❌ {stats['error']} Errori"
                try: await app.bot.send_message(chat_id=target_chat, text=report, parse_mode="HTML")
                except: pass
        except asyncio.CancelledError:
            logging.info("Watch folder task cancelled.")
            break

# --- HANDLERS ---
async def handle_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /setup <api_key>"""
    user_id = update.effective_user.id
    args = context.args
    
    if not args:
        await update.message.reply_text(
            "⚙️ <b>Настройка аккаунта Immich</b>\n\n"
            "Используйте команду:\n"
            "<code>/setup ВАШ_API_КЛЮЧ</code>\n\n"
            "Где взять ключ:\n"
            "1. Откройте Immich\n"
            "2. Перейдите в Настройки -> Безопасность -> Ключи API\n"
            "3. Создайте новый ключ и скопируйте его сюда.",
            parse_mode="HTML"
        )
        return
    
    api_key = args[0]
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    await update.message.reply_text("⏳ Проверяю ключ и получаю данные аккаунта...")
    
    try:
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        if resp.status_code == 200:
            user_info = resp.json()
            email = user_info.get("email")
            
            if not email:
                await update.message.reply_text("❌ Не удалось получить email из Immich. Проверьте ключ.")
                return
            
            mapping = load_user_mapping()
            mapping[email] = {
                "immich_api_key": api_key,
                "telegram_user_id": user_id,
                "max_user_id": None
            }
            save_user_mapping(mapping)
            
            await update.message.reply_text(
                f"✅ <b>Аккаунт успешно настроен!</b>\n\n"
                f"📧 Email: <code>{email}</code>\n"
                f"🆔 Telegram ID: <code>{user_id}</code>\n\n"
                f"Теперь вы можете отправлять фото и видео, и они будут загружаться в ваш личный профиль Immich.",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(f"❌ Неверный API ключ или ошибка подключения к Immich. (Код ответа: {resp.status_code})\nПроверьте ключ и переменную IMMICH_URL.")
    except requests.exceptions.ConnectionError:
        await update.message.reply_text("❌ Не удалось подключиться к Immich. Проверьте переменную IMMICH_URL в настройках бота.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при проверке ключа: {e}")

async def send_tags_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    
    if not user_config:
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key> для начала работы.")
        return

    update_history(None, None, update.effective_chat.id)
    curr = USER_TAGS_MEM.get(user_id, {}).get("tags", [None, None])
    msg_text = "🗂 <b>Gestione Tag</b>\nSeleziona album o fotografo dai tasti sotto."
    if curr[0] or curr[1]:
        msg_text += f"\n\n🔹 <b>Attivi:</b>\n📂 Album: {curr[0] or '--'}\n👤 Foto: {curr[1] or '--'}"
    await update.message.reply_text(msg_text, reply_markup=await get_tags_markup(user_id), parse_mode="HTML")

async def get_tags_markup(user_id):
    data = load_history()
    curr = USER_TAGS_MEM.get(user_id, {}).get("tags", [None, None])
    keyboard = []
    if data.get("albums"):
        keyboard.append([InlineKeyboardButton("📂 ALBUM RECENTI:", callback_data="none")])
        row = []
        for i, alb in enumerate(data["albums"][:6]):
            label = f"✅ {alb}" if curr[0] == alb else alb
            row.append(InlineKeyboardButton(label, callback_data=f"alb_{i}"))
            if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
    if data.get("photographers"):
        keyboard.append([InlineKeyboardButton("👤 FOTOGRAFI:", callback_data="none")])
        row = []
        for i, foto in enumerate(data["photographers"][:4]):
            label = f"✅ {foto}" if curr[1] == foto else foto
            row.append(InlineKeyboardButton(label, callback_data=f"foto_{i}"))
            if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🗑 Cancella Tag", callback_data="reset_tags")])
    return InlineKeyboardMarkup(keyboard)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    
    user_config = get_user_config(user_id)
    if not user_config:
        await query.answer("Аккаунт не настроен!", show_alert=True)
        return

    data = query.data; hist = load_history()
    curr = USER_TAGS_MEM.get(user_id, {"tags": [None, None], "time": time.time()})
    if data == "reset_tags": curr["tags"] = [None, None]
    elif data.startswith("alb_"): 
        idx = int(data.split("_")[1])
        if idx < len(hist["albums"]):
            val = hist["albums"][idx]; curr["tags"][0] = None if curr["tags"][0] == val else val
    elif data.startswith("foto_"):
        idx = int(data.split("_")[1])
        if idx < len(hist["photographers"]):
            val = hist["photographers"][idx]; curr["tags"][1] = None if curr["tags"][1] == val else val
    curr["time"] = time.time(); USER_TAGS_MEM[user_id] = curr
    msg_text = "🗂 <b>Gestione Tag</b>\nSeleziona album o fotografo dai tasti sotto."
    if curr["tags"][0] or curr["tags"][1]:
        msg_text += f"\n\n🔹 <b>Attivi:</b>\n📂 Album: {curr['tags'][0] or '--'}\n👤 Foto: {curr['tags'][1] or '--'}"
    try: await query.edit_message_text(msg_text, reply_markup=await get_tags_markup(user_id), parse_mode="HTML")
    except: pass

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    
    if not user_config:
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key> для начала работы.")
        return

    text = update.message.text
    if len(text) > 200: return
    ta, tf = parse_tags(text)
    if ta or tf:
        curr_mem = USER_TAGS_MEM.get(user_id, {"tags": [None, None], "time": time.time()})
        if ta: curr_mem["tags"][0] = ta
        if tf: curr_mem["tags"][1] = tf
        curr_mem["time"] = time.time(); USER_TAGS_MEM[user_id] = curr_mem
        update_history(ta, tf, update.effective_chat.id)
        await update.message.reply_text(f"✅ Tag aggiornati!\n📂 {curr_mem['tags'][0]}\n👤 {curr_mem['tags'][1]}")

async def handle_any_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    
    if not user_config:
        await update.message.reply_text(
            "⚠️ <b>Аккаунт не настроен!</b>\n\n"
            "Прежде чем отправлять файлы, настройте свой аккаунт Immich:\n"
            "<code>/setup ВАШ_API_КЛЮЧ</code>",
            parse_mode="HTML"
        )
        return

    update_history(None, None, update.effective_chat.id)
    msg = update.message; tags = get_effective_tags(user_id, msg.caption or "")
    file_obj = None; fname = "file.jpg"
    
    if msg.document:
        fname = msg.document.file_name or "file"
        mime = msg.document.mime_type or ""
        file_size = msg.document.file_size or 0
        
        if "image" in mime or "video" in mime or fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
            if file_size > 20 * 1024 * 1024 and telethon_client:
                job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
                os.makedirs(job_dir, exist_ok=True)
                dest_path = os.path.join(job_dir, fname)
                
                sm = await msg.reply_text("⏳ <b>Скачивание большого файла через Telethon...</b>", parse_mode="HTML")
                
                try:
                    telethon_msg = await telethon_client.get_messages(msg.chat_id, ids=msg.message_id)
                    await telethon_client.download_media(telethon_msg, file=dest_path)
                    stats, albums = await process_directory_content(
                        job_dir, msg.date, msg.message_id, 
                        user_config=user_config, 
                        manual_tags=tags, bot=context.bot, chat_id=msg.chat_id
                    )
                    shutil.rmtree(job_dir)
                    rep = f"📦 <b>Большой архив обработан!</b>\n✅ {stats['success']} Фото\n♻️ {stats['duplicate']} Дубликатов"
                    if albums: rep += f"\n📂 <b>Альбомы:</b> {', '.join(albums)}"
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
                    return
                except Exception as e:
                    logging.error(f"Telethon download error: {e}")
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=f"❌ Ошибка скачивания: {e}")
                    shutil.rmtree(job_dir, ignore_errors=True)
                    return
            
            try:
                file_obj = await msg.document.get_file()
            except TelegramError as e:
                if "too big" in str(e).lower():
                    await msg.reply_text(f"❌ Файл слишком большой ({file_size / (1024*1024):.1f} МБ). Настройте Telethon или используйте папку импорта.")
                    return
                raise

    elif msg.photo:
        file_obj = await msg.photo[-1].get_file(); fname = f"photo_{file_obj.file_unique_id}.jpg"
    elif msg.video:
        file_obj = await msg.video.get_file(); fname = msg.video.file_name or f"video_{file_obj.file_unique_id}.mp4"
    elif msg.video_note:
        file_size = msg.video_note.file_size or 0
        
        if file_size > 20 * 1024 * 1024 and telethon_client:
            job_dir = os.path.join(TEMP_DIR, f"vnote_{msg.message_id}")
            os.makedirs(job_dir, exist_ok=True)
            fname = f"video_note_{msg.video_note.file_unique_id}.mp4"
            dest_path = os.path.join(job_dir, fname)
            
            sm = await msg.reply_text("⏳ <b>Скачивание большого видеосообщения через Telethon...</b>", parse_mode="HTML")
            
            try:
                telethon_msg = await telethon_client.get_messages(msg.chat_id, ids=msg.message_id)
                await telethon_client.download_media(telethon_msg, file=dest_path)
                status, _ = await upload_file_path_to_immich(
                    dest_path, fname, msg.date, msg.message_id,
                    user_api_key=user_config["api_key"],
                    extra_album=tags[0] if tags and tags[0] else None,
                    fotografo=tags[1] if tags and len(tags) > 1 else None
                )
                shutil.rmtree(job_dir)
                report = "✅ Видео загружено!" if status == "success" else "♻️ Дубликат (теги обновлены)."
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=report, parse_mode="HTML")
                return
            except Exception as e:
                logging.error(f"Telethon video_note download error: {e}")
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=f"❌ Ошибка скачивания: {e}")
                shutil.rmtree(job_dir, ignore_errors=True)
                return
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                file_obj = await msg.video_note.get_file()
                fname = f"video_note_{file_obj.file_unique_id}.mp4"
                break
            except TelegramError as e:
                if "timed out" in str(e).lower() and attempt < max_retries - 1:
                    logging.warning(f"Таймаут при скачивании video_note, попытка {attempt + 1}/{max_retries}")
                    await asyncio.sleep(2)
                    continue
                else:
                    logging.error(f"Ошибка скачивания video_note: {e}")
                    await msg.reply_text(f"❌ Не удалось скачать видеосообщение: {e}")
                    return
                    
    if file_obj:
        if fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
            job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}"); os.makedirs(job_dir, exist_ok=True)
            sm = await msg.reply_text("⏳ <b>Archivio ricevuto...</b>", parse_mode="HTML")
            await file_obj.download_to_drive(os.path.join(job_dir, fname))
            stats, albums = await process_directory_content(
                job_dir, msg.date, msg.message_id, 
                user_config=user_config, 
                manual_tags=tags, bot=context.bot, chat_id=msg.chat_id
            )
            shutil.rmtree(job_dir)
            rep = f"📦 <b>Elaborato!</b>\n✅ {stats['success']} Foto\n♻️ {stats['duplicate']} Duplicati"
            if albums: rep += f"\n📂 <b>Album:</b> {', '.join(albums)}"
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
        else:
            tmp = os.path.join(TEMP_DIR, fname); await file_obj.download_to_drive(tmp)
            status, _ = await upload_file_path_to_immich(
                tmp, fname, msg.date, msg.message_id, 
                user_api_key=user_config["api_key"], 
                extra_album=tags[0] if tags and tags[0] else None, 
                fotografo=tags[1] if tags and len(tags)>1 else None
            )
            os.remove(tmp)
            report = "✅ Caricata!" if status == "success" else "♻️ Duplicata (Tag Aggiornati)."
            if tags and tags[0]: report += f"\n📂 Album: {tags[0]}"
            if tags and len(tags)>1 and tags[1]: report += f"\n👤 Foto: {tags[1]}"
            await msg.reply_text(report, parse_mode="HTML")

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    
    if not user_config:
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key> для начала работы.")
        return

    update_history(None, None, update.effective_chat.id)
    url_m = re.search(r'(https?://\S+)', update.message.text)
    if not url_m: return
    url = url_m.group(1); tags = get_effective_tags(user_id, update.message.text)
    JD_DOMAINS = ["mobidrive.com", "fromsmash.com", "swisstransfer.com", "mega.nz", "drive.google.com", "dropbox.com", "1fichier.com", "filecrypt.cc", "mediafire.com"]
    if any(d in url for d in JD_DOMAINS) or "/jd" in update.message.text:
        ok, msg = send_to_jdownloader(url, tags=tags)
        if ok: await update.message.reply_text(f"🦅 <b>JDownloader:</b> Inviato!", parse_mode="HTML")
        else: await update.message.reply_text(f"🦅 <b>JDownloader Errore:</b> {msg}", parse_mode="HTML")
        return
    
    job_dir = os.path.join(TEMP_DIR, f"job_{update.message.message_id}"); os.makedirs(job_dir, exist_ok=True)
    msg = await update.message.reply_text("⏳ <b>Link ricevuto...</b>", parse_mode="HTML")
    success = False
    
    try:
        if "wetransfer.com" in url or "we.tl" in url:
            cmd = ["python3", "/opt/transferwee/transferwee.py", "download", url]
            proc = await asyncio.create_subprocess_exec(*cmd, cwd=job_dir); await proc.wait(); success = (proc.returncode == 0)
        else:
            if any(x in url for x in ["youtube", "youtu.be", "instagram", "tiktok"]):
                cmd = ["yt-dlp", "-o", f"{job_dir}/%(title)s.%(ext)s", url]
            else:
                cmd = ["curl", "-L", "-J", "-O", "--output-dir", job_dir, url]
            proc = await asyncio.create_subprocess_exec(*cmd); await proc.wait(); success = (proc.returncode == 0)
    except Exception as e:
        logging.error(f"Download error: {e}")
        success = False
    
    downloaded_files = [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]
    if not success or not downloaded_files:
        try:
            cmd = ["wget", "--content-disposition", "--trust-server-names", "-P", job_dir, url]
            proc = await asyncio.create_subprocess_exec(*cmd); await proc.wait()
            downloaded_files = [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]
            success = len(downloaded_files) > 0
        except:
            pass
    
    downloaded_files = [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]
    for fname in downloaded_files:
        full_path = os.path.join(job_dir, fname)
        if "?" in fname or "=" in fname or not any(fname.lower().endswith(ext) for ext in ['.zip', '.rar', '.7z', '.jpg', '.jpeg', '.png', '.mp4', '.mov']):
            import urllib.parse
            parsed = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed.query)
            if 'filename' in query_params:
                new_name = query_params['filename'][0]
                new_name = urllib.parse.unquote(new_name)
                new_name = re.sub(r'[<>:"/\\|?*]', '_', new_name)
                new_path = os.path.join(job_dir, new_name)
                try:
                    os.rename(full_path, new_path)
                    logging.info(f"Переименован файл: {fname} -> {new_name}")
                except Exception as e:
                    logging.warning(f"Не удалось переименовать {fname}: {e}")
                    
    stats, albums = await process_directory_content(
        job_dir, update.message.date, update.message.message_id, 
        user_config=user_config, 
        manual_tags=tags, bot=context.bot, chat_id=update.message.chat_id
    )
    shutil.rmtree(job_dir)
    rep = f"✅ <b>Completato!</b>\n{stats['success']} Caricati\n{stats['duplicate']} Duplicati (Aggiornati)"
    if albums: rep += f"\n📂 Album: {', '.join(albums)}"
    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id, text=rep, parse_mode="HTML")


# --- ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, TelegramError): # Базовый класс для всех ошибок telegram
        logging.warning(f"⚠️ Ошибка Telegram: {error}. Повторная попытка будет выполнена автоматически.")
    else:
        logging.error("❌ Неожиданная ошибка в боте:", exc_info=error)


# --- ACTIVE-PASSIVE MAIN LOOP ---
async def main():
    await app.initialize()
    await app.start()
    
    is_active = False
    watch_task = None
    
    if try_acquire_lock(LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
        is_active = True
        await app.updater.start_polling(drop_pending_updates=False)
        watch_task = asyncio.create_task(watch_folder_task(app))
        logging.info("🔒 Lock acquired on start. ACTIVE mode.")
    else:
        logging.info("⏳ Lock held by another instance. PASSIVE mode.")
        
    try:
        while True:
            await asyncio.sleep(LOCK_CHECK_INTERVAL)
            
            if is_active:
                if not maintain_lock(LOCK_FILE_PATH, INSTANCE_ID):
                    logging.warning("🔓 Failed to maintain lock. Stopping updater. PASSIVE mode.")
                    is_active = False
                    
                    if watch_task:
                        watch_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await watch_task
                        watch_task = None
                    
                    await app.updater.stop()
                    await asyncio.sleep(4)
            else:
                if try_acquire_lock(LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
                    logging.info("🔒 Lock acquired. Starting updater. ACTIVE mode.")
                    await asyncio.sleep(3)
                    await app.updater.start_polling(drop_pending_updates=False)
                    watch_task = asyncio.create_task(watch_folder_task(app))
                    is_active = True
                    
    except asyncio.CancelledError:
        logging.info("Main loop cancelled.")
    finally:
        if is_active:
            await app.updater.stop()
        if watch_task and not watch_task.done():
            watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watch_task
        
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
    app.add_handler(CommandHandler("start", send_tags_menu))
    app.add_handler(CommandHandler("tags", send_tags_menu))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Entity("url") | filters.Entity("text_link"), handle_url))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.VIDEO_NOTE | filters.Document.ALL, handle_any_media))
    
    print("Bot avviato (v46 - Open Access with Strict Setup)...")
    asyncio.run(main())
