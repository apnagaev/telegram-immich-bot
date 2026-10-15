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
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import BytesIO
from PIL import Image, ExifTags
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter, TelegramError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters, CommandHandler, CallbackQueryHandler
from telethon.tl.types import DocumentAttributeFilename
from telethon import TelegramClient
from telegram.request import HTTPXRequest
import contextlib

# --- CONFIGURAZIONE ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

TOKEN = os.getenv("TELEGRAM_TOKEN")
IMMICH_URL = os.getenv("IMMICH_URL", "http://immich:2283").rstrip("/")
IMPORT_DIR = os.getenv("IMPORT_DIR", "/import")
HISTORY_FILE = os.path.join(IMPORT_DIR, "bot_history.json")
USER_MAPPING_FILE = os.path.join(IMPORT_DIR, "user_mapping.json")

TEMP_DIR = os.getenv("TEMP_DIR", "/tmp/bot_downloads")
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

INSTANCE_ID = os.getenv("HOSTNAME", str(uuid.uuid4()))
INSTANCE_SHORT = hashlib.md5(INSTANCE_ID.encode()).hexdigest()[:8]

# --- CONCURRENCY CONFIG ---
# Глобальные лимиты защищают сеть/диск, per-user лимиты не дают одному
# пользователю monopolizirovat' все ресурсы.
MAX_CONCURRENT_TELETHON_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_TELETHON_DOWNLOADS", "4"))
MAX_CONCURRENT_EXTRACTS           = int(os.getenv("MAX_CONCURRENT_EXTRACTS", "2"))
MAX_CONCURRENT_UPLOADS            = int(os.getenv("MAX_CONCURRENT_UPLOADS", "6"))
MAX_CONCURRENT_UPLOADS_PER_JOB    = int(os.getenv("MAX_CONCURRENT_UPLOADS_PER_JOB", "3"))
MAX_CONCURRENT_JOBS_PER_USER      = int(os.getenv("MAX_CONCURRENT_JOBS_PER_USER", "2"))
IO_MAX_WORKERS                    = int(os.getenv("IO_MAX_WORKERS", "16"))
CPU_MAX_WORKERS                   = int(os.getenv("CPU_MAX_WORKERS", "4"))
WATCH_INTERVAL                    = int(os.getenv("WATCH_INTERVAL", "30"))

IO_EXECUTOR  = ThreadPoolExecutor(max_workers=IO_MAX_WORKERS,  thread_name_prefix="io")
CPU_EXECUTOR = ThreadPoolExecutor(max_workers=CPU_MAX_WORKERS, thread_name_prefix="cpu")

TELETHON_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TELETHON_DOWNLOADS)
EXTRACT_SEMAPHORE  = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTS)
UPLOAD_SEMAPHORE   = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

USER_SEMAPHORES: dict = {}
USER_SEMAPHORES_LOCK = asyncio.Lock()


async def run_io(func, *args, **kwargs):
    """Запуск блокирующей I/O-операции в пуле потоков (не блокирует event loop)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(IO_EXECUTOR, partial(func, *args, **kwargs))


async def run_cpu(func, *args, **kwargs):
    """Запуск тяжёлой операции (распаковка, архивы) в отдельном пуле потоков."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(CPU_EXECUTOR, partial(func, *args, **kwargs))


async def get_user_semaphore(key: str) -> asyncio.Semaphore:
    """Персональный семафор: один юзер не может занять больше N слотов одновременно."""
    async with USER_SEMAPHORES_LOCK:
        sem = USER_SEMAPHORES.get(key)
        if sem is None:
            sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS_PER_USER)
            USER_SEMAPHORES[key] = sem
        return sem


# --- CONSTANTS ---
SUPPORTED_MEDIA_EXTS = ('.jpg', '.jpeg', '.png', '.heic', '.mp4', '.mov', '.avi', '.webp',
                        '.raf', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.rw2', '.orf', '.raw')
ARCHIVE_EXTS = (".zip", ".rar", ".7z", ".tar", ".gz")
WATCH_EXTS = SUPPORTED_MEDIA_EXTS + ARCHIVE_EXTS
SPECIAL_DIRS = {"corrupted", "failed_extraction", "unsupported_files", "tg-immich-bot"}
SERVICE_FILES = {"put_files_here.txt", "bot_history.json", "user_mapping.json"}

# --- TELETHON CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = "telegram_session"

telethon_client = None


async def init_telethon_client():
    """
    Каждый экземпляр копирует сессию в /tmp с уникальным суффиксом,
    чтобы контейнеры не дрались за одну базу SQLite.
    """
    global telethon_client
    if not (API_ID and API_HASH):
        logging.warning("⚠️ TELEGRAM_API_ID или TELEGRAM_API_HASH не заданы")
        return

    src = f"{SESSION_NAME}.session"
    if not await run_io(os.path.exists, src):
        logging.error(f"❌ Файл сессии '{src}' не найден!")
        return

    local_session = f"/tmp/{SESSION_NAME}_{INSTANCE_SHORT}"

    try:
        await run_io(shutil.copy2, src, local_session + ".session")
        logging.info(f"📋 Сессия скопирована в {local_session}.session")
    except Exception as e:
        logging.error(f"❌ Не удалось скопировать сессию: {e}")
        return

    try:
        telethon_client = TelegramClient(local_session, API_ID, API_HASH)
        await telethon_client.connect()
        if not await telethon_client.is_user_authorized():
            logging.error("❌ Telethon не авторизован!")
            await telethon_client.disconnect()
            telethon_client = None
        else:
            logging.info("✅ Telethon подключен и авторизован")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации Telethon: {e}", exc_info=True)
        telethon_client = None


# --- GLOBALS ---
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
    logging.error("Variabile d'ambiente TELEGRAM_TOKEN mancante!")
    exit(1)


# --- ATOMIC JSON WRITE ---
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
            try:
                os.remove(temp_path)
            except Exception:
                pass


# --- USER MAPPING ---
def load_user_mapping() -> dict:
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


async def get_user_config_async(telegram_user_id: int) -> dict:
    return await run_io(get_user_config, telegram_user_id)


# --- NOTIFICATIONS TO FIRST USER ---
_LAST_ROLE_NOTIFY = {"role": None, "ts": 0.0}


async def notify_first_user(app, text: str) -> bool:
    """Отправляет уведомление первому пользователю из user_mapping.json. Никогда не бросает исключений."""
    try:
        mapping = await run_io(load_user_mapping)
        if not mapping:
            logging.info("📣 Уведомление пропущено: user_mapping.json пуст")
            return False
        first_email = list(mapping.keys())[0]
        tg_id = (mapping.get(first_email) or {}).get("telegram_user_id")
        if not tg_id:
            logging.info(f"📣 Уведомление пропущено: у {first_email} нет telegram_user_id")
            return False
        await app.bot.send_message(chat_id=tg_id, text=text, parse_mode="HTML")
        logging.info(f"📣 Уведомление отправлено {first_email} (id={tg_id})")
        return True
    except Exception as e:
        logging.warning(f"Не удалось отправить уведомление первому пользователю: {e}")
        return False


async def notify_bot_started(app, is_active: bool):
    role = "🟢 ACTIVE" if is_active else "🟡 PASSIVE"
    text = (
        f"🚀 <b>Бот запущен</b>\n\n"
        f"🆔 Instance: <code>{INSTANCE_SHORT}</code>\n"
        f"🎭 Роль: {role}\n"
        f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"⚙️ <b>Лимиты параллельности:</b>\n"
        f"• Telethon-загрузок: {MAX_CONCURRENT_TELETHON_DOWNLOADS}\n"
        f"• Распаковок архивов: {MAX_CONCURRENT_EXTRACTS}\n"
        f"• Загрузок в Immich: {MAX_CONCURRENT_UPLOADS}\n"
        f"• Задач на пользователя: {MAX_CONCURRENT_JOBS_PER_USER}\n"
        f"• IO-потоков: {IO_MAX_WORKERS} / CPU-потоков: {CPU_MAX_WORKERS}"
    )
    await notify_first_user(app, text)
    _LAST_ROLE_NOTIFY["role"] = "ACTIVE" if is_active else "PASSIVE"
    _LAST_ROLE_NOTIFY["ts"] = time.time()


async def notify_role_change(app, new_role: str, reason: str = ""):
    """Уведомление о смене роли с защитой от спама при частом переключении."""
    now = time.time()
    if _LAST_ROLE_NOTIFY["role"] == new_role and (now - _LAST_ROLE_NOTIFY["ts"]) < 60:
        return
    old_role = _LAST_ROLE_NOTIFY["role"] or "—"
    _LAST_ROLE_NOTIFY["role"] = new_role
    _LAST_ROLE_NOTIFY["ts"] = now

    icon = "🟢" if new_role == "ACTIVE" else "🟡"
    text = (
        f"🔄 <b>Смена роли инстанса</b>\n\n"
        f"🆔 Instance: <code>{INSTANCE_SHORT}</code>\n"
        f"{old_role} → {icon} <b>{new_role}</b>\n"
    )
    if reason:
        text += f"ℹ️ Причина: {reason}\n"
    text += f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    await notify_first_user(app, text)


async def notify_bot_stopped(app):
    await notify_first_user(
        app,
        f"🛑 <b>Бот остановлен</b>\n🆔 Instance: <code>{INSTANCE_SHORT}</code>\n"
        f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )


# --- TELETHON SAFE HELPERS ---
async def telethon_get_message_safe(context, chat, message_id, file_size=None, file_name=None,
                                    retries=3, delay=2):
    """
    Надёжное получение сообщения через Telethon.
    Ищет СТРОГО в нужном диалоге (без глобального перебора — иначе подтягиваются
    чужие сообщения с тем же id из других чатов).
    """
    if telethon_client is None:
        raise Exception("Telethon не инициализирован")
    if not telethon_client.is_connected():
        await telethon_client.connect()
    if not await telethon_client.is_user_authorized():
        raise Exception("Telethon не авторизован")

    target_entity = None
    if chat.type == "private":
        bot_id = context.bot.id
        try:
            target_entity = await telethon_client.get_entity(bot_id)
        except Exception:
            async for dialog in telethon_client.iter_dialogs():
                if dialog.id == bot_id:
                    target_entity = dialog.entity
                    break
    else:
        target_entity = chat.id

    if target_entity is None:
        raise Exception("Не удалось определить целевой диалог")

    # Попытка 1: по ID с принудительной синхронизацией кэша
    for attempt in range(retries):
        try:
            await telethon_client.get_messages(target_entity, limit=10)
            msg = await telethon_client.get_messages(target_entity, ids=message_id)
            if msg is not None and getattr(msg, "id", None) == message_id and msg.media:
                logging.info(f"✅ Найдено сообщение {message_id} по ID (попытка {attempt + 1})")
                return msg
        except Exception as e:
            logging.warning(f"get_messages по ID (попытка {attempt + 1}): {e}")
        if attempt < retries - 1:
            await asyncio.sleep(delay)

    # Попытка 2: по размеру и имени файла
    if file_size and file_size > 0:
        logging.info(f"🔎 Поиск по ID не удался, ищу по размеру ({file_size} байт)...")
        try:
            async for msg in telethon_client.iter_messages(target_entity, limit=50):
                if not msg.media:
                    continue
                if getattr(msg, 'document', None) and msg.document.size == file_size:
                    doc_name = next((a.file_name for a in msg.document.attributes
                                     if hasattr(a, 'file_name')), None)
                    if not file_name or not doc_name or doc_name.lower() == file_name.lower():
                        logging.info(f"✅ Найдено по размеру/имени: msg_id={msg.id}")
                        return msg
                if getattr(msg, 'video', None) and msg.video.size == file_size:
                    return msg
                if getattr(msg, 'video_note', None) and msg.video_note.size == file_size:
                    return msg
        except Exception as e:
            logging.error(f"Ошибка итерации сообщений: {e}")

    raise Exception(
        f"Telethon не нашёл сообщение {message_id} в диалоге с ботом "
        f"(chat_id={chat.id}, type={chat.type}). Проверьте, что .session принадлежит "
        f"аккаунту, который отправляет файл боту."
    )


async def telethon_download_to_path(message, dest_path):
    result = await telethon_client.download_media(message, file=dest_path)
    if not result:
        raise Exception("download_media вернул None")

    final_path = result if isinstance(result, str) else dest_path
    if final_path != dest_path and await run_io(os.path.exists, final_path):
        await run_io(shutil.move, final_path, dest_path)
        final_path = dest_path

    if not await run_io(os.path.exists, dest_path):
        raise Exception(f"Файл не появился на диске: {dest_path}")
    if await run_io(os.path.getsize, dest_path) == 0:
        raise Exception(f"Файл пустой: {dest_path}")
    return dest_path


# --- PERMISSION CHECK (полностью синхронная, запускается в пуле потоков) ---
def _get_error_detail(resp):
    try:
        data = resp.json()
        if isinstance(data, dict):
            msg = data.get("message") or data.get("error") or data.get("detail")
            if msg:
                return str(msg)[:200]
            if "errors" in data and isinstance(data["errors"], list):
                return "; ".join(str(e) for e in data["errors"][:3])[:200]
    except Exception:
        pass
    try:
        return resp.text[:200]
    except Exception:
        return ""


def _extract_asset_id_from_response(data):
    if isinstance(data, dict):
        if "assets" in data:
            assets_data = data["assets"]
            if isinstance(assets_data, dict) and "items" in assets_data:
                items = assets_data["items"]
                if items and isinstance(items, list):
                    item = items[0]
                    if isinstance(item, dict) and "id" in item:
                        return item["id"]
            elif isinstance(assets_data, list) and assets_data:
                item = assets_data[0]
                if isinstance(item, dict) and "id" in item:
                    return item["id"]
        if "items" in data and isinstance(data["items"], list) and data["items"]:
            item = data["items"][0]
            if isinstance(item, dict) and "id" in item:
                return item["id"]
        if "result" in data and isinstance(data["result"], dict):
            items = data["result"].get("items", [])
            if items:
                item = items[0]
                if isinstance(item, dict) and "id" in item:
                    return item["id"]
        if "assets" in data and isinstance(data["assets"], list) and data["assets"]:
            item = data["assets"][0]
            if isinstance(item, dict) and "id" in item:
                return item["id"]
    return None


def _get_existing_asset_id(headers):
    attempts = [
        ("POST", f"{IMMICH_URL}/api/search/metadata", {"page": 1, "size": 1}),
        ("POST", f"{IMMICH_URL}/api/search/random", {"count": 1}),
    ]
    for method, endpoint, payload in attempts:
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
            if resp.status_code != 200:
                continue
            asset_id = _extract_asset_id_from_response(resp.json())
            if asset_id:
                return asset_id
        except Exception:
            continue
    return None


def _get_existing_album_id(headers):
    try:
        resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers, timeout=10)
        if resp.status_code == 200:
            albums = resp.json()
            if isinstance(albums, list) and albums:
                for album in albums:
                    if isinstance(album, dict) and album.get("id"):
                        album_name = album.get("albumName", "")
                        if not album_name.startswith("_perm_") and not album_name.startswith("_test_"):
                            return album["id"], album_name
                return albums[0]["id"], albums[0].get("albumName", "")
    except Exception:
        pass
    return None, None


def _check_api_key_permissions_sync(api_key: str) -> dict:
    results = {perm: {"status": False, "message": "Не проверено"} for perm in REQUIRED_PERMISSIONS}
    headers = {"x-api-key": api_key, "Accept": "application/json"}

    # 1. user.read / asset / asset.read
    try:
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        if resp.status_code == 200:
            for k in ("user.read", "asset", "asset.read"):
                results[k] = {"status": True, "message": "✅ OK"}
        elif resp.status_code == 403:
            for k in ("user.read", "asset", "asset.read"):
                results[k] = {"status": False, "message": "❌ Нет доступа (403)"}
        else:
            detail = _get_error_detail(resp)
            results["user.read"] = {"status": False, "message": f"⚠️ Ошибка {resp.status_code}: {detail}"}
    except Exception as e:
        results["user.read"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}

    # 2. album / album.read
    try:
        resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers, timeout=10)
        if resp.status_code == 200:
            results["album"] = {"status": True, "message": "✅ OK"}
            results["album.read"] = {"status": True, "message": "✅ OK"}
        elif resp.status_code == 403:
            results["album.read"] = {"status": False, "message": "❌ Нет доступа (403)"}
        else:
            results["album.read"] = {"status": False,
                                     "message": f"⚠️ Ошибка {resp.status_code}: {_get_error_detail(resp)}"}
    except Exception as e:
        results["album.read"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}

    # 3. album.create
    test_album_id = None
    try:
        test_album_name = f"_perm_test_{uuid.uuid4().hex[:12]}"
        resp = requests.post(f"{IMMICH_URL}/api/albums", headers=headers,
                             json={"albumName": test_album_name}, timeout=10)
        if resp.status_code in (200, 201):
            test_album_id = resp.json().get("id")
            results["album.create"] = {"status": True, "message": "✅ OK"}
            try:
                del_resp = requests.delete(f"{IMMICH_URL}/api/albums/{test_album_id}",
                                           headers=headers, timeout=10)
                if del_resp.status_code not in (200, 204):
                    results["album.create"]["message"] = "✅ OK (альбом не удалён)"
                test_album_id = None
            except Exception:
                pass
        elif resp.status_code == 403:
            results["album.create"] = {"status": False, "message": "❌ Нет доступа (403)"}
        else:
            results["album.create"] = {"status": False,
                                       "message": f"⚠️ Ошибка {resp.status_code}: {_get_error_detail(resp)}"}
    except Exception as e:
        results["album.create"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}
    finally:
        if test_album_id:
            try:
                requests.delete(f"{IMMICH_URL}/api/albums/{test_album_id}", headers=headers, timeout=10)
            except Exception:
                pass

    # 4. Существующий ассет/альбом
    existing_asset_id = _get_existing_asset_id(headers)
    existing_album_id, existing_album_name = _get_existing_album_id(headers)
    created_test_asset = False

    if not existing_asset_id:
        test_file_path = os.path.join(TEMP_DIR, f"perm_test_{uuid.uuid4().hex[:12]}.jpg")
        try:
            img = Image.new('RGB', (1920, 1080), color=(120, 80, 200))
            img.save(test_file_path, format='JPEG', quality=90)
            now_iso = datetime.now().isoformat()
            with open(test_file_path, 'rb') as f:
                files = {"assetData": ("perm_test.jpg", f, "image/jpeg")}
                data = {
                    "deviceAssetId": f"perm-check-{uuid.uuid4().hex}",
                    "deviceId": "tg-immich-bot-perm-check",
                    "fileCreatedAt": now_iso,
                    "fileModifiedAt": now_iso,
                    "isFavorite": "false",
                }
                resp = requests.post(f"{IMMICH_URL}/api/assets", headers=headers,
                                     files=files, data=data, timeout=30)
                if resp.status_code in (200, 201):
                    existing_asset_id = resp.json().get("id")
                    created_test_asset = True
        except Exception:
            pass
        finally:
            if os.path.exists(test_file_path):
                try:
                    os.remove(test_file_path)
                except Exception:
                    pass

    # 5. asset.download
    if existing_asset_id:
        try:
            dl = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}/original",
                              headers=headers, timeout=30)
            if dl.status_code == 200:
                results["asset.download"] = {"status": True, "message": "✅ OK"}
            elif dl.status_code == 403:
                results["asset.download"] = {"status": False, "message": "❌ Нет доступа (403)"}
            else:
                results["asset.download"] = {"status": False, "message": f"⚠️ Ошибка {dl.status_code}"}
        except Exception as e:
            results["asset.download"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}
    else:
        results["asset.download"] = {"status": False, "message": "ℹ️ Невозможно проверить (нет ассетов)"}

    # 6. asset.update
    if existing_asset_id:
        original_description = ""
        try:
            gr = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers, timeout=10)
            if gr.status_code == 200:
                original_description = gr.json().get("description") or ""
            ur = requests.put(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers,
                              json={"description": f"perm_test_{uuid.uuid4().hex[:8]}"}, timeout=10)
            if ur.status_code == 200:
                results["asset.update"] = {"status": True, "message": "✅ OK"}
            elif ur.status_code == 403:
                results["asset.update"] = {"status": False, "message": "❌ Нет доступа (403)"}
            else:
                results["asset.update"] = {"status": False, "message": f"⚠️ Ошибка {ur.status_code}"}
        except Exception as e:
            results["asset.update"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}
        finally:
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers,
                             json={"description": original_description}, timeout=10)
            except Exception:
                pass
    else:
        results["asset.update"] = {"status": False, "message": "ℹ️ Невозможно проверить (нет ассетов)"}

    # 7. albumAsset.create
    if existing_asset_id and existing_album_id:
        try:
            r = requests.put(f"{IMMICH_URL}/api/albums/{existing_album_id}/assets", headers=headers,
                             json={"ids": [existing_asset_id]}, timeout=10)
            if r.status_code == 200:
                results["albumAsset.create"] = {"status": True,
                                                "message": f"✅ OK (добавлен в '{existing_album_name}')"}
            elif r.status_code == 403:
                results["albumAsset.create"] = {"status": False, "message": "❌ Нет доступа (403)"}
            else:
                results["albumAsset.create"] = {"status": False, "message": f"⚠️ Ошибка {r.status_code}"}
        except Exception as e:
            results["albumAsset.create"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}
    else:
        results["albumAsset.create"] = {"status": False,
                                        "message": "ℹ️ Невозможно проверить (нет ассетов или альбомов)"}

    # 8. asset.upload
    if existing_asset_id and not created_test_asset:
        try:
            gr = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers, timeout=10)
            if gr.status_code == 200:
                info = gr.json()
                device_asset_id = info.get("deviceAssetId", f"perm-check-{uuid.uuid4().hex}")
                original_filename = info.get("originalFileName", "test.jpg")
                file_created_at = info.get("fileCreatedAt", datetime.now().isoformat())
                file_modified_at = info.get("fileModifiedAt", datetime.now().isoformat())

                dl = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}/original",
                                  headers=headers, timeout=30)
                if dl.status_code == 200:
                    files = {"assetData": (original_filename, BytesIO(dl.content))}
                    data = {
                        "deviceAssetId": device_asset_id,
                        "deviceId": "tg-immich-bot-perm-check",
                        "fileCreatedAt": file_created_at,
                        "fileModifiedAt": file_modified_at,
                        "isFavorite": "false",
                    }
                    r = requests.post(f"{IMMICH_URL}/api/assets", headers=headers,
                                      files=files, data=data, timeout=30)
                    if r.status_code in (200, 201):
                        rj = r.json()
                        st = rj.get("status", "")
                        if st == "duplicate":
                            results["asset.upload"] = {"status": True, "message": "✅ OK (duplicate)"}
                        elif st == "created":
                            results["asset.upload"] = {"status": True, "message": "✅ OK (created)"}
                            nid = rj.get("id")
                            if nid:
                                try:
                                    requests.delete(f"{IMMICH_URL}/api/assets", headers=headers,
                                                    json={"ids": [nid]}, timeout=10)
                                except Exception:
                                    pass
                        else:
                            results["asset.upload"] = {"status": True, "message": "✅ OK"}
                    elif r.status_code == 409:
                        results["asset.upload"] = {"status": True, "message": "✅ OK (duplicate 409)"}
                    elif r.status_code == 403:
                        results["asset.upload"] = {"status": False, "message": "❌ Нет доступа (403)"}
                    else:
                        results["asset.upload"] = {"status": False,
                                                   "message": f"⚠️ Ошибка {r.status_code}: {_get_error_detail(r)}"}
        except Exception as e:
            results["asset.upload"] = {"status": False, "message": f"❌ Ошибка: {str(e)[:80]}"}
    elif created_test_asset:
        results["asset.upload"] = {"status": True, "message": "✅ OK (через тестовый ассет)"}
    else:
        results["asset.upload"] = {"status": False, "message": "ℹ️ Невозможно проверить (нет ассетов)"}

    if created_test_asset and existing_asset_id:
        try:
            requests.delete(f"{IMMICH_URL}/api/assets", headers=headers,
                            json={"ids": [existing_asset_id]}, timeout=10)
        except Exception:
            pass

    return results


async def check_api_key_permissions(api_key: str) -> dict:
    """Не блокирует event loop — вся тяжёлая работа в пуле потоков."""
    return await run_io(_check_api_key_permissions_sync, api_key)


def format_permissions_report(results: dict) -> str:
    lines = []
    has_failure = False
    has_info = False

    for perm, description in REQUIRED_PERMISSIONS.items():
        status = results.get(perm, {}).get("status", False)
        message = results.get(perm, {}).get("message", "Не проверено")
        is_info = "ℹ️" in message or "Невозможно проверить" in message

        if is_info:
            icon, has_info = "ℹ️", True
        elif status:
            icon = "✅"
        else:
            icon, has_failure = "❌", True

        lines.append(f"{icon} <code>{perm}</code> — {description}")
        if not status and not is_info and "Не проверено" not in message:
            lines.append(f"   └─ {message}")

    if not has_failure:
        if has_info:
            lines.insert(0, "✅ <b>Основные права присутствуют!</b>\n")
            lines.append("\nℹ️ <i>Некоторые права не удалось проверить автоматически "
                         "(нет ассетов/альбомов). Это нормально для нового аккаунта.</i>")
        else:
            lines.insert(0, "✅ <b>Все необходимые права присутствуют!</b>\n")
    else:
        lines.insert(0, "⚠️ <b>Некоторые права отсутствуют:</b>\n")
        lines.append("\n💡 Создайте новый API-ключ и вручную отметьте ровно 10 разрешений из списка выше.")

    return "\n".join(lines)


# --- LOCK MECHANISM ---
LOCK_FILE_PATH = os.getenv("LOCK_FILE_PATH", "/shared_storage/bot.lock")
LOCK_TIMEOUT = int(os.getenv("LOCK_TIMEOUT", "30"))
LOCK_CHECK_INTERVAL = int(os.getenv("LOCK_CHECK_INTERVAL", "10"))


def try_acquire_lock(lock_path, instance_id, timeout):
    try:
        d = os.path.dirname(lock_path)
        if d:
            os.makedirs(d, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
        logging.info(f"🔒 Lock создан: {lock_path} ({instance_id})")
        return True
    except FileExistsError:
        try:
            with open(lock_path, 'r') as f:
                data = json.load(f)
            age = time.time() - data.get("last_heartbeat", 0)
            owner = data.get("instance_id", "?")
            if age > timeout:
                logging.warning(f"⚠️ Lock от '{owner}' протух ({age:.0f}s) — перехватываю")
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            return False
        except Exception as e:
            logging.error(f"❌ Чтение lock failed: {e}, перезаписываю")
            try:
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            except Exception:
                return False
    except Exception as e:
        logging.error(f"❌ Lock acquisition error ({lock_path}): {e}", exc_info=True)
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
        os.chmod(path, 0o777 if os.path.isdir(path) else 0o666)
    except Exception:
        pass


def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {"albums": [], "photographers": [], "last_chat_id": None}
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            data.setdefault("last_chat_id", None)
            return data
    except Exception:
        return {"albums": [], "photographers": [], "last_chat_id": None}


def save_history(data):
    _atomic_json_write(HISTORY_FILE, data)


def update_history(album, photographer, chat_id=None):
    data = load_history()
    changed = False
    if album:
        if album in data["albums"]:
            data["albums"].remove(album)
        data["albums"].insert(0, album)
        data["albums"] = data["albums"][:10]
        changed = True
    if photographer:
        if photographer in data["photographers"]:
            data["photographers"].remove(photographer)
        data["photographers"].insert(0, photographer)
        data["photographers"] = data["photographers"][:10]
        changed = True
    if chat_id:
        data["last_chat_id"] = chat_id
        changed = True
    if changed:
        save_history(data)


async def update_history_async(album, photographer, chat_id=None):
    await run_io(update_history, album, photographer, chat_id)


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

        logging.info(f"Альбом '{requested_name}' не найден. Создаю '{fallback_name}'...")
        cr = requests.post(f"{IMMICH_URL}/api/albums", headers=headers,
                           json={"albumName": fallback_name}, timeout=10)
        if cr.status_code in (200, 201):
            nid = cr.json()["id"]
            ALBUM_CACHE[fallback_cache_key] = nid
            return nid
    except Exception as e:
        logging.error(f"Ошибка работы с альбомами: {e}")
    return None


def parse_tags(text):
    if not text:
        return None, None
    album, fotografo = None, None
    alb_m = re.search(r"#album\s+([^#\n\/]+)", text, re.IGNORECASE)
    if alb_m:
        album = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "",
                       alb_m.group(1).strip(), flags=re.IGNORECASE).strip()
    foto_m = re.search(r"#fotografo\s+([^#\n\/]+)", text, re.IGNORECASE)
    if foto_m:
        fotografo = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "",
                           foto_m.group(1).strip(), flags=re.IGNORECASE).strip()
    return album, fotografo


def clean_caption(text):
    if not text:
        return None
    cleaned = re.sub(r"#album\s+[^#\n\/]+", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"#fotografo\s+[^#\n\/]+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip() if cleaned.strip() else None


def get_effective_tags(user_id, current_text):
    ta, tf = parse_tags(current_text)
    if ta or tf:
        USER_TAGS_MEM[user_id] = {"tags": [ta, tf], "time": time.time()}
        return ta, tf
    if user_id in USER_TAGS_MEM:
        memo = USER_TAGS_MEM[user_id]
        if time.time() - memo["time"] < 1800:
            return memo["tags"]
    return None, None


def _stat_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def is_file_stable(filepath, wait_time=5):
    """СИНХРОННАЯ версия — вызывать только из пула потоков!"""
    if filepath.endswith(".part"):
        return False
    if os.path.exists(filepath + ".part"):
        return False
    if wait_time <= 0:
        return _stat_size(filepath) > 0
    size1 = _stat_size(filepath)
    if size1 <= 0:
        return False
    time.sleep(wait_time)
    size2 = _stat_size(filepath)
    return size1 == size2 and size1 > 0


async def is_file_stable_async(filepath, wait_time=0):
    """АСИНХРОННАЯ версия — не блокирует event loop."""
    if filepath.endswith(".part"):
        return False
    if await run_io(os.path.exists, filepath + ".part"):
        return False
    if wait_time <= 0:
        return (await run_io(_stat_size, filepath)) > 0
    size1 = await run_io(_stat_size, filepath)
    if size1 <= 0:
        return False
    await asyncio.sleep(wait_time)
    size2 = await run_io(_stat_size, filepath)
    return size1 == size2 and size1 > 0


def calculate_checksum(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def find_asset_by_checksum_sync(checksum, api_key):
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    try:
        resp = requests.post(f"{IMMICH_URL}/api/assets/check", headers=headers,
                             json={"checksums": [checksum]}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("results"):
                existing = data["results"][0]
                if existing.get("action") == "reject":
                    return existing.get("assetId")
    except Exception as e:
        logging.error(f"Ошибка проверки checksum: {e}")
    return None


# --- UPLOAD ---
def _upload_file_path_to_immich_sync(file_path, original_name, telegram_date, message_id,
                                     user_api_key, override_date=None, extra_album=None,
                                     fotografo=None, caption=None):
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
            resp = requests.post(f"{IMMICH_URL}/api/assets", headers=headers,
                                 files=files, data=data, timeout=120)

        asset_id = None
        status = "error"

        if resp.status_code in (200, 201):
            rj = resp.json()
            asset_id = rj.get("id")
            if rj.get("status") == "duplicate":
                status = "duplicate"
                if not asset_id:
                    asset_id = find_asset_by_checksum_sync(calculate_checksum(file_path), user_api_key)
            else:
                status = "success"
        elif resp.status_code == 409:
            status = "duplicate"
            asset_id = find_asset_by_checksum_sync(calculate_checksum(file_path), user_api_key)

        if not asset_id:
            return status, None

        description_parts = []
        if caption:
            cc = clean_caption(caption)
            if cc:
                description_parts.append(cc)
        if fotografo:
            description_parts.append(f"Fotografo: {fotografo}")

        if description_parts:
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{asset_id}", headers=headers,
                             json={"description": "\n\n".join(description_parts)}, timeout=10)
            except Exception as e:
                logging.error(f"Desc upd error: {e}")

        target_ids = []
        aid = get_or_create_album(extra_album, user_api_key)
        if aid:
            target_ids.append(aid)
        if fotografo:
            fid = get_or_create_album(fotografo, user_api_key)
            if fid:
                target_ids.append(fid)

        for alb_id in set(target_ids):
            try:
                requests.put(f"{IMMICH_URL}/api/albums/{alb_id}/assets", headers=headers,
                             json={"ids": [asset_id]}, timeout=10)
            except Exception as e:
                logging.warning(f"Не удалось добавить в альбом {alb_id}: {e}")

        return status, asset_id
    except Exception as e:
        logging.error(f"Upload error: {e}")
        return "error", None


async def upload_file_path_to_immich(file_path, original_name, telegram_date, message_id,
                                     user_api_key, override_date=None, extra_album=None,
                                     fotografo=None, caption=None):
    """Неблокирующая загрузка с глобальным ограничением параллельности."""
    async with UPLOAD_SEMAPHORE:
        return await run_io(
            _upload_file_path_to_immich_sync,
            file_path, original_name, telegram_date, message_id, user_api_key,
            override_date, extra_album, fotografo, caption
        )


# --- FS HELPERS (sync, запускаются в пуле) ---
def _find_archives(directory):
    found = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in SPECIAL_DIRS]
        for f in files:
            if f.lower().endswith(ARCHIVE_EXTS):
                p = os.path.join(root, f)
                if is_file_stable(p, wait_time=1):
                    found.append(p)
    return found


def _collect_items(directory):
    items = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in SPECIAL_DIRS]
        for f in files:
            items.append((root, f))
    return items


def _extract_archive_sync(archive_path):
    ext_to = os.path.join(os.path.dirname(archive_path),
                          "extracted_" + os.path.basename(archive_path))
    os.makedirs(ext_to, exist_ok=True)
    fix_perms(ext_to)
    patoolib.extract_archive(archive_path, outdir=ext_to)
    os.remove(archive_path)
    return ext_to


def _move_unsupported_sync(src):
    unsupported_dir = os.path.join(IMPORT_DIR, "unsupported_files")
    os.makedirs(unsupported_dir, exist_ok=True)
    base = os.path.basename(src)
    dest = os.path.join(unsupported_dir, base)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(base)
        dest = os.path.join(unsupported_dir, f"{stem}_{int(time.time() * 1000)}{ext}")
    shutil.move(src, dest)


def _cleanup_dirs_sync(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in dirs:
            d_path = os.path.join(root, name)
            if name in SPECIAL_DIRS or d_path == directory:
                continue
            try:
                os.rmdir(d_path)
            except Exception:
                pass


def _rmtree_sync(path):
    shutil.rmtree(path, ignore_errors=True)


def _scan_import_dir_has_content():
    for root, dirs, files in os.walk(IMPORT_DIR):
        dirs[:] = [d for d in dirs if d not in SPECIAL_DIRS]
        for file in files:
            if file.lower().endswith(WATCH_EXTS):
                if is_file_stable(os.path.join(root, file), wait_time=2):
                    return True
    return False


# --- PROCESSORS ---
async def process_directory_content(directory, msg_date, msg_id, user_config: dict,
                                    manual_tags=None, bot=None, chat_id=None, caption=None):
    if not user_config or not user_config.get("api_key"):
        logging.error("Попытка обработки без конфигурации пользователя!")
        return {"success": 0, "duplicate": 0, "error": 0, "unsupported": 0}, []

    user_api_key = user_config["api_key"]
    total_stats = {"success": 0, "duplicate": 0, "error": 0, "unsupported": 0}
    current_job_albums = set()

    # ── 1. Распаковка архивов (параллельно, в CPU-пуле, не блокирует loop) ──
    for round_no in range(12):
        archives = await run_io(_find_archives, directory)
        if not archives:
            break
        logging.info(f"📦 Раунд {round_no + 1}: найдено архивов — {len(archives)}")

        async def _extract_one(path):
            async with EXTRACT_SEMAPHORE:
                try:
                    await run_cpu(_extract_archive_sync, path)
                    return True
                except Exception as e:
                    logging.error(f"Ошибка распаковки {path}: {e}")
                    total_stats["error"] += 1
                    return False

        await asyncio.gather(*[_extract_one(p) for p in archives], return_exceptions=True)
    else:
        logging.warning("⚠️ Слишком глубокая вложенность архивов — распаковка остановлена")

    # ── 2. Собираем список файлов ──
    all_items = await run_io(_collect_items, directory)
    total_count = len(all_items)
    if total_count == 0:
        return total_stats, list(current_job_albums)

    progress_msg = None
    if total_count > 5 and bot and chat_id:
        try:
            progress_msg = await bot.send_message(
                chat_id=chat_id,
                text=f"⏳ <b>Avanzamento:</b> 0/{total_count}...",
                parse_mode="HTML"
            )
        except Exception:
            pass

    job_sem = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS_PER_JOB)
    progress_lock = asyncio.Lock()
    state = {"done": 0}

    async def _process_one(root, file):
        async with job_sem:
            full_p = os.path.join(root, file)
            if not await is_file_stable_async(full_p, wait_time=0):
                return

            if file.lower().endswith(SUPPORTED_MEDIA_EXTS):
                final_alb = manual_tags[0] if manual_tags and manual_tags[0] else None
                final_foto = manual_tags[1] if manual_tags and len(manual_tags) > 1 and manual_tags[1] else None

                rel_p = os.path.relpath(full_p, directory)
                p_alb, p_foto = parse_tags(rel_p)
                if p_alb and not final_alb:
                    final_alb = p_alb
                if p_foto and not final_foto:
                    final_foto = p_foto
                if final_alb:
                    current_job_albums.add(final_alb)

                fb_date = msg_date
                if str(msg_id).startswith("watch"):
                    try:
                        fb_date = datetime.fromtimestamp(await run_io(os.path.getmtime, full_p))
                    except Exception:
                        pass

                status, _ = await upload_file_path_to_immich(
                    full_p, file, fb_date, f"{msg_id}-{file}",
                    user_api_key=user_api_key,
                    extra_album=final_alb,
                    fotografo=final_foto,
                    caption=caption
                )

                if status in ("success", "duplicate"):
                    try:
                        await run_io(os.remove, full_p)
                    except Exception:
                        pass
                    total_stats["success" if status == "success" else "duplicate"] += 1
                else:
                    total_stats["error"] += 1
            else:
                if root == IMPORT_DIR and file in SERVICE_FILES:
                    return
                try:
                    await run_io(_move_unsupported_sync, full_p)
                    total_stats["unsupported"] += 1
                except Exception:
                    pass

            if progress_msg and bot and chat_id:
                async with progress_lock:
                    state["done"] += 1
                    done = state["done"]
                if done % 5 == 0 or done == total_count:
                    perc = int((done / total_count) * 100)
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg.message_id,
                            text=f"⏳ <b>Avanzamento:</b> {done}/{total_count} ({perc}%)",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

    CHUNK = 64
    for i in range(0, total_count, CHUNK):
        chunk = all_items[i:i + CHUNK]
        await asyncio.gather(*[_process_one(r, f) for r, f in chunk], return_exceptions=True)

    if progress_msg:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
        except Exception:
            pass

    await run_io(_cleanup_dirs_sync, directory)
    return total_stats, list(current_job_albums)


# --- WATCH FOLDER TASK ---
async def watch_folder_task(app):
    while True:
        try:
            await asyncio.sleep(WATCH_INTERVAL)
            if not await run_io(os.path.exists, IMPORT_DIR):
                continue

            items = await run_io(os.listdir, IMPORT_DIR)
            relevant = [i for i in items if i not in SPECIAL_DIRS and i not in SERVICE_FILES]
            if not relevant:
                continue

            has_content = await run_io(_scan_import_dir_has_content)
            if not has_content:
                continue

            mapping = await run_io(load_user_mapping)
            if not mapping:
                logging.warning("⚠️ Watch folder: нет настроенных пользователей. Пропуск.")
                continue

            first_email = list(mapping.keys())[0]
            default_user_config = {
                "email": first_email,
                "api_key": mapping[first_email]["immich_api_key"],
                "max_user_id": mapping[first_email].get("max_user_id")
            }

            hist = await run_io(load_history)
            target_chat = hist.get("last_chat_id")

            if target_chat:
                try:
                    await app.bot.send_message(
                        chat_id=target_chat,
                        text=f"📂 <b>Importazione Automatica Iniziata!</b> (User: {default_user_config['email']})",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            stats, albums = await process_directory_content(
                IMPORT_DIR, datetime.now(), "watchdog",
                user_config=default_user_config,
                bot=app.bot, chat_id=target_chat
            )

            if target_chat and sum(stats.values()) > 0:
                report = (f"📂 <b>Importazione Completata!</b>\n"
                          f"✅ {stats['success']} Caricati\n♻️ {stats['duplicate']} Duplicati")
                if albums:
                    report += f"\n📂 <b>Album:</b> {', '.join(albums)}"
                if stats['unsupported'] > 0:
                    report += f"\n⚠️ {stats['unsupported']} Non supportati"
                if stats['error'] > 0:
                    report += f"\n❌ {stats['error']} Errori"
                try:
                    await app.bot.send_message(chat_id=target_chat, text=report, parse_mode="HTML")
                except Exception:
                    pass
        except asyncio.CancelledError:
            logging.info("Watch folder task cancelled.")
            break
        except Exception as e:
            logging.error(f"Watch folder error: {e}", exc_info=True)


# --- HANDLERS ---
async def handle_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args

    if not args:
        permissions_list = "\n".join([f"• <code>{p}</code> — {d}" for p, d in REQUIRED_PERMISSIONS.items()])
        await update.message.reply_text(
            f"⚙️ <b>Настройка аккаунта Immich</b>\n\n"
            f"Используйте команду:\n<code>/setup ВАШ_API_КЛЮЧ</code>\n\n"
            f"<b>Необходимые права API-ключа (ровно 10):</b>\n{permissions_list}\n\n"
            f"<b>Как создать ключ:</b>\n"
            f"1. Откройте Immich → Профиль → Account Settings\n"
            f"2. Перейдите в раздел <b>API Keys</b>\n"
            f"3. Нажмите <b>New API Key</b>\n"
            f"4. Вручную отметьте все 10 разрешений из списка выше\n"
            f"5. Нажмите <b>Generate</b> и скопируйте ключ\n"
            f"6. Отправьте: <code>/setup &lt;ключ&gt;</code>\n\n"
            f"💡 <b>Важно:</b> Не используйте разрешение <b>All</b>.",
            parse_mode="HTML"
        )
        return

    api_key = args[0]
    status_msg = await update.message.reply_text("⏳ Проверяю ключ и необходимые права...")

    try:
        def _fetch_me():
            return requests.get(f"{IMMICH_URL}/api/users/me",
                                headers={"x-api-key": api_key, "Accept": "application/json"},
                                timeout=10)

        resp = await run_io(_fetch_me)
        if resp.status_code != 200:
            await status_msg.edit_text(f"❌ Неверный API ключ или ошибка подключения.\nКод: {resp.status_code}")
            return

        email = resp.json().get("email")
        if not email:
            await status_msg.edit_text("❌ Не удалось получить email из Immich.")
            return

        await status_msg.edit_text("⏳ Проверяю наличие всех необходимых прав API-ключа...")
        permissions_results = await check_api_key_permissions(api_key)

        has_failure = any(
            not r.get("status", False)
            and "ℹ️" not in r.get("message", "")
            and "Невозможно проверить" not in r.get("message", "")
            for r in permissions_results.values()
        )

        def _save():
            mapping = load_user_mapping()
            mapping[email] = {"immich_api_key": api_key, "telegram_user_id": user_id, "max_user_id": None}
            save_user_mapping(mapping)

        await run_io(_save)

        report = format_permissions_report(permissions_results)
        if not has_failure:
            final = (f"✅ <b>Аккаунт успешно настроен!</b>\n\n📧 Email: <code>{email}</code>\n"
                     f"🆔 Telegram ID: <code>{user_id}</code>\n\n{report}\n\n"
                     f"Теперь вы можете отправлять фото и видео.")
        else:
            final = (f"⚠️ <b>Аккаунт настроен, но есть проблемы с правами!</b>\n\n📧 Email: <code>{email}</code>\n"
                     f"🆔 Telegram ID: <code>{user_id}</code>\n\n{report}\n\n"
                     f"💡 Проверьте разрешения и повторите /setup.")

        await status_msg.edit_text(final, parse_mode="HTML")
    except requests.exceptions.ConnectionError:
        await status_msg.edit_text("❌ Не удалось подключиться к Immich. Проверьте IMMICH_URL.")
    except Exception as e:
        logging.error(f"Ошибка в handle_setup: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка при проверке ключа: {e}")


async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = await get_user_config_async(user_id)

    if not user_config:
        await update.message.reply_text("⚠️ <b>Аккаунт не настроен!</b>\n\n<code>/setup ВАШ_API_КЛЮЧ</code>",
                                        parse_mode="HTML")
        return

    status_msg = await update.message.reply_text("⏳ Получаю информацию и проверяю права...")

    try:
        permissions_results = await check_api_key_permissions(user_config["api_key"])
        report = format_permissions_report(permissions_results)

        has_failure = any(
            not r.get("status", False)
            and "ℹ️" not in r.get("message", "")
            and "Невозможно проверить" not in r.get("message", "")
            for r in permissions_results.values()
        )

        def _me():
            return requests.get(f"{IMMICH_URL}/api/users/me",
                                headers={"x-api-key": user_config["api_key"], "Accept": "application/json"},
                                timeout=10)

        resp = await run_io(_me)
        user_info = resp.json() if resp.status_code == 200 else {}

        st = f"📊 <b>Статус аккаунта</b>\n\n📧 <b>Email:</b> <code>{user_config['email']}</code>\n"
        st += f"🆔 <b>Telegram ID:</b> <code>{user_id}</code>\n"
        if user_info.get('name'):
            st += f"👤 <b>Имя:</b> {user_info.get('name')}\n"

        api_key = user_config["api_key"]
        masked = api_key[:6] + "••••••••" + api_key[-4:] if len(api_key) > 12 else "••••••"
        st += f"🔑 <b>API Key:</b> <code>{masked}</code>\n"

        st += f"\n🖥 <b>Instance:</b> <code>{INSTANCE_SHORT}</code>\n"

        st += "\n📱 <b>Статус Telethon:</b>\n"
        if telethon_client is not None:
            try:
                if telethon_client.is_connected() and await telethon_client.is_user_authorized():
                    st += "✅ <b>Подключен и авторизован</b> (файлы >20 МБ)\n"
                elif telethon_client.is_connected():
                    st += "⚠️ <b>Подключен, но не авторизован</b>\n"
                else:
                    st += "❌ <b>Не подключен</b>\n"
            except Exception as e:
                st += f"❌ <b>Ошибка проверки:</b> {str(e)[:60]}\n"
        else:
            st += "⚠️ <b>Не настроен</b>\n"

        # Текущая нагрузка
        try:
            st += (f"\n⚡ <b>Нагрузка:</b>\n"
                   f"• Telethon слотов свободно: {TELETHON_SEMAPHORE._value}/{MAX_CONCURRENT_TELETHON_DOWNLOADS}\n"
                   f"• Распаковка свободно: {EXTRACT_SEMAPHORE._value}/{MAX_CONCURRENT_EXTRACTS}\n"
                   f"• Upload свободно: {UPLOAD_SEMAPHORE._value}/{MAX_CONCURRENT_UPLOADS}\n"
                   f"• Активных пользователей: {len(USER_SEMAPHORES)}\n")
        except Exception:
            pass

        st += f"\n<b>Проверка прав API-ключа:</b>\n{report}\n\n"
        st += ("✅ <b>Все права присутствуют. Бот готов к работе!</b>" if not has_failure
               else "⚠️ <b>Некоторые права отсутствуют.</b>")

        await status_msg.edit_text(st, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в handle_status: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка при получении статуса: {e}")


async def send_tags_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await get_user_config_async(user_id):
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. /setup <api_key>")
        return

    await update_history_async(None, None, update.effective_chat.id)
    curr = USER_TAGS_MEM.get(user_id, {}).get("tags", [None, None])
    msg_text = "🗂 <b>Gestione Tag</b>\nSeleziona album o fotografo dai tasti sotto."
    if curr[0] or curr[1]:
        msg_text += f"\n\n🔹 <b>Attivi:</b>\n📂 Album: {curr[0] or '--'}\n👤 Foto: {curr[1] or '--'}"
    await update.message.reply_text(msg_text, reply_markup=await get_tags_markup(user_id), parse_mode="HTML")


async def get_tags_markup(user_id):
    data = await run_io(load_history)
    curr = USER_TAGS_MEM.get(user_id, {}).get("tags", [None, None])
    keyboard = []
    if data.get("albums"):
        keyboard.append([InlineKeyboardButton("📂 ALBUM RECENTI:", callback_data="none")])
        row = []
        for i, alb in enumerate(data["albums"][:6]):
            row.append(InlineKeyboardButton(f"✅ {alb}" if curr[0] == alb else alb,
                                            callback_data=f"alb_{i}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
    if data.get("photographers"):
        keyboard.append([InlineKeyboardButton("👤 FOTOGRAFI:", callback_data="none")])
        row = []
        for i, foto in enumerate(data["photographers"][:4]):
            row.append(InlineKeyboardButton(f"✅ {foto}" if curr[1] == foto else foto,
                                            callback_data=f"foto_{i}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🗑 Cancella Tag", callback_data="reset_tags")])
    return InlineKeyboardMarkup(keyboard)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    if not await get_user_config_async(user_id):
        await query.answer("Аккаунт не настроен!", show_alert=True)
        return

    data = query.data
    hist = await run_io(load_history)
    curr = USER_TAGS_MEM.get(user_id, {"tags": [None, None], "time": time.time()})

    if data == "reset_tags":
        curr["tags"] = [None, None]
    elif data.startswith("alb_"):
        idx = int(data.split("_")[1])
        if idx < len(hist["albums"]):
            val = hist["albums"][idx]
            curr["tags"][0] = None if curr["tags"][0] == val else val
    elif data.startswith("foto_"):
        idx = int(data.split("_")[1])
        if idx < len(hist["photographers"]):
            val = hist["photographers"][idx]
            curr["tags"][1] = None if curr["tags"][1] == val else val

    curr["time"] = time.time()
    USER_TAGS_MEM[user_id] = curr

    msg_text = "🗂 <b>Gestione Tag</b>\nSeleziona album o fotografo dai tasti sotto."
    if curr["tags"][0] or curr["tags"][1]:
        msg_text += f"\n\n🔹 <b>Attivi:</b>\n📂 Album: {curr['tags'][0] or '--'}\n👤 Foto: {curr['tags'][1] or '--'}"
    try:
        await query.edit_message_text(msg_text, reply_markup=await get_tags_markup(user_id), parse_mode="HTML")
    except Exception:
        pass


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await get_user_config_async(user_id):
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. /setup <api_key>")
        return

    text = update.message.text
    if len(text) > 200:
        return
    ta, tf = parse_tags(text)
    if ta or tf:
        curr_mem = USER_TAGS_MEM.get(user_id, {"tags": [None, None], "time": time.time()})
        if ta:
            curr_mem["tags"][0] = ta
        if tf:
            curr_mem["tags"][1] = tf
        curr_mem["time"] = time.time()
        USER_TAGS_MEM[user_id] = curr_mem
        await update_history_async(ta, tf, update.effective_chat.id)
        await update.message.reply_text(f"✅ Tag aggiornati!\n📂 {curr_mem['tags'][0]}\n👤 {curr_mem['tags'][1]}")


async def handle_any_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = await get_user_config_async(user_id)
    if not user_config:
        await update.message.reply_text(
            "⚠️ <b>Аккаунт не настроен!</b>\n\n<code>/setup ВАШ_API_КЛЮЧ</code>", parse_mode="HTML")
        return

    await update_history_async(None, None, update.effective_chat.id)

    # Персональный семафор: большой файл одного юзера НЕ блокирует остальных
    user_sem = await get_user_semaphore(f"{user_id}:{update.effective_chat.id}")
    async with user_sem:
        await _process_media(update, context, user_id, user_config)


async def _process_media(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, user_config: dict):
    msg = update.message
    tags = get_effective_tags(user_id, msg.caption or "")
    raw_caption = msg.caption or None
    file_obj = None
    fname = "file.jpg"
    api_key = user_config["api_key"]

    # ---------- DOCUMENT ----------
    if msg.document:
        fname = msg.document.file_name or "file"
        mime = msg.document.mime_type or ""
        file_size = msg.document.file_size or 0
        is_archive = fname.lower().endswith(ARCHIVE_EXTS)

        if "image" in mime or "video" in mime or is_archive:
            if file_size > 20 * 1024 * 1024 and telethon_client:
                job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
                await run_io(os.makedirs, job_dir, exist_ok=True)
                dest_path = os.path.join(job_dir, fname)
                sm = await msg.reply_text("⏳ <b>Скачивание большого файла через Telethon...</b>",
                                          parse_mode="HTML")
                try:
                    async with TELETHON_SEMAPHORE:
                        tmsg = await telethon_get_message_safe(
                            context, msg.chat, msg.message_id,
                            file_size=file_size, file_name=fname
                        )
                        if not tmsg.media:
                            raise Exception("В сообщении нет медиа для скачивания")
                        logging.info(f"⬇️ Скачиваю {fname} ({file_size / 1048576:.1f} МБ) → {dest_path}")
                        await telethon_download_to_path(tmsg, dest_path)

                    size = await run_io(os.path.getsize, dest_path)
                    logging.info(f"✅ Скачано: {dest_path} ({size / 1048576:.1f} МБ)")

                    if not is_archive:
                        new_path = dest_path + ".zip"
                        await run_io(os.rename, dest_path, new_path)
                        dest_path, fname = new_path, fname + ".zip"

                    stats, albums = await process_directory_content(
                        job_dir, msg.date, msg.message_id,
                        user_config=user_config, manual_tags=tags,
                        bot=context.bot, chat_id=msg.chat_id, caption=raw_caption
                    )
                    await run_io(_rmtree_sync, job_dir)

                    rep = (f"📦 <b>Большой архив обработан!</b>\n"
                           f"✅ {stats['success']} Фото\n♻️ {stats['duplicate']} Дубликатов")
                    if albums:
                        rep += f"\n📂 <b>Альбомы:</b> {', '.join(albums)}"
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                        text=rep, parse_mode="HTML")
                    return
                except Exception as e:
                    logging.error(f"Telethon download error: {e}", exc_info=True)
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                        text=f"❌ Ошибка скачивания: {e}")
                    await run_io(_rmtree_sync, job_dir)
                    return

            try:
                file_obj = await msg.document.get_file()
            except TelegramError as e:
                if "too big" in str(e).lower():
                    await msg.reply_text(f"❌ Файл слишком большой ({file_size / 1048576:.1f} МБ).")
                    return
                raise

    # ---------- PHOTO ----------
    elif msg.photo:
        file_obj = await msg.photo[-1].get_file()
        fname = f"photo_{file_obj.file_unique_id}.jpg"

    # ---------- VIDEO ----------
    elif msg.video:
        file_size = msg.video.file_size or 0
        fname = msg.video.file_name or f"video_{msg.video.file_unique_id}.mp4"
        if file_size > 20 * 1024 * 1024 and telethon_client:
            job_dir = os.path.join(TEMP_DIR, f"video_{msg.message_id}")
            await run_io(os.makedirs, job_dir, exist_ok=True)
            dest_path = os.path.join(job_dir, fname)
            sm = await msg.reply_text("⏳ <b>Скачивание большого видео через Telethon...</b>",
                                      parse_mode="HTML")
            try:
                async with TELETHON_SEMAPHORE:
                    tmsg = await telethon_get_message_safe(
                        context, msg.chat, msg.message_id, file_size=file_size, file_name=fname)
                    if not tmsg.media:
                        raise Exception("В сообщении нет медиа для скачивания")
                    await telethon_download_to_path(tmsg, dest_path)

                if not await run_io(os.path.exists, dest_path) or await run_io(os.path.getsize, dest_path) == 0:
                    raise Exception("Файл не скачан или пустой")

                status, _ = await upload_file_path_to_immich(
                    dest_path, fname, msg.date, msg.message_id, user_api_key=api_key,
                    extra_album=tags[0] if tags and tags[0] else None,
                    fotografo=tags[1] if tags and len(tags) > 1 else None,
                    caption=raw_caption
                )
                await run_io(_rmtree_sync, job_dir)
                report = "✅ Видео загружено!" if status == "success" else "♻️ Дубликат (теги обновлены)."
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                    text=report, parse_mode="HTML")
                return
            except Exception as e:
                logging.error(f"Telethon video download error: {e}", exc_info=True)
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                    text=f"❌ Ошибка скачивания: {e}")
                await run_io(_rmtree_sync, job_dir)
                return
        try:
            file_obj = await msg.video.get_file()
        except TelegramError as e:
            if "too big" in str(e).lower():
                await msg.reply_text(f"❌ Видео слишком большое ({file_size / 1048576:.1f} МБ).")
                return
            raise

    # ---------- VIDEO NOTE ----------
    elif msg.video_note:
        file_size = msg.video_note.file_size or 0
        if file_size > 20 * 1024 * 1024 and telethon_client:
            job_dir = os.path.join(TEMP_DIR, f"vnote_{msg.message_id}")
            await run_io(os.makedirs, job_dir, exist_ok=True)
            fname = f"video_note_{msg.video_note.file_unique_id}.mp4"
            dest_path = os.path.join(job_dir, fname)
            sm = await msg.reply_text("⏳ <b>Скачивание большого видеосообщения через Telethon...</b>",
                                      parse_mode="HTML")
            try:
                async with TELETHON_SEMAPHORE:
                    tmsg = await telethon_get_message_safe(
                        context, msg.chat, msg.message_id, file_size=file_size, file_name=fname)
                    if not tmsg.media:
                        raise Exception("В сообщении нет медиа для скачивания")
                    await telethon_download_to_path(tmsg, dest_path)

                if not await run_io(os.path.exists, dest_path) or await run_io(os.path.getsize, dest_path) == 0:
                    raise Exception("Файл не скачан или пустой")

                status, _ = await upload_file_path_to_immich(
                    dest_path, fname, msg.date, msg.message_id, user_api_key=api_key,
                    extra_album=tags[0] if tags and tags[0] else None,
                    fotografo=tags[1] if tags and len(tags) > 1 else None,
                    caption=raw_caption
                )
                await run_io(_rmtree_sync, job_dir)
                report = "✅ Видео загружено!" if status == "success" else "♻️ Дубликат (теги обновлены)."
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                    text=report, parse_mode="HTML")
                return
            except Exception as e:
                logging.error(f"Telethon video_note download error: {e}", exc_info=True)
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                    text=f"❌ Ошибка скачивания: {e}")
                await run_io(_rmtree_sync, job_dir)
                return

        for attempt in range(3):
            try:
                file_obj = await msg.video_note.get_file()
                fname = f"video_note_{file_obj.file_unique_id}.mp4"
                break
            except TelegramError as e:
                if "timed out" in str(e).lower() and attempt < 2:
                    await asyncio.sleep(2)
                    continue
                await msg.reply_text(f"❌ Не удалось скачать видеосообщение: {e}")
                return

    # ---------- ОБЩИЙ ПУТЬ (Bot API) ----------
    if file_obj:
        if fname.lower().endswith(ARCHIVE_EXTS):
            job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
            await run_io(os.makedirs, job_dir, exist_ok=True)
            sm = await msg.reply_text("⏳ <b>Archivio ricevuto...</b>", parse_mode="HTML")
            await file_obj.download_to_drive(os.path.join(job_dir, fname))
            stats, albums = await process_directory_content(
                job_dir, msg.date, msg.message_id,
                user_config=user_config, manual_tags=tags,
                bot=context.bot, chat_id=msg.chat_id, caption=raw_caption
            )
            await run_io(_rmtree_sync, job_dir)
            rep = f"📦 <b>Elaborato!</b>\n✅ {stats['success']} Foto\n♻️ {stats['duplicate']} Duplicati"
            if albums:
                rep += f"\n📂 <b>Album:</b> {', '.join(albums)}"
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id,
                                                text=rep, parse_mode="HTML")
        else:
            tmp = os.path.join(TEMP_DIR, f"{msg.message_id}_{fname}")
            await file_obj.download_to_drive(tmp)
            status, _ = await upload_file_path_to_immich(
                tmp, fname, msg.date, msg.message_id, user_api_key=api_key,
                extra_album=tags[0] if tags and tags[0] else None,
                fotografo=tags[1] if tags and len(tags) > 1 else None,
                caption=raw_caption
            )
            try:
                await run_io(os.remove, tmp)
            except Exception:
                pass
            report = "✅ Caricata!" if status == "success" else "♻️ Duplicata (Tag Aggiornati)."
            if tags and tags[0]:
                report += f"\n📂 Album: {tags[0]}"
            if tags and len(tags) > 1 and tags[1]:
                report += f"\n👤 Foto: {tags[1]}"
            await msg.reply_text(report, parse_mode="HTML")


async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = await get_user_config_async(user_id)   # ← исправлен NameError
    if not user_config:
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. /setup <api_key>")
        return

    await update_history_async(None, None, update.effective_chat.id)

    url_m = re.search(r'(https?://\S+)', update.message.text)
    if not url_m:
        return
    url = url_m.group(1)
    tags = get_effective_tags(user_id, update.message.text)

    user_sem = await get_user_semaphore(f"{user_id}:{update.effective_chat.id}")
    async with user_sem:
        job_dir = os.path.join(TEMP_DIR, f"job_{update.message.message_id}")
        await run_io(os.makedirs, job_dir, exist_ok=True)
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

        def _list():
            return [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]

        downloaded_files = await run_io(_list)
        if not success or not downloaded_files:
            try:
                proc = await asyncio.create_subprocess_exec(
                    "wget", "--content-disposition", "-P", job_dir, url)
                await proc.wait()
                downloaded_files = await run_io(_list)
            except Exception:
                pass

        def _rename():
            import urllib.parse
            for fn in downloaded_files:
                full_path = os.path.join(job_dir, fn)
                if "?" in fn or "=" in fn or not any(
                        fn.lower().endswith(e) for e in ['.zip', '.rar', '.7z', '.jpg', '.jpeg', '.png', '.mp4', '.mov']):
                    parsed = urllib.parse.urlparse(url)
                    qp = urllib.parse.parse_qs(parsed.query)
                    if 'filename' in qp:
                        new_name = re.sub(r'[<>:"/\\|?*]', '_', urllib.parse.unquote(qp['filename'][0]))
                        try:
                            os.rename(full_path, os.path.join(job_dir, new_name))
                            logging.info(f"Переименован файл: {fn} -> {new_name}")
                        except Exception as e:
                            logging.warning(f"Не удалось переименовать {fn}: {e}")

        await run_io(_rename)

        stats, albums = await process_directory_content(
            job_dir, update.message.date, update.message.message_id,
            user_config=user_config, manual_tags=tags,
            bot=context.bot, chat_id=update.message.chat_id
        )
        await run_io(_rmtree_sync, job_dir)

        rep = f"✅ <b>Completato!</b>\n{stats['success']} Caricati\n{stats['duplicate']} Duplicati"
        if albums:
            rep += f"\n📂 Album: {', '.join(albums)}"
        await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id,
                                            text=rep, parse_mode="HTML")


# --- ERROR HANDLER ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, TelegramError):
        logging.warning(f"⚠️ Ошибка Telegram: {error}.")
    else:
        logging.error("❌ Неожиданная ошибка в боте:", exc_info=error)


# --- MAIN ---
async def main():
    await init_telethon_client()

    await app.initialize()
    await app.start()

    is_active = False
    watch_task = None

    if await run_io(try_acquire_lock, LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
        is_active = True
        await app.updater.start_polling(drop_pending_updates=False)
        watch_task = asyncio.create_task(watch_folder_task(app))
        logging.info("🔒 Lock acquired on start. ACTIVE mode.")
    else:
        logging.info("⏳ Lock held by another instance. PASSIVE mode.")

    # Уведомление первого пользователя о старте и текущей роли
    await notify_bot_started(app, is_active)

    try:
        while True:
            await asyncio.sleep(LOCK_CHECK_INTERVAL)
            if is_active:
                if not await run_io(maintain_lock, LOCK_FILE_PATH, INSTANCE_ID):
                    logging.warning("🔓 Failed to maintain lock → PASSIVE mode.")
                    is_active = False
                    if watch_task:
                        watch_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await watch_task
                        watch_task = None
                    await app.updater.stop()
                    await notify_role_change(app, "PASSIVE", "потеряна блокировка (heartbeat)")
                    await asyncio.sleep(4)
            else:
                if await run_io(try_acquire_lock, LOCK_FILE_PATH, INSTANCE_ID, LOCK_TIMEOUT):
                    logging.info("🔒 Lock acquired → ACTIVE mode.")
                    await asyncio.sleep(3)
                    await app.updater.start_polling(drop_pending_updates=False)
                    watch_task = asyncio.create_task(watch_folder_task(app))
                    is_active = True
                    await notify_role_change(app, "ACTIVE", "блокировка перехвачена")
    except asyncio.CancelledError:
        logging.info("Main loop cancelled.")
    finally:
        if is_active:
            await app.updater.stop()
        if watch_task and not watch_task.done():
            watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watch_task
        await run_io(release_lock, LOCK_FILE_PATH, INSTANCE_ID)
        await notify_bot_stopped(app)
        await app.stop()
        await app.shutdown()
        IO_EXECUTOR.shutdown(wait=False)
        CPU_EXECUTOR.shutdown(wait=False)
        if telethon_client is not None:
            with contextlib.suppress(Exception):
                await telethon_client.disconnect()
        logging.info("🛑 Bot shutdown complete.")


if __name__ == "__main__":
    request = HTTPXRequest(
        connection_pool_size=16,
        read_timeout=60.0,
        write_timeout=60.0,
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
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.VIDEO_NOTE | filters.Document.ALL,
                                   handle_any_media))
    print("Bot avviato (v62 - Non-blocking IO + Per-User Fairness + Role Notifications)...")
    asyncio.run(main())
