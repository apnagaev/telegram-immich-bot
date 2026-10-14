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

TOKEN = os.getenv("TELEGRAM_TOKEN")
IMMICH_URL = os.getenv("IMMICH_URL", "http://immich:2283").rstrip("/")
IMPORT_DIR = os.getenv("IMPORT_DIR", "/import")
HISTORY_FILE = os.path.join(IMPORT_DIR, "bot_history.json")
USER_MAPPING_FILE = os.path.join(IMPORT_DIR, "user_mapping.json")

TEMP_DIR = "/tmp/bot_downloads"
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

# INSTANCE_ID ДОЛЖЕН быть объявлен ДО использования в Telethon
INSTANCE_ID = os.getenv("HOSTNAME", str(uuid.uuid4()))

# --- TELETHON CONFIG ---
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = "telegram_session"

telethon_client = None
TELETHON_SEMAPHORE = asyncio.Semaphore(3)  # Максимум 3 параллельные загрузки

async def init_telethon_client():
    """
    Каждый экземпляр копирует сессию в /tmp с уникальным суффиксом,
    чтобы контейнеры не дрались за одну базу SQLite.
    Инициализация происходит в том же event loop, что и бот.
    """
    global telethon_client
    if not (API_ID and API_HASH):
        logging.warning("⚠️ TELEGRAM_API_ID или TELEGRAM_API_HASH не заданы")
        return

    src = f"{SESSION_NAME}.session"
    if not os.path.exists(src):
        logging.error(f"❌ Файл сессии '{src}' не найден! Создайте его локально и пробросьте в контейнер.")
        return

    suffix = hashlib.md5(INSTANCE_ID.encode()).hexdigest()[:8]
    local_session = f"/tmp/{SESSION_NAME}_{suffix}"

    try:
        shutil.copy2(src, local_session + ".session")
        logging.info(f"📋 Сессия скопирована в {local_session}.session")
    except Exception as e:
        logging.error(f"❌ Не удалось скопировать сессию: {e}")
        return

    try:
        telethon_client = TelegramClient(local_session, API_ID, API_HASH)
        await telethon_client.connect()
        logging.info("✅ Telethon подключен")
        if not await telethon_client.is_user_authorized():
            logging.error("❌ Telethon не авторизован! Копия сессии недействительна.")
            await telethon_client.disconnect()
            telethon_client = None
        else:
            logging.info("✅ Telethon авторизован и готов к работе")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации Telethon: {e}", exc_info=True)
        telethon_client = None

# --- GLOBALS ---
USER_TAGS_MEM = {}
ALBUM_CACHE = {}  # Ключ: (api_key_hash, album_name_lower)

# Финальный минимальный список разрешений (ровно 10)
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
    """Атомарная запись JSON с файловой блокировкой для предотвращения race conditions."""
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
            except:
                pass

# --- USER MAPPING FUNCTIONS ---
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

# --- TELETHON SAFE HELPERS ---
async def telethon_get_message_safe(context, chat, message_id, retries=3, delay=2):
    """
    Надёжное получение сообщения через Telethon.
    1) Пробует быстрых кандидатов (ID бота для личных чатов, chat_id).
    2) Если не найдено — перебирает все диалоги сессии.
    Логирует личность сессии для диагностики несовпадения аккаунтов.
    """
    if telethon_client is None:
        raise Exception("Telethon не инициализирован")

    if not telethon_client.is_connected():
        logging.warning("🔄 Telethon отключен — переподключаюсь")
        await telethon_client.connect()

    if not await telethon_client.is_user_authorized():
        raise Exception("Telethon не авторизован после переподключения")

    # --- Диагностика: чья это сессия ---
    try:
        me = await telethon_client.get_me()
        logging.info(f"🔎 Telethon-сессия: id={me.id}, username={getattr(me, 'username', None)}")
    except Exception as e:
        logging.warning(f"Не удалось получить get_me(): {e}")

    # --- Кандидаты-сущности ---
    candidate_ids = []
    if chat.type == "private":
        try:
            candidate_ids.append(context.bot.id)  # диалог пользователя с ботом
        except Exception:
            pass
        candidate_ids.append(chat.id)
    else:
        candidate_ids.append(chat.id)
    candidate_ids = list(dict.fromkeys(candidate_ids))
    logging.info(f"🔎 Ищу сообщение {message_id}, кандидаты: {candidate_ids}")

    # --- Попытка 1: быстрые кандидаты ---
    for attempt in range(1, retries + 1):
        for cid in candidate_ids:
            try:
                try:
                    entity = await telethon_client.get_entity(cid)
                except Exception:
                    entity = cid  # если не резолвится — пробуем как есть
                msg = await telethon_client.get_messages(entity, ids=message_id)
                if msg is not None and getattr(msg, "id", None) == message_id:
                    logging.info(f"✅ Найдено сообщение {message_id} (entity={cid}, попытка {attempt})")
                    return msg
            except Exception as e:
                logging.debug(f"get_messages(entity={cid}) ошибка: {e}")
        if attempt < retries:
            logging.warning(f"⚠️ Сообщение {message_id} не найдено, попытка {attempt}/{retries}")
            await asyncio.sleep(delay)

    # --- Попытка 2: перебор всех диалогов (надёжный запасной путь) ---
    logging.info("🔁 Быстрые кандидаты не сработали. Перебираю диалоги...")
    checked = 0
    try:
        async for dialog in telethon_client.iter_dialogs():
            checked += 1
            if checked > 100:  # разумный лимит
                break
            try:
                msg = await telethon_client.get_messages(dialog.entity, ids=message_id)
                if msg is not None and getattr(msg, "id", None) == message_id:
                    logging.info(f"✅ Найдено сообщение {message_id} в диалоге '{dialog.name}' (id={dialog.id})")
                    return msg
            except Exception:
                continue
    except Exception as e:
        logging.warning(f"Перебор диалогов завершился с ошибкой: {e}")

    raise Exception(
        f"Telethon не нашёл сообщение {message_id} (чат {chat.id}, type={chat.type}). "
        f"Проверьте в логах строку '🔎 Telethon-сессия': если её id != {chat.id}, "
        f"значит файл .session принадлежит другому аккаунту — перегенерируйте его."
    )

async def telethon_download_to_path(message, dest_path):
    """Скачивает медиа из сообщения и возвращает реальный путь к файлу."""
    result = await telethon_client.download_media(message, file=dest_path)
    logging.info(f"⬇️ download_media вернул: {result!r}")

    if not result:
        raise Exception("download_media вернул None")

    final_path = result if isinstance(result, str) else dest_path

    if final_path != dest_path and os.path.exists(final_path):
        shutil.move(final_path, dest_path)
        logging.info(f"📦 Перемещено: {final_path} → {dest_path}")
        final_path = dest_path

    if not os.path.exists(dest_path):
        raise Exception(f"Файл не появился на диске: {dest_path}")
    if os.path.getsize(dest_path) == 0:
        raise Exception(f"Файл пустой: {dest_path}")

    return dest_path

# --- PERMISSION CHECK FUNCTIONS ---
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
                if items and isinstance(items, list) and len(items) > 0:
                    item = items[0]
                    if isinstance(item, dict) and "id" in item:
                        return item["id"]
            elif isinstance(assets_data, list) and len(assets_data) > 0:
                item = assets_data[0]
                if isinstance(item, dict) and "id" in item:
                    return item["id"]
        if "items" in data and isinstance(data["items"], list) and len(data["items"]) > 0:
            item = data["items"][0]
            if isinstance(item, dict) and "id" in item:
                return item["id"]
        if "result" in data and isinstance(data["result"], dict):
            items = data["result"].get("items", [])
            if items and len(items) > 0:
                item = items[0]
                if isinstance(item, dict) and "id" in item:
                    return item["id"]
        if "assets" in data and isinstance(data["assets"], list) and len(data["assets"]) > 0:
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
            if method == "POST":
                resp = requests.post(endpoint, headers=headers, json=payload, timeout=10)
            else:
                resp = requests.get(endpoint, headers=headers, params=payload, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            asset_id = _extract_asset_id_from_response(data)
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
            if isinstance(albums, list) and len(albums) > 0:
                for album in albums:
                    if isinstance(album, dict) and album.get("id"):
                        album_name = album.get("albumName", "")
                        if not album_name.startswith("_perm_") and not album_name.startswith("_test_"):
                            return album["id"], album_name
                return albums[0]["id"], albums[0].get("albumName", "")
    except Exception:
        pass
    return None, None

async def check_api_key_permissions(api_key: str) -> dict:
    results = {}
    for perm in REQUIRED_PERMISSIONS:
        results[perm] = {"status": False, "message": "Не проверено"}

    headers = {"x-api-key": api_key, "Accept": "application/json"}

    # 1. user.read, asset, asset.read
    try:
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        if resp.status_code == 200:
            results["user.read"]["status"] = True
            results["user.read"]["message"] = "✅ OK"
            results["asset"]["status"] = True
            results["asset"]["message"] = "✅ OK"
            results["asset.read"]["status"] = True
            results["asset.read"]["message"] = "✅ OK"
        elif resp.status_code == 403:
            results["user.read"]["status"] = False
            results["user.read"]["message"] = "❌ Нет доступа (403)"
            results["asset"]["status"] = False
            results["asset"]["message"] = "❌ Нет доступа (403)"
            results["asset.read"]["status"] = False
            results["asset.read"]["message"] = "❌ Нет доступа (403)"
        else:
            detail = _get_error_detail(resp)
            results["user.read"]["status"] = False
            results["user.read"]["message"] = f"⚠️ Ошибка {resp.status_code}: {detail}"
    except Exception as e:
        results["user.read"]["status"] = False
        results["user.read"]["message"] = f"❌ Ошибка: {str(e)[:80]}"

    # 2. album, album.read
    try:
        resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers, timeout=10)
        if resp.status_code == 200:
            results["album"]["status"] = True
            results["album"]["message"] = "✅ OK"
            results["album.read"]["status"] = True
            results["album.read"]["message"] = "✅ OK"
        elif resp.status_code == 403:
            results["album.read"]["status"] = False
            results["album.read"]["message"] = "❌ Нет доступа (403)"
        else:
            detail = _get_error_detail(resp)
            results["album.read"]["status"] = False
            results["album.read"]["message"] = f"⚠️ Ошибка {resp.status_code}: {detail}"
    except Exception as e:
        results["album.read"]["status"] = False
        results["album.read"]["message"] = f"❌ Ошибка: {str(e)[:80]}"

    # 3. album.create
    test_album_id = None
    try:
        test_album_name = f"_perm_test_{uuid.uuid4().hex[:12]}"
        resp = requests.post(
            f"{IMMICH_URL}/api/albums",
            headers=headers,
            json={"albumName": test_album_name},
            timeout=10
        )
        if resp.status_code in [200, 201]:
            test_album_id = resp.json().get("id")
            results["album.create"]["status"] = True
            try:
                del_resp = requests.delete(f"{IMMICH_URL}/api/albums/{test_album_id}", headers=headers, timeout=10)
                if del_resp.status_code in [200, 204]:
                    results["album.create"]["message"] = "✅ OK"
                    test_album_id = None
                else:
                    results["album.create"]["message"] = "✅ OK (альбом не удалён)"
            except:
                results["album.create"]["message"] = "✅ OK"
        elif resp.status_code == 403:
            results["album.create"]["status"] = False
            results["album.create"]["message"] = "❌ Нет доступа (403)"
        else:
            detail = _get_error_detail(resp)
            results["album.create"]["status"] = False
            results["album.create"]["message"] = f"⚠️ Ошибка {resp.status_code}: {detail}"
    except Exception as e:
        results["album.create"]["status"] = False
        results["album.create"]["message"] = f"❌ Ошибка: {str(e)[:80]}"
    finally:
        if test_album_id:
            try:
                requests.delete(f"{IMMICH_URL}/api/albums/{test_album_id}", headers=headers, timeout=10)
            except:
                pass

    # 4. Получаем существующий ассет и альбом
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
                resp = requests.post(f"{IMMICH_URL}/api/assets", headers=headers, files=files, data=data, timeout=30)
                if resp.status_code in [200, 201]:
                    existing_asset_id = resp.json().get("id")
                    created_test_asset = True
        except Exception:
            pass
        finally:
            if os.path.exists(test_file_path):
                try:
                    os.remove(test_file_path)
                except:
                    pass

    # 5. asset.download
    if existing_asset_id:
        try:
            dl_resp = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}/original", headers=headers, timeout=30)
            if dl_resp.status_code == 200:
                results["asset.download"]["status"] = True
                results["asset.download"]["message"] = "✅ OK"
            elif dl_resp.status_code == 403:
                results["asset.download"]["status"] = False
                results["asset.download"]["message"] = "❌ Нет доступа (403)"
            else:
                results["asset.download"]["status"] = False
                results["asset.download"]["message"] = f"⚠️ Ошибка {dl_resp.status_code}"
        except Exception as e:
            results["asset.download"]["status"] = False
            results["asset.download"]["message"] = f"❌ Ошибка: {str(e)[:80]}"
    else:
        results["asset.download"]["status"] = False
        results["asset.download"]["message"] = "ℹ️ Невозможно проверить (нет ассетов)"

    # 6. asset.update
    if existing_asset_id:
        original_description = ""
        try:
            get_resp = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers, timeout=10)
            if get_resp.status_code == 200:
                original_description = get_resp.json().get("description") or ""

            update_resp = requests.put(
                f"{IMMICH_URL}/api/assets/{existing_asset_id}",
                headers=headers,
                json={"description": f"perm_test_{uuid.uuid4().hex[:8]}"},
                timeout=10
            )
            if update_resp.status_code == 200:
                results["asset.update"]["status"] = True
                results["asset.update"]["message"] = "✅ OK"
            elif update_resp.status_code == 403:
                results["asset.update"]["status"] = False
                results["asset.update"]["message"] = "❌ Нет доступа (403)"
            else:
                results["asset.update"]["status"] = False
                results["asset.update"]["message"] = f"⚠️ Ошибка {update_resp.status_code}"
        except Exception as e:
            results["asset.update"]["status"] = False
            results["asset.update"]["message"] = f"❌ Ошибка: {str(e)[:80]}"
        finally:
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers, json={"description": original_description}, timeout=10)
            except:
                pass
    else:
        results["asset.update"]["status"] = False
        results["asset.update"]["message"] = "ℹ️ Невозможно проверить (нет ассетов)"

    # 7. albumAsset.create
    if existing_asset_id and existing_album_id:
        try:
            resp = requests.put(
                f"{IMMICH_URL}/api/albums/{existing_album_id}/assets",
                headers=headers,
                json={"ids": [existing_asset_id]},
                timeout=10
            )
            if resp.status_code == 200:
                results["albumAsset.create"]["status"] = True
                results["albumAsset.create"]["message"] = f"✅ OK (добавлен в '{existing_album_name}')"
            elif resp.status_code == 403:
                results["albumAsset.create"]["status"] = False
                results["albumAsset.create"]["message"] = "❌ Нет доступа (403)"
            else:
                results["albumAsset.create"]["status"] = False
                results["albumAsset.create"]["message"] = f"⚠️ Ошибка {resp.status_code}"
        except Exception as e:
            results["albumAsset.create"]["status"] = False
            results["albumAsset.create"]["message"] = f"❌ Ошибка: {str(e)[:80]}"
    else:
        results["albumAsset.create"]["status"] = False
        results["albumAsset.create"]["message"] = "ℹ️ Невозможно проверить (нет ассетов или альбомов)"

    # 8. asset.upload
    if existing_asset_id and not created_test_asset:
        try:
            get_resp = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}", headers=headers, timeout=10)
            if get_resp.status_code == 200:
                asset_info = get_resp.json()
                device_asset_id = asset_info.get("deviceAssetId", f"perm-check-{uuid.uuid4().hex}")
                original_filename = asset_info.get("originalFileName", "test.jpg")
                file_created_at = asset_info.get("fileCreatedAt", datetime.now().isoformat())
                file_modified_at = asset_info.get("fileModifiedAt", datetime.now().isoformat())

                dl_resp = requests.get(f"{IMMICH_URL}/api/assets/{existing_asset_id}/original", headers=headers, timeout=30)
                if dl_resp.status_code == 200:
                    asset_bytes = dl_resp.content
                    files = {"assetData": (original_filename, BytesIO(asset_bytes))}
                    data = {
                        "deviceAssetId": device_asset_id,
                        "deviceId": "tg-immich-bot-perm-check",
                        "fileCreatedAt": file_created_at,
                        "fileModifiedAt": file_modified_at,
                        "isFavorite": "false",
                    }
                    resp = requests.post(f"{IMMICH_URL}/api/assets", headers=headers, files=files, data=data, timeout=30)

                    if resp.status_code in [200, 201]:
                        res_json = resp.json()
                        status = res_json.get("status", "")
                        if status == "duplicate":
                            results["asset.upload"]["status"] = True
                            results["asset.upload"]["message"] = "✅ OK (duplicate)"
                        elif status == "created":
                            results["asset.upload"]["status"] = True
                            results["asset.upload"]["message"] = "✅ OK (created)"
                            new_asset_id = res_json.get("id")
                            if new_asset_id:
                                try:
                                    requests.delete(f"{IMMICH_URL}/api/assets", headers=headers, json={"ids": [new_asset_id]}, timeout=10)
                                except:
                                    pass
                        else:
                            results["asset.upload"]["status"] = True
                            results["asset.upload"]["message"] = "✅ OK"
                    elif resp.status_code == 409:
                        results["asset.upload"]["status"] = True
                        results["asset.upload"]["message"] = "✅ OK (duplicate 409)"
                    elif resp.status_code == 403:
                        results["asset.upload"]["status"] = False
                        results["asset.upload"]["message"] = "❌ Нет доступа (403)"
                    else:
                        results["asset.upload"]["status"] = False
                        results["asset.upload"]["message"] = f"⚠️ Ошибка {resp.status_code}: {_get_error_detail(resp)}"
        except Exception as e:
            results["asset.upload"]["status"] = False
            results["asset.upload"]["message"] = f"❌ Ошибка: {str(e)[:80]}"
    elif created_test_asset:
        results["asset.upload"]["status"] = True
        results["asset.upload"]["message"] = "✅ OK (через тестовый ассет)"
    else:
        results["asset.upload"]["status"] = False
        results["asset.upload"]["message"] = "ℹ️ Невозможно проверить (нет ассетов)"

    # Очистка тестового ассета
    if created_test_asset and existing_asset_id:
        try:
            requests.delete(f"{IMMICH_URL}/api/assets", headers=headers, json={"ids": [existing_asset_id]}, timeout=10)
        except:
            pass

    return results

def format_permissions_report(results: dict) -> str:
    lines = []
    has_failure = False
    has_info = False

    for perm, description in REQUIRED_PERMISSIONS.items():
        status = results.get(perm, {}).get("status", False)
        message = results.get(perm, {}).get("message", "Не проверено")

        is_info = "ℹ️" in message or "Невозможно проверить" in message

        if is_info:
            icon = "ℹ️"
            has_info = True
        elif status:
            icon = "✅"
        else:
            icon = "❌"
            has_failure = True

        lines.append(f"{icon} <code>{perm}</code> — {description}")
        if not status and not is_info and "Не проверено" not in message:
            lines.append(f"   └─ {message}")

    if not has_failure:
        if has_info:
            lines.insert(0, "✅ <b>Основные права присутствуют!</b>\n")
            lines.append("\nℹ️ <i>Некоторые права не удалось проверить автоматически (нет ассетов/альбомов). Это нормально для нового аккаунта.</i>")
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
            logging.debug(f"⏳ Lock занят '{owner}', heartbeat {age:.1f}s назад")
            return False
        except Exception as e:
            logging.error(f"❌ Чтение lock failed: {e}, перезаписываю")
            try:
                with open(lock_path, 'w') as f:
                    json.dump({"instance_id": instance_id, "last_heartbeat": time.time()}, f)
                return True
            except Exception as e2:
                logging.error(f"❌ Перезапись lock failed: {e2}")
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
        if os.path.isdir(path):
            os.chmod(path, 0o777)
        else:
            os.chmod(path, 0o666)
    except:
        pass

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return {"albums": [], "photographers": [], "last_chat_id": None}
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if "last_chat_id" not in data:
                data["last_chat_id"] = None
            return data
    except:
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

def get_or_create_album(requested_name, api_key):
    """Ищет альбом по имени. Если не находит или имя пустое, создает 'Telegram Upload'.
    Кэш изолирован по хэшу api_key, чтобы разные пользователи не путали альбомы."""
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
        cr = requests.post(f"{IMMICH_URL}/api/albums", headers=headers, json={"albumName": fallback_name}, timeout=10)
        if cr.status_code in [200, 201]:
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
        album = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "", alb_m.group(1).strip(), flags=re.IGNORECASE).strip()
    foto_m = re.search(r"#fotografo\s+([^#\n\/]+)", text, re.IGNORECASE)
    if foto_m:
        fotografo = re.sub(r"\.(zip|rar|7z|tar|gz|jpg|jpeg|png|heic|webp)$", "", foto_m.group(1).strip(), flags=re.IGNORECASE).strip()
    return album, fotografo

def clean_caption(text):
    """Очищает caption от служебных тегов #album и #fotografo."""
    if not text:
        return None
    cleaned = text
    cleaned = re.sub(r"#album\s+[^#\n\/]+", "", cleaned, flags=re.IGNORECASE)
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

def is_file_stable(filepath, wait_time=5):
    if filepath.endswith(".part"):
        return False
    if os.path.exists(filepath + ".part"):
        return False
    if wait_time <= 0:
        try:
            return os.path.getsize(filepath) > 0
        except:
            return False
    try:
        size1 = os.path.getsize(filepath)
        time.sleep(wait_time)
        size2 = os.path.getsize(filepath)
        return size1 == size2 and size1 > 0
    except:
        return False

def calculate_checksum(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
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
                if not asset_id:
                    chk = calculate_checksum(file_path)
                    asset_id = await find_asset_by_checksum(chk, user_api_key)
            else:
                status = "success"
        elif resp.status_code == 409:
            status = "duplicate"
            chk = calculate_checksum(file_path)
            asset_id = await find_asset_by_checksum(chk, user_api_key)

        if not asset_id:
            return status, None

        description_parts = []
        if caption:
            cleaned_caption = clean_caption(caption)
            if cleaned_caption:
                description_parts.append(cleaned_caption)
        if fotografo:
            description_parts.append(f"Fotografo: {fotografo}")

        if description_parts:
            final_description = "\n\n".join(description_parts)
            try:
                requests.put(f"{IMMICH_URL}/api/assets/{asset_id}", headers=headers, json={"description": final_description}, timeout=10)
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

        for alb_id in list(set(target_ids)):
            try:
                requests.put(f"{IMMICH_URL}/api/albums/{alb_id}/assets", headers=headers, json={"ids": [asset_id]}, timeout=10)
            except Exception as e:
                logging.warning(f"Не удалось добавить в альбом {alb_id}: {e}")

        return status, asset_id
    except Exception as e:
        logging.error(f"Upload error: {e}")
        return "error", None

# --- PROCESSORS ---
async def process_directory_content(directory, msg_date, msg_id, user_config: dict, manual_tags=None, bot=None, chat_id=None, caption=None):
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
                    if not is_file_stable(full_p, wait_time=3):
                        continue
                    ext_to = os.path.join(root, "extracted_" + file)
                    os.makedirs(ext_to, exist_ok=True)
                    fix_perms(ext_to)
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
        for file in files:
            all_items.append((root, file))

    total_count = len(all_items)
    processed_count = 0
    progress_msg = None
    if total_count > 5 and bot and chat_id:
        try:
            progress_msg = await bot.send_message(chat_id=chat_id, text=f"⏳ <b>Avanzamento:</b> 0/{total_count}...", parse_mode="HTML")
        except:
            pass

    for root, file in all_items:
        processed_count += 1
        full_p = os.path.join(root, file)
        # Для распакованных файлов проверка мгновенная (без ожидания)
        if not is_file_stable(full_p, wait_time=0):
            continue
        if file.lower().endswith(('.jpg', '.jpeg', '.png', '.heic', '.mp4', '.mov', '.avi', '.webp',
                                   '.raf', '.cr2', '.cr3', '.nef', '.arw', '.dng', '.rw2', '.orf', '.raw')):
            final_alb = manual_tags[0] if manual_tags and manual_tags[0] else None
            final_foto = manual_tags[1] if manual_tags and len(manual_tags) > 1 and manual_tags[1] else None

            rel_p = os.path.relpath(full_p, directory)
            p_alb, p_foto = parse_tags(rel_p)

            if p_foto:
                logging.info(f"Found photographer in path: {p_foto}")
            if p_alb and not final_alb:
                final_alb = p_alb
            if p_foto and not final_foto:
                final_foto = p_foto
            if final_alb:
                current_job_albums.add(final_alb)

            fb_date = msg_date
            if str(msg_id).startswith("watch"):
                try:
                    fb_date = datetime.fromtimestamp(os.path.getmtime(full_p))
                except:
                    pass

            status, _ = await upload_file_path_to_immich(
                full_p, file, fb_date, f"{msg_id}-{file}",
                user_api_key=user_api_key,
                extra_album=final_alb,
                fotografo=final_foto,
                caption=caption
            )

            if status in ["success", "duplicate"]:
                try:
                    os.remove(full_p)
                except:
                    pass
                if status == "success":
                    total_stats["success"] += 1
                else:
                    total_stats["duplicate"] += 1
            else:
                total_stats["error"] += 1

            if progress_msg and processed_count % 5 == 0:
                perc = int((processed_count / total_count) * 100)
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"⏳ <b>Avanzamento:</b> {processed_count}/{total_count} ({perc}%)", parse_mode="HTML")
                except:
                    pass
            await asyncio.sleep(1)
        else:
            if root == IMPORT_DIR and (file == "put_files_here.txt" or file == "bot_history.json" or file == "user_mapping.json"):
                continue
            unsupported_dir = os.path.join(IMPORT_DIR, "unsupported_files")
            os.makedirs(unsupported_dir, exist_ok=True)
            try:
                shutil.move(full_p, os.path.join(unsupported_dir, file))
                total_stats["unsupported"] += 1
            except:
                pass

    if progress_msg:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=progress_msg.message_id)
        except:
            pass

    for root, dirs, files in os.walk(directory, topdown=False):
        for name in dirs:
            d_path = os.path.join(root, name)
            if name in ["corrupted", "failed_extraction", "unsupported_files", "tg-immich-bot"] or d_path == directory:
                continue
            try:
                os.rmdir(d_path)
            except:
                pass

    return total_stats, list(current_job_albums)

# --- WATCH FOLDER TASK ---
async def watch_folder_task(app):
    special_dirs = ["corrupted", "failed_extraction", "unsupported_files", "tg-immich-bot"]
    while True:
        try:
            await asyncio.sleep(30)
            if not os.path.exists(IMPORT_DIR):
                continue
            items = os.listdir(IMPORT_DIR)
            relevant = [i for i in items if i not in special_dirs and i not in ["put_files_here.txt", "bot_history.json", "user_mapping.json"]]
            if not relevant:
                continue

            has_valid_content = False
            for root, dirs, files in os.walk(IMPORT_DIR):
                dirs[:] = [d for d in dirs if d not in special_dirs]
                for file in files:
                    if file.lower().endswith((".jpg", ".jpeg", ".png", ".heic", ".mp4", ".mov", ".avi", ".zip", ".rar", ".7z", ".tar", ".webp",
                                              ".raf", ".cr2", ".cr3", ".nef", ".arw", ".dng", ".rw2", ".orf", ".raw", ".gz")):
                        if is_file_stable(os.path.join(root, file), wait_time=2):
                            has_valid_content = True
                            break
                if has_valid_content:
                    break

            if not has_valid_content:
                continue

            mapping = load_user_mapping()
            if not mapping:
                logging.warning("⚠️ Watch folder: нет настроенных пользователей. Пропуск.")
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
                try:
                    await app.bot.send_message(chat_id=target_chat, text=f"📂 <b>Importazione Automatica Iniziata!</b> (User: {default_user_config['email']})", parse_mode="HTML")
                except:
                    pass

            stats, albums = await process_directory_content(
                IMPORT_DIR, datetime.now(), "watchdog",
                user_config=default_user_config,
                bot=app.bot, chat_id=target_chat
            )

            if target_chat and (sum(stats.values()) > 0):
                report = f"📂 <b>Importazione Completata!</b>\n✅ {stats['success']} Caricati\n♻️ {stats['duplicate']} Duplicati"
                if albums:
                    report += f"\n📂 <b>Album:</b> {', '.join(albums)}"
                if stats['unsupported'] > 0:
                    report += f"\n⚠️ {stats['unsupported']} Non supportati"
                if stats['error'] > 0:
                    report += f"\n❌ {stats['error']} Errori"
                try:
                    await app.bot.send_message(chat_id=target_chat, text=report, parse_mode="HTML")
                except:
                    pass
        except asyncio.CancelledError:
            logging.info("Watch folder task cancelled.")
            break

# --- HANDLERS ---
async def handle_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args

    if not args:
        permissions_list = "\n".join([f"• <code>{perm}</code> — {desc}" for perm, desc in REQUIRED_PERMISSIONS.items()])
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
            f"💡 <b>Важно:</b> Не используйте разрешение <b>All</b> — это избыточно и небезопасно.",
            parse_mode="HTML"
        )
        return

    api_key = args[0]
    status_msg = await update.message.reply_text("⏳ Проверяю ключ и необходимые права...")

    try:
        headers = {"x-api-key": api_key, "Accept": "application/json"}
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)

        if resp.status_code != 200:
            await status_msg.edit_text(f"❌ Неверный API ключ или ошибка подключения к Immich.\nКод ответа: {resp.status_code}")
            return

        user_info = resp.json()
        email = user_info.get("email")
        if not email:
            await status_msg.edit_text("❌ Не удалось получить email из Immich. Проверьте ключ.")
            return

        await status_msg.edit_text("⏳ Проверяю наличие всех необходимых прав API-ключа...\n\nЭто может занять несколько секунд.")
        permissions_results = await check_api_key_permissions(api_key)

        has_failure = any(
            not r.get("status", False) and
            "ℹ️" not in r.get("message", "") and
            "Невозможно проверить" not in r.get("message", "")
            for r in permissions_results.values()
        )

        mapping = load_user_mapping()
        mapping[email] = {"immich_api_key": api_key, "telegram_user_id": user_id, "max_user_id": None}
        save_user_mapping(mapping)

        permissions_report = format_permissions_report(permissions_results)

        if not has_failure:
            final_message = f"✅ <b>Аккаунт успешно настроен!</b>\n\n📧 Email: <code>{email}</code>\n🆔 Telegram ID: <code>{user_id}</code>\n\n{permissions_report}\n\nТеперь вы можете отправлять фото и видео."
        else:
            final_message = f"⚠️ <b>Аккаунт настроен, но есть проблемы с правами!</b>\n\n📧 Email: <code>{email}</code>\n🆔 Telegram ID: <code>{user_id}</code>\n\n{permissions_report}\n\n💡 Проверьте, что отмечены все 10 разрешений, и повторите /setup."

        await status_msg.edit_text(final_message, parse_mode="HTML")
    except requests.exceptions.ConnectionError:
        await status_msg.edit_text("❌ Не удалось подключиться к Immich. Проверьте переменную IMMICH_URL.")
    except Exception as e:
        logging.error(f"Ошибка в handle_setup: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка при проверке ключа: {e}")

async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)

    if not user_config:
        await update.message.reply_text("⚠️ <b>Аккаунт не настроен!</b>\n\nИспользуйте команду:\n<code>/setup ВАШ_API_КЛЮЧ</code>", parse_mode="HTML")
        return

    status_msg = await update.message.reply_text("⏳ Получаю информацию и проверяю права...")

    try:
        permissions_results = await check_api_key_permissions(user_config["api_key"])
        permissions_report = format_permissions_report(permissions_results)

        has_failure = any(
            not r.get("status", False) and
            "ℹ️" not in r.get("message", "") and
            "Невозможно проверить" not in r.get("message", "")
            for r in permissions_results.values()
        )

        headers = {"x-api-key": user_config["api_key"], "Accept": "application/json"}
        resp = requests.get(f"{IMMICH_URL}/api/users/me", headers=headers, timeout=10)
        user_info = resp.json() if resp.status_code == 200 else {}

        status_text = f"📊 <b>Статус аккаунта</b>\n\n"
        status_text += f"📧 <b>Email:</b> <code>{user_config['email']}</code>\n"
        status_text += f"🆔 <b>Telegram ID:</b> <code>{user_id}</code>\n"
        if user_info.get('name'):
            status_text += f"👤 <b>Имя:</b> {user_info.get('name')}\n"

        api_key = user_config["api_key"]
        masked_key = api_key[:6] + "••••••••" + api_key[-4:] if len(api_key) > 12 else "••••••"
        status_text += f"🔑 <b>API Key:</b> <code>{masked_key}</code>\n"

        # Статус Telethon
        status_text += "\n📱 <b>Статус Telethon:</b>\n"
        if telethon_client is not None:
            try:
                is_connected = telethon_client.is_connected()
                is_authorized = await telethon_client.is_user_authorized()
                if is_connected and is_authorized:
                    status_text += "✅ <b>Подключен и авторизован</b> (готов к загрузке файлов >20 МБ)\n"
                elif is_connected and not is_authorized:
                    status_text += "⚠️ <b>Подключен, но не авторизован</b> (проверьте файл сессии)\n"
                else:
                    status_text += "❌ <b>Не подключен</b>\n"
            except Exception as e:
                status_text += f"❌ <b>Ошибка проверки:</b> {str(e)[:60]}\n"
        else:
            status_text += "⚠️ <b>Не настроен</b> (отсутствуют API_ID/API_HASH или файл сессии)\n"

        status_text += f"\n<b>Проверка прав API-ключа:</b>\n{permissions_report}\n\n"

        if not has_failure:
            status_text += "✅ <b>Все права присутствуют. Бот готов к работе!</b>"
        else:
            status_text += "⚠️ <b>Некоторые права отсутствуют. Создайте новый API-ключ и отметьте только необходимые разрешения.</b>"

        await status_msg.edit_text(status_text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в handle_status: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка при получении статуса: {e}")

async def send_tags_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not get_user_config(user_id):
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key>.")
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
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
    if data.get("photographers"):
        keyboard.append([InlineKeyboardButton("👤 FOTOGRAFI:", callback_data="none")])
        row = []
        for i, foto in enumerate(data["photographers"][:4]):
            label = f"✅ {foto}" if curr[1] == foto else foto
            row.append(InlineKeyboardButton(label, callback_data=f"foto_{i}"))
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
    if not get_user_config(user_id):
        await query.answer("Аккаунт не настроен!", show_alert=True)
        return

    data = query.data
    hist = load_history()
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
    except:
        pass

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not get_user_config(user_id):
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key>.")
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
        update_history(ta, tf, update.effective_chat.id)
        await update.message.reply_text(f"✅ Tag aggiornati!\n📂 {curr_mem['tags'][0]}\n👤 {curr_mem['tags'][1]}")

async def handle_any_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_config = get_user_config(user_id)
    if not user_config:
        await update.message.reply_text("⚠️ <b>Аккаунт не настроен!</b>\n\nПрежде чем отправлять файлы, настройте свой аккаунт Immich:\n<code>/setup ВАШ_API_КЛЮЧ</code>", parse_mode="HTML")
        return

    update_history(None, None, update.effective_chat.id)
    msg = update.message
    tags = get_effective_tags(user_id, msg.caption or "")
    raw_caption = msg.caption or None
    file_obj = None
    fname = "file.jpg"

    if msg.document:
        fname = msg.document.file_name or "file"
        mime = msg.document.mime_type or ""
        file_size = msg.document.file_size or 0
        if "image" in mime or "video" in mime or fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
            # Большие файлы через Telethon
            if file_size > 20 * 1024 * 1024 and telethon_client:
                job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
                os.makedirs(job_dir, exist_ok=True)
                dest_path = os.path.join(job_dir, fname)
                sm = await msg.reply_text("⏳ <b>Скачивание большого файла через Telethon...</b>", parse_mode="HTML")
                try:
                    async with TELETHON_SEMAPHORE:
                        telethon_msg = await telethon_get_message_safe(context, msg.chat, msg.message_id)
                        if not telethon_msg.media:
                            raise Exception("В сообщении нет медиа для скачивания")
                        logging.info(f"⬇️ Скачиваю {fname} ({file_size / 1024 / 1024:.1f} МБ) → {dest_path}")
                        await telethon_download_to_path(telethon_msg, dest_path)

                    if not os.path.exists(dest_path):
                        raise Exception(f"Файл не появился на диске: {dest_path}")
                    size = os.path.getsize(dest_path)
                    if size == 0:
                        raise Exception("Файл пустой (0 байт)")
                    logging.info(f"✅ Скачано: {dest_path} ({size / 1024 / 1024:.1f} МБ)")

                    # Если нет расширения архива — добавляем .zip
                    if not fname.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
                        new_path = dest_path + ".zip"
                        os.rename(dest_path, new_path)
                        dest_path = new_path
                        fname = fname + ".zip"
                        logging.info(f"📦 Добавлено расширение: {dest_path}")

                    stats, albums = await process_directory_content(
                        job_dir, msg.date, msg.message_id,
                        user_config=user_config,
                        manual_tags=tags, bot=context.bot, chat_id=msg.chat_id,
                        caption=raw_caption
                    )
                    shutil.rmtree(job_dir)
                    rep = f"📦 <b>Большой архив обработан!</b>\n✅ {stats['success']} Фото\n♻️ {stats['duplicate']} Дубликатов"
                    if albums:
                        rep += f"\n📂 <b>Альбомы:</b> {', '.join(albums)}"
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
                    return
                except Exception as e:
                    logging.error(f"Telethon download error: {e}", exc_info=True)
                    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=f"❌ Ошибка скачивания: {e}")
                    shutil.rmtree(job_dir, ignore_errors=True)
                    return
            # Малые файлы через Bot API
            try:
                file_obj = await msg.document.get_file()
            except TelegramError as e:
                if "too big" in str(e).lower():
                    await msg.reply_text(f"❌ Файл слишком большой ({file_size / (1024 * 1024):.1f} МБ). Настройте Telethon или используйте папку импорта.")
                    return
                raise
    elif msg.photo:
        file_obj = await msg.photo[-1].get_file()
        fname = f"photo_{file_obj.file_unique_id}.jpg"
    elif msg.video:
        file_obj = await msg.video.get_file()
        fname = msg.video.file_name or f"video_{file_obj.file_unique_id}.mp4"
    elif msg.video_note:
        file_size = msg.video_note.file_size or 0
        # Большие видеосообщения через Telethon
        if file_size > 20 * 1024 * 1024 and telethon_client:
            job_dir = os.path.join(TEMP_DIR, f"vnote_{msg.message_id}")
            os.makedirs(job_dir, exist_ok=True)
            fname = f"video_note_{msg.video_note.file_unique_id}.mp4"
            dest_path = os.path.join(job_dir, fname)
            sm = await msg.reply_text("⏳ <b>Скачивание большого видеосообщения через Telethon...</b>", parse_mode="HTML")
            try:
                async with TELETHON_SEMAPHORE:
                    telethon_msg = await telethon_get_message_safe(context, msg.chat, msg.message_id)
                    if not telethon_msg.media:
                        raise Exception("В сообщении нет медиа для скачивания")
                    await telethon_download_to_path(telethon_msg, dest_path)

                if not os.path.exists(dest_path) or os.path.getsize(dest_path) == 0:
                    raise Exception("Файл не скачан или пустой")

                status, _ = await upload_file_path_to_immich(
                    dest_path, fname, msg.date, msg.message_id,
                    user_api_key=user_config["api_key"],
                    extra_album=tags[0] if tags and tags[0] else None,
                    fotografo=tags[1] if tags and len(tags) > 1 else None,
                    caption=raw_caption
                )
                shutil.rmtree(job_dir)
                report = "✅ Видео загружено!" if status == "success" else "♻️ Дубликат (теги обновлены)."
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=report, parse_mode="HTML")
                return
            except Exception as e:
                logging.error(f"Telethon video_note download error: {e}", exc_info=True)
                await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=f"❌ Ошибка скачивания: {e}")
                shutil.rmtree(job_dir, ignore_errors=True)
                return
        # Малые видеосообщения через Bot API
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
            job_dir = os.path.join(TEMP_DIR, f"zip_{msg.message_id}")
            os.makedirs(job_dir, exist_ok=True)
            sm = await msg.reply_text("⏳ <b>Archivio ricevuto...</b>", parse_mode="HTML")
            await file_obj.download_to_drive(os.path.join(job_dir, fname))
            stats, albums = await process_directory_content(
                job_dir, msg.date, msg.message_id,
                user_config=user_config,
                manual_tags=tags, bot=context.bot, chat_id=msg.chat_id,
                caption=raw_caption
            )
            shutil.rmtree(job_dir)
            rep = f"📦 <b>Elaborato!</b>\n✅ {stats['success']} Foto\n♻️ {stats['duplicate']} Duplicati"
            if albums:
                rep += f"\n📂 <b>Album:</b> {', '.join(albums)}"
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=sm.message_id, text=rep, parse_mode="HTML")
        else:
            tmp = os.path.join(TEMP_DIR, fname)
            await file_obj.download_to_drive(tmp)
            status, _ = await upload_file_path_to_immich(
                tmp, fname, msg.date, msg.message_id,
                user_api_key=user_config["api_key"],
                extra_album=tags[0] if tags and tags[0] else None,
                fotografo=tags[1] if tags and len(tags) > 1 else None,
                caption=raw_caption
            )
            os.remove(tmp)
            report = "✅ Caricata!" if status == "success" else "♻️ Duplicata (Tag Aggiornati)."
            if tags and tags[0]:
                report += f"\n📂 Album: {tags[0]}"
            if tags and len(tags) > 1 and tags[1]:
                report += f"\n👤 Foto: {tags[1]}"
            await msg.reply_text(report, parse_mode="HTML")

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not get_user_config(user_id):
        await update.message.reply_text("⚠️ Ваш аккаунт не настроен. Используйте команду /setup <api_key>.")
        return

    update_history(None, None, update.effective_chat.id)
    url_m = re.search(r'(https?://\S+)', update.message.text)
    if not url_m:
        return
    url = url_m.group(1)
    tags = get_effective_tags(user_id, update.message.text)

    job_dir = os.path.join(TEMP_DIR, f"job_{update.message.message_id}")
    os.makedirs(job_dir, exist_ok=True)
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
            downloaded_files = [f for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f))]
        except:
            pass

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
    rep = f"✅ <b>Completato!</b>\n{stats['success']} Caricati\n{stats['duplicate']} Duplicati"
    if albums:
        rep += f"\n📂 Album: {', '.join(albums)}"
    await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id, text=rep, parse_mode="HTML")

# --- ERROR HANDLER ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, TelegramError):
        logging.warning(f"⚠️ Ошибка Telegram: {error}. Повторная попытка будет выполнена автоматически.")
    else:
        logging.error("❌ Неожиданная ошибка в боте:", exc_info=error)

# --- MAIN ---
async def main():
    # Инициализируем Telethon в том же event loop, что и бот
    await init_telethon_client()

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
    app.add_handler(CommandHandler("status", handle_status))
    app.add_handler(CommandHandler("start", send_tags_menu))
    app.add_handler(CommandHandler("tags", send_tags_menu))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Entity("url") | filters.Entity("text_link"), handle_url))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.VIDEO_NOTE | filters.Document.ALL, handle_any_media))
    print("Bot avviato (v60 - Concurrent Safety + Telethon Session Copy + Safe Download)...")
    asyncio.run(main())