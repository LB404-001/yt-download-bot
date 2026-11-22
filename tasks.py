import os
import shutil
import subprocess
import time
import uuid
from contextlib import ExitStack
from pathlib import Path
from typing import Optional

import requests
from celery import Celery
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/downloads"))
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

TG_MAX_SIZE = 50 * 1024 * 1024       # лимит Bot API на отправку файлов
TG_THUMB_MAX_SIZE = 200 * 1024       # лимит Telegram на миниатюру
THUMB_BG_COLOR = os.getenv("THUMB_BG_COLOR", "black")  # цвет полей

# Обход «Sign in to confirm you're not a bot»
COOKIES_FILE = os.getenv("COOKIES_FILE")      # путь к cookies.txt (формат Netscape)
YTDLP_PROXY = os.getenv("YTDLP_PROXY")        # например http://user:pass@host:port
FIREFOX_ROOT = Path(os.getenv("FIREFOX_PROFILE_ROOT", "/firefox/.mozilla/firefox"))

app = Celery("downloader", broker=BROKER_URL, backend=RESULT_BACKEND)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,               # задача не потеряется, если воркер упадёт
    worker_prefetch_multiplier=1,      # долгие задачи не набираем про запас
    task_soft_time_limit=600,
    task_time_limit=660,
    result_expires=3600,
)


# ---------------------------------------------------------------- yt-dlp ---

def _find_firefox_profile() -> Optional[Path]:
    """Ищет профиль Firefox с кукисами; при нескольких берёт тот, что обновлялся последним."""
    candidates = [p.parent for p in FIREFOX_ROOT.glob("*/cookies.sqlite")]
    if not candidates:
        return None
    return max(candidates, key=lambda p: (p / "cookies.sqlite").stat().st_mtime)


def _build_opts(workdir: Path) -> dict:
    opts = {
        "quiet": False,
        "format": "bestaudio",
        "writethumbnail": True,
        "outtmpl": str(workdir / "%(title)s.%(ext)s"),
        "postprocessors": [
            {"key": "FFmpegThumbnailsConvertor", "format": "jpg", "when": "before_dl"},
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3"},
            {"key": "FFmpegMetadata", "add_metadata": True},
            # already_have_thumbnail: не удалять jpg после встраивания,
            # он нужен для миниатюры в Telegram
            {"key": "EmbedThumbnail", "already_have_thumbnail": True},
        ],
    }

    profile = _find_firefox_profile()
    if profile:
        opts["cookiesfrombrowser"] = ("firefox", str(profile))
    elif COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        # yt-dlp перезаписывает cookie-файл, а исходный смонтирован только для чтения,
        # поэтому каждая задача работает со своей копией
        local_cookies = workdir / "cookies.txt"
        shutil.copyfile(COOKIES_FILE, local_cookies)
        opts["cookiefile"] = str(local_cookies)

    if YTDLP_PROXY:
        opts["proxy"] = YTDLP_PROXY

    return opts


# -------------------------------------------------------------- thumbnail ---

def _prepare_thumbnail(audio_path: str, workdir: Path) -> Optional[Path]:
    """Делает из обложки jpg 320x320 с полями (без обрезки) и размером <= 200 КБ.

    Возвращает None, если не получилось: тогда аудио уйдёт без миниатюры.
    """
    src = Path(audio_path).with_suffix(".jpg")
    if not src.exists():
        candidates = [p for p in workdir.glob("*.jpg") if p.name != "tg_thumb.jpg"]
        if not candidates:
            return None
        src = candidates[0]

    dst = workdir / "tg_thumb.jpg"
    # вписываем картинку целиком в 320x320 и добиваем полями до квадрата
    vf = (
        "scale=320:320:force_original_aspect_ratio=decrease,"
        f"pad=320:320:(ow-iw)/2:(oh-ih)/2:color={THUMB_BG_COLOR}"
    )
    # качество понижаем, пока не влезем в 200 КБ
    for q in (3, 6, 10, 15, 25):
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-i", str(src),
                 "-vf", vf, "-frames:v", "1", "-q:v", str(q), str(dst)],
                check=True, timeout=60,
            )
        except (subprocess.SubprocessError, OSError):
            return None
        if dst.stat().st_size <= TG_THUMB_MAX_SIZE:
            return dst
    return None


# --------------------------------------------------------------- telegram ---

def _tg_send_message(chat_id: int, text: str) -> None:
    try:
        requests.post(f"{TG_API}/sendMessage",
                      data={"chat_id": chat_id, "text": text}, timeout=30)
    except requests.RequestException:
        pass


def _tg_send_audio(chat_id: int, meta: dict, thumb: Optional[Path] = None,
                   attempts: int = 3) -> dict:
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            with ExitStack() as stack:
                files = {
                    "audio": (Path(meta["path"]).name,
                              stack.enter_context(open(meta["path"], "rb")),
                              "audio/mpeg"),
                }
                if thumb:
                    files["thumbnail"] = ("thumb.jpg",
                                          stack.enter_context(open(thumb, "rb")),
                                          "image/jpeg")
                resp = requests.post(
                    f"{TG_API}/sendAudio",
                    data={
                        "chat_id": chat_id,
                        "title": meta["title"] or "",
                        "performer": meta["performer"] or "",
                        "duration": int(meta["duration"] or 0),
                    },
                    files=files,
                    timeout=(10, 300),
                )
            data = resp.json()
            if data.get("ok"):
                return data["result"]
            # 429: Telegram просит подождать
            if resp.status_code == 429:
                time.sleep(data.get("parameters", {}).get("retry_after", 5))
                continue
            raise RuntimeError(f"Telegram error: {data}")
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            time.sleep(2 * attempt)
    raise RuntimeError(f"Не удалось отправить файл: {last_exc}")


# ------------------------------------------------------------------- task ---

@app.task(
    bind=True,
    name="downloader.download_and_send",
    autoretry_for=(DownloadError,),
    retry_backoff=True,
    retry_backoff_max=60,
    max_retries=2,
)
def download_and_send(self, url: str, chat_id: int) -> dict:
    workdir = DOWNLOAD_DIR / (self.request.id or uuid.uuid4().hex)
    workdir.mkdir(parents=True, exist_ok=True)

    try:
        with YoutubeDL(_build_opts(workdir)) as ydl:
            info = ydl.extract_info(url)

        meta = {
            "path": info["requested_downloads"][0]["filepath"],
            "title": info.get("title"),
            "performer": info.get("uploader"),
            "duration": info.get("duration"),  # секунды
        }

        if os.path.getsize(meta["path"]) > TG_MAX_SIZE:
            _tg_send_message(chat_id, "Файл больше 50 МБ, Telegram не даст его отправить.")
            return {"status": "too_large", "title": meta["title"]}

        thumb = _prepare_thumbnail(meta["path"], workdir)
        msg = _tg_send_audio(chat_id, meta, thumb)
        return {"status": "sent", "title": meta["title"], "message_id": msg["message_id"]}

    except DownloadError:
        # если ретраи ещё остались, Celery перезапустит задачу, пользователю пока не пишем
        if self.request.retries >= self.max_retries:
            _tg_send_message(chat_id, "Не удалось скачать это видео.")
        raise
    except Exception:
        _tg_send_message(chat_id, "Произошла ошибка при обработке запроса.")
        raise
    finally:
        shutil.rmtree(workdir, ignore_errors=True)