import asyncio
import logging
import os
from warnings import filterwarnings

from celery import Celery
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.warnings import PTBUserWarning

filterwarnings(
    action="ignore",
    message=r".*CallbackQueryHandler",
    category=PTBUserWarning,
)

log_level_colors = {
    logging.INFO: "\033[32m",
    logging.WARNING: "\033[33m",
    logging.DEBUG: "\033[36m",
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[41m",
    "RESET": "\033[0m",
}


class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = log_level_colors.get(record.levelno, "")
        reset = log_level_colors["RESET"]
        message = super().format(record)
        return f"{color}{message}{reset}"


log_handler = logging.StreamHandler()
log_handler.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] - %(message)s"))

logger = logging.getLogger("bot")
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

# Лёгкий клиент Celery: задачу ставим по имени, код воркера (yt-dlp, ffmpeg) боту не нужен
celery_app = Celery("downloader", broker=BROKER_URL, backend=RESULT_BACKEND)
celery_app.conf.update(task_serializer="json", accept_content=["json"])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Пришли ссылку, и я скачаю аудио.")


async def msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    url = text.split("&")[0]

    if not url.startswith(("http://", "https://")):
        await update.message.reply_text("Пришли ссылку, которая начинается с http:// или https://")
        return

    chat_id = update.effective_chat.id
    try:
        # send_task синхронный (обращается к Redis), поэтому уводим его в поток
        result = await asyncio.to_thread(
            celery_app.send_task,
            "downloader.download_and_send",
            args=[url, chat_id],
        )
    except Exception:
        logger.exception("Не удалось поставить задачу в очередь")
        await update.message.reply_text("Ошибка: не удалось поставить задачу в очередь")
        return

    logger.info("Задача %s поставлена: chat_id=%s url=%s", result.id, chat_id, url)
    await update.message.reply_text("Скачиваю, пришлю, когда будет готово")


def run():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg))
    app.run_polling()


if __name__ == "__main__":
    run()