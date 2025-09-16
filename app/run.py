import os
import asyncio
import logging

from aiogram import Bot, Dispatcher, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import aiogram
import aiohttp
import uvicorn

from .config import settings
from .handlers import start, status, parse
from . import webhook
from .storage.db import init_db

# ---------- логирование ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------- глобальный семафор для ограничения параллельных задач ----------
BOT_CONCURRENCY = int(os.getenv("BOT_CONCURRENCY", "2"))
SEM = asyncio.Semaphore(BOT_CONCURRENCY)  # импортируй в хендлере: from app.run import SEM

# ---------- инициализация бота/диспетчера ----------
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

# ---------- регистрация хендлеров ----------
start.register(dp)
status.register(dp)
parse.register(dp)


def main():
    # создаём БД и таблицы, если их ещё нет
    init_db()

    log.info(
        "Starting bot in %s mode (aiogram=%s, aiohttp=%s)",
        settings.MODE,
        aiogram.__version__,
        aiohttp.__version__,
    )

    if settings.MODE == "polling":
        # локальный режим: long polling
        executor.start_polling(dp, skip_updates=True)

    else:
        # прод-режим: webhook (например, на Replit)
        webhook.set_dispatcher(dp)

        async def _run():
            try:
                # регистрируем вебхук у Telegram
                await webhook.setup_webhook(bot)

                # поднимаем HTTP-сервер для приёма апдейтов
                config = uvicorn.Config(
                    webhook.app,
                    host=settings.WEBAPP_HOST,
                    port=settings.WEBAPP_PORT,
                    log_level="info",
                )
                server = uvicorn.Server(config)
                await server.serve()
            finally:
                # на выключении снимаем вебхук
                await webhook.remove_webhook(bot)

        asyncio.run(_run())


if __name__ == "__main__":
    main()
