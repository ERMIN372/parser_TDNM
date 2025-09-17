import os
import asyncio
import logging

import aiogram
import aiohttp
import uvicorn
from aiogram import Bot, Dispatcher, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from .middlewares.busy import BusyMiddleware

from .config import settings
from .handlers import (
    start,
    status,
    parse,
    payments as h_payments,
    admin as h_admin,
)
from . import webhook
from .storage.db import init_db

bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

dp.middleware.setup(BusyMiddleware())  # ← ВАЖНО: глобальная блокировка

# ---------- логирование ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------- инициализация бота / диспетчера ----------
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

# ---------- регистрация хендлеров (после создания dp!) ----------
start.register(dp)
status.register(dp)
parse.register(dp)
h_payments.register(dp)   # /buy + callback'и оплаты
h_admin.register(dp)      # /admin, /addcredits, /grant_unlim, /astats


def main():
    # создать БД/таблицы при старте
    init_db()

    log.info(
        "Starting bot in %s mode (aiogram=%s, aiohttp=%s)",
        settings.MODE,
        aiogram.__version__,
        aiohttp.__version__,
    )

    if settings.MODE == "polling":
        # локально / на Replit: long-polling
        executor.start_polling(dp, skip_updates=True)
    else:
        # вебхук-режим (если используешь)
        webhook.set_dispatcher(dp)

        async def _run():
            try:
                await webhook.setup_webhook(bot)
                config = uvicorn.Config(
                    webhook.app,
                    host=settings.WEBAPP_HOST,
                    port=settings.WEBAPP_PORT,
                    log_level="info",
                )
                server = uvicorn.Server(config)
                await server.serve()
            finally:
                await webhook.remove_webhook(bot)

        asyncio.run(_run())


if __name__ == "__main__":
    main()
