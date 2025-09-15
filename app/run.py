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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

# register handlers
start.register(dp)
status.register(dp)
parse.register(dp)


def main():
    log.info(
        "Starting bot in %s mode (aiogram=%s, aiohttp=%s)",
        settings.MODE,
        aiogram.__version__,
        aiohttp.__version__,
    )
    if settings.MODE == "polling":
        executor.start_polling(dp, skip_updates=True)
    else:
        webhook.set_dispatcher(dp)

        async def _run():
            try:
                await webhook.setup_webhook(bot)
                config = uvicorn.Config(webhook.app, host=settings.WEBAPP_HOST, port=settings.WEBAPP_PORT, log_level="info")
                server = uvicorn.Server(config)
                await server.serve()
            finally:
                await webhook.remove_webhook(bot)
        asyncio.run(_run())


if __name__ == "__main__":
    main()
