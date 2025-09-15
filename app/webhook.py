from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types

from .config import settings

app = FastAPI()
_dp: Dispatcher | None = None
_bot: Bot | None = None


def set_dispatcher(dp: Dispatcher):
    global _dp, _bot
    _dp = dp
    _bot = dp.bot


@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None:
        return {"status": "dispatcher not ready"}
    data = await request.json()
    update = types.Update(**data)
    await _dp.process_update(update)
    return {"status": "ok"}


async def setup_webhook(bot: Bot):
    await bot.set_webhook(settings.WEBHOOK_URL)


async def remove_webhook(bot: Bot):
    await bot.delete_webhook()
