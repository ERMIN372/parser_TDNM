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


@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None:
        return {"status": "dispatcher not ready"}
    data = await request.json()
    update = types.Update(**data)
    await _dp.process_update(update)
    return {"status": "ok"}


async def setup_webhook(bot: Bot):
    """Setup webhook with validation and error handling for deployment"""
    if not settings.WEBHOOK_URL:
        raise ValueError("WEBHOOK_URL is required for webhook mode")
    
    # Validate webhook URL format
    if not settings.WEBHOOK_URL.startswith(('https://', 'http://localhost')):
        raise ValueError("WEBHOOK_URL must use HTTPS (or HTTP for localhost)")
    
    if '/webhook' not in settings.WEBHOOK_URL:
        raise ValueError("WEBHOOK_URL should end with '/webhook' endpoint")
    
    try:
        await bot.set_webhook(settings.WEBHOOK_URL)
        print(f"Webhook successfully set to: {settings.WEBHOOK_URL}")
    except Exception as e:
        print(f"Failed to set webhook: {e}")
        print("This might be expected during development or if the deployment URL is not yet accessible")
        # Re-raise for proper error handling
        raise


async def remove_webhook(bot: Bot):
    await bot.delete_webhook()
