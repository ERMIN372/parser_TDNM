
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from aiogram import Bot, Dispatcher, types

from .config import settings
from .utils.logging import log_event


app = FastAPI()
_dp: Dispatcher | None = None
_bot: Bot | None = None


def set_dispatcher(dp: Dispatcher):
    global _dp, _bot
    _dp = dp
    _bot = dp.bot


@app.get("/")
async def root() -> PlainTextResponse:
    return PlainTextResponse("HR-Assist webhook is running.")


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "version": settings.BUILD_VERSION})


@app.get("/_debug")
async def debug_info() -> JSONResponse:
    data = {
        "status": "ok",
        "mode": settings.MODE,
        "webhook_url": settings.WEBHOOK_URL,
        "webapp_port": settings.WEBAPP_PORT,
        "webapp_port_source": settings.WEBAPP_PORT_SOURCE,
        "build_version": settings.BUILD_VERSION,
        "dispatcher_ready": _dp is not None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "health_url": f"http://0.0.0.0:{settings.WEBAPP_PORT}/health",
    }
    return JSONResponse(data)


@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None:
        return {"status": "dispatcher not ready"}
    data = await request.json()
    update = types.Update(**data)
    if _bot:
        Bot.set_current(_bot)
    await _dp.process_update(update)
    return {"status": "ok"}


async def setup_webhook(bot: Bot):
    """Setup webhook with validation and logging."""
    if not settings.WEBHOOK_URL:
        raise ValueError("WEBHOOK_URL is required for webhook mode")

    if not settings.WEBHOOK_URL.startswith(("https://", "http://localhost")):
        raise ValueError("WEBHOOK_URL must use HTTPS (or HTTP for localhost)")

    if '/webhook' not in settings.WEBHOOK_URL:
        raise ValueError("WEBHOOK_URL should end with '/webhook' endpoint")

    info = await bot.get_webhook_info()
    log_event(
        "webhook_info",
        message="Webhook info fetched",
        url=info.url,
        pending_updates=info.pending_update_count,
        last_error_message=info.last_error_message,
        has_custom_certificate=info.has_custom_certificate,
        ip_address=info.ip_address,
    )

    desired_url = settings.WEBHOOK_URL
    if info.url == desired_url:
        log_event("webhook_setup", message="Webhook already configured", url=desired_url)
        return

    if info.url:
        await bot.delete_webhook(drop_pending_updates=True)
        log_event("webhook_deleted", message="Previous webhook removed", previous_url=info.url)

    await bot.set_webhook(desired_url)
    log_event("webhook_setup", message="Webhook configured successfully", url=desired_url)


async def remove_webhook(bot: Bot):
    await bot.delete_webhook()
    log_event("webhook_removed", message="Webhook removed")
