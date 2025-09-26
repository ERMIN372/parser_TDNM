
from typing import Any, Dict

from aiogram import Bot, Dispatcher, types
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from .config import settings
from .utils.diagnostics import collect_runtime_diagnostics
from .utils.logging import log_event


app = FastAPI()
_dp: Dispatcher | None = None
_bot: Bot | None = None


def set_dispatcher(dp: Dispatcher):
    global _dp, _bot
    _dp = dp
    _bot = dp.bot
    Dispatcher.set_current(dp)
    Bot.set_current(dp.bot)


@app.get("/")
async def root() -> PlainTextResponse:
    return PlainTextResponse("HR-Assist webhook is running.")


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "version": settings.BUILD_VERSION})


@app.get("/_debug")
async def debug_info() -> JSONResponse:
    data: Dict[str, Any] = {
        "status": "ok",
        "mode": settings.MODE,
        "webhook_url": settings.WEBHOOK_URL,
        "webapp_port": settings.WEBAPP_PORT,
        "webapp_port_source": settings.WEBAPP_PORT_SOURCE,
        "build_version": settings.BUILD_VERSION,
        "health_url": f"http://0.0.0.0:{settings.WEBAPP_PORT}/health",
    }
    data.update(collect_runtime_diagnostics(_dp))
    return JSONResponse(data)


@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None:
        return {"status": "dispatcher not ready"}
    data = await request.json()
    update = types.Update(**data)

    try:
        previous_bot = Bot.get_current()
    except LookupError:
        previous_bot = None

    try:
        previous_dp = Dispatcher.get_current()
    except LookupError:
        previous_dp = None

    if _bot:
        Bot.set_current(_bot)

    if previous_dp is None:
        log_event(
            "dispatcher_context_reset",
            level="DEBUG",
            message="Dispatcher context was empty before processing update",
        )

    Dispatcher.set_current(_dp)
    try:
        await _dp.process_update(update)
    finally:
        if previous_dp:
            Dispatcher.set_current(previous_dp)
        if previous_bot:
            Bot.set_current(previous_bot)
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
