from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, types
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .utils.logging import log_event, setup_logging


setup_logging()

logger = logging.getLogger("webhook")
http_logger = logging.getLogger("http")


class AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            duration = (time.perf_counter() - start) * 1000
            client_ip = request.client.host if request.client else "-"
            http_logger.info(
                '%s "%s %s" %s %.1fms',
                client_ip,
                request.method,
                request.url.path,
                status_code,
                duration,
            )


app = FastAPI()
app.add_middleware(AccessLogMiddleware)

_dp: Dispatcher | None = None
_bot: Bot | None = None


def set_dispatcher(dp: Dispatcher):
    global _dp, _bot
    _dp = dp
    _bot = dp.bot


@app.get("/")
async def root_status():
    return {
        "status": "ok",
        "message": "Telegram Bot Server is running",
        "mode": settings.MODE,
        "dispatcher_ready": _dp is not None,
    }


@app.get("/health")
async def health_check():
    port = os.getenv("PORT") or str(settings.WEBAPP_PORT)
    return {
        "status": "ok",
        "mode": settings.MODE,
        "port": port,
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/version")
async def version_info():
    rev = os.getenv("GIT_REV", "local")
    return {"rev": rev, "env": "replit"}


def _extract_update_details(update: types.Update) -> dict[str, object | None]:
    message = (
        update.message
        or update.edited_message
        or update.channel_post
        or update.edited_channel_post
    )

    has_message = message is not None
    message_text = None
    chat_id = None

    if message:
        chat_id = getattr(message.chat, "id", None)
        message_text = getattr(message, "text", None) or getattr(message, "caption", None)
    elif update.callback_query:
        callback = update.callback_query
        if callback.message and callback.message.chat:
            chat_id = callback.message.chat.id
        else:
            chat_id = getattr(callback.from_user, "id", None)
        message_text = callback.data
    elif update.inline_query:
        chat_id = getattr(update.inline_query.from_user, "id", None)
        message_text = update.inline_query.query
    elif update.chosen_inline_result:
        chat_id = getattr(update.chosen_inline_result.from_user, "id", None)
        message_text = update.chosen_inline_result.query
    elif update.shipping_query:
        chat_id = getattr(update.shipping_query.from_user, "id", None)
    elif update.pre_checkout_query:
        chat_id = getattr(update.pre_checkout_query.from_user, "id", None)

    return {
        "update_id": update.update_id,
        "has_message": has_message,
        "message_text": message_text,
        "chat_id": chat_id,
    }


@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None or _bot is None:
        logger.warning("Dispatcher is not ready to handle webhook update")
        return {"status": "dispatcher not ready"}

    data = await request.json()
    update = types.Update(**data)
    details = _extract_update_details(update)

    log_event("webhook_received", **details)

    try:
        await _dp.feed_update(_bot, update)
    except Exception as exc:
        logger.exception("Failed to process webhook update")
        log_event("webhook_error", level="ERROR", err=str(exc), **details)
        raise

    log_event("webhook_dispatched", **details)
    return {"status": "ok"}


async def setup_webhook(bot: Bot) -> None:
    logger.info("Setting webhook to %s", settings.WEBHOOK_URL)
    try:
        await bot.set_webhook(settings.WEBHOOK_URL)
    except Exception:
        logger.exception("Failed to set webhook to %s", settings.WEBHOOK_URL)
        raise
    else:
        logger.info("Webhook successfully set to: %s", settings.WEBHOOK_URL)
        if logger.isEnabledFor(logging.DEBUG):
            info = await bot.get_webhook_info()
            logger.debug("Webhook info: %s", info.to_python())


async def remove_webhook(bot: Bot):
    await bot.delete_webhook()
