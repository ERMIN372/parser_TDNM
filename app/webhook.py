from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, types
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .utils.logging import setup_logging


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


@app.post("/webhook")
async def handle_update(request: Request):
    if _dp is None:
        return {"status": "dispatcher not ready"}
    data = await request.json()
    update = types.Update(**data)
    await _dp.process_update(update)
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
