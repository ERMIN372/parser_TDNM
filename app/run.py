from __future__ import annotations

import asyncio
import logging
import os

import aiogram
import aiohttp
import uvicorn
from aiogram import Dispatcher, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from .utils.logging import setup_logging

setup_logging()

from . import webhook
from .config import settings
from .handlers import (  # noqa: E402  (setup_logging must run before handlers)
    admin as h_admin,
    admin_debug,
    parse,
    payments as h_payments,
    referrals as h_referrals,
    start,
    status,
)
from .middlewares.busy import BusyMiddleware
from .middlewares.debug import DebugMiddleware
from .middlewares.operation_logger import OperationLoggerMiddleware
from .storage.db import init_db
from .utils.logging import (
    build_audit_summary,
    complete_operation,
    log_event,
    set_audit_sink,
)
from .utils.telegram_logging import LoggedBot


logger = logging.getLogger("app.startup")


def create_dispatcher() -> Dispatcher:
    bot = LoggedBot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher(bot, storage=MemoryStorage())

    dp.middleware.setup(DebugMiddleware())
    dp.middleware.setup(OperationLoggerMiddleware())
    dp.middleware.setup(BusyMiddleware())

    audit_chat_id = os.getenv("LOG_TO_AUDIT_CHAT_ID")
    if audit_chat_id:
        async def _send_audit(payload: dict) -> None:
            text = build_audit_summary(payload)
            try:
                await bot.send_message(int(audit_chat_id), text)
            except Exception as exc:  # pragma: no cover - audit is best effort
                log_event("audit_delivery_failed", level="WARN", err=str(exc))

        set_audit_sink(_send_audit)

    start.register(dp)
    status.register(dp)
    parse.register(dp)
    h_payments.register(dp)
    h_referrals.register(dp)
    h_admin.register(dp)
    admin_debug.register(dp)

    async def _error_handler(update, error):  # noqa: ANN001
        log_event("exception", level="ERROR", err=str(error))
        complete_operation(ok=False, err=str(error))
        return True

    dp.register_errors_handler(_error_handler)

    return dp


dp = create_dispatcher()
bot = dp.bot


def _resolve_listen_port() -> int:
    port_env = os.getenv("PORT")
    if port_env is None:
        if settings.MODE == "webhook":
            raise RuntimeError("PORT environment variable must be set in webhook mode")
        return settings.WEBAPP_PORT
    try:
        return int(port_env)
    except ValueError as exc:
        raise RuntimeError("PORT environment variable must be an integer") from exc


def _validate_webhook_url() -> None:
    url = (settings.WEBHOOK_URL or "").strip()
    if not url:
        raise RuntimeError("WEBHOOK_URL must be set when MODE=webhook")
    if not url.startswith("https://"):
        raise RuntimeError("WEBHOOK_URL must start with https://")
    if not url.endswith("/webhook"):
        raise RuntimeError("WEBHOOK_URL must end with /webhook")


def main() -> None:
    init_db()

    log_event(
        "bot_start",
        message=(
            f"Starting bot in {settings.MODE} mode "
            f"(aiogram={aiogram.__version__}, aiohttp={aiohttp.__version__})"
        ),
    )

    listen_port = _resolve_listen_port()
    if settings.MODE == "webhook":
        _validate_webhook_url()
    settings.WEBAPP_PORT = listen_port

    logger.info(
        "Final configuration: mode=%s, webhook_url=%s, port=%s",
        settings.MODE,
        settings.WEBHOOK_URL or "-",
        listen_port,
    )

    if settings.MODE == "polling":
        executor.start_polling(dp, skip_updates=True)
        return

    webhook.set_dispatcher(dp)

    async def _run() -> None:
        webhook_setup_success = False
        try:
            try:
                await webhook.setup_webhook(bot)
                webhook_setup_success = True
                log_event("webhook_setup", message="Webhook configured successfully")
            except Exception as exc:
                log_event(
                    "webhook_setup_failed",
                    level="WARN",
                    message=f"Initial webhook setup failed: {exc}. Server will start anyway.",
                )
                logger.warning("Initial webhook setup failed: %s", exc, exc_info=True)

            config = uvicorn.Config(
                webhook.app,
                host=settings.WEBAPP_HOST,
                port=listen_port,
                log_level="info",
            )
            server = uvicorn.Server(config)
            log_event(
                "server_start",
                message=f"Starting server on {settings.WEBAPP_HOST}:{listen_port}",
            )
            logger.info("Uvicorn running on http://%s:%s", settings.WEBAPP_HOST, listen_port)
            await server.serve()
        finally:
            # Only try to remove webhook if it was successfully set
            if webhook_setup_success:
                try:
                    await webhook.remove_webhook(bot)
                except Exception as exc:
                    log_event(
                        "webhook_cleanup_failed",
                        level="WARN",
                        message=f"Failed to cleanup webhook: {exc}",
                    )
                    logger.warning("Failed to cleanup webhook: %s", exc, exc_info=True)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
