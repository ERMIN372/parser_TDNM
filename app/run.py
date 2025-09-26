from __future__ import annotations

import asyncio
import logging
import os
import traceback

import aiogram
import aiohttp
import uvicorn
from aiogram import Bot, Dispatcher, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import TelegramAPIError

from . import webhook
from .config import settings
from .handlers import (
    admin as h_admin,
    parse,
    payments as h_payments,
    referrals as h_referrals,
    start,
    status,
)
from .middlewares.busy import BusyMiddleware
from .middlewares.operation_logger import OperationLoggerMiddleware
from .storage.db import init_db
from .utils.logging import (
    build_audit_summary,
    complete_operation,
    get_operation_context,
    log_event,
    set_audit_sink,
    setup_logging,
)
from .utils.telegram_logging import LoggedBot


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("bot")


def create_dispatcher() -> Dispatcher:
    setup_logging()

    bot = LoggedBot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
    Bot.set_current(bot)
    dp = Dispatcher(bot, storage=MemoryStorage())

    # ---- global errors handler (aiogram v2) ----
    @dp.errors_handler()
    async def _global_errors_handler(update, exception):
        tb = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        exc_type = type(exception).__name__
        if isinstance(exception, TelegramAPIError):
            exc_type = f"TelegramAPIError:{exc_type}"

        snapshot: dict[str, object] | str
        try:
            snapshot = update.to_python() if hasattr(update, "to_python") else str(update)
        except Exception:  # pragma: no cover - defensive logging
            snapshot = str(update)

        ctx = get_operation_context()
        chat_id = getattr(ctx, "chat_id", None) if ctx else None

        log_event(
            "unhandled_exception",
            level="ERROR",
            message=str(exception) or exc_type,
            exception_type=exc_type,
            traceback=tb[:15000],
            update_snapshot=snapshot,
            chat_id=chat_id,
        )
        complete_operation(ok=False, err=str(exception) or exc_type, force=True)
        return True
    # -------------------------------------------

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

    async def _error_handler(update, error):  # noqa: ANN001
        log_event("exception", level="ERROR", err=str(error))
        complete_operation(ok=False, err=str(error))
        return True

    dp.register_errors_handler(_error_handler)

    return dp


dp = create_dispatcher()
bot = dp.bot


def main() -> None:
    init_db()

    log_event(
        "bot_start",
        message=(
            f"Starting bot in {settings.MODE} mode "
            f"(aiogram={aiogram.__version__}, aiohttp={aiohttp.__version__})"
        ),
    )

    logger.info("startup_ok", extra={"event": "startup"})

    health_url = f"http://0.0.0.0:{settings.WEBAPP_PORT}/health"
    log_event(
        "startup_banner",
        message=(
            f"mode={settings.MODE} webhook_url={settings.WEBHOOK_URL or '-'} "
            f"port={settings.WEBAPP_PORT} (source={settings.WEBAPP_PORT_SOURCE}) "
            f"health={health_url}"
        ),
        mode=settings.MODE,
        webhook_url=settings.WEBHOOK_URL,
        webapp_port=settings.WEBAPP_PORT,
        webapp_port_source=settings.WEBAPP_PORT_SOURCE,
        health_url=health_url,
        build_version=settings.BUILD_VERSION,
    )

    if settings.MODE == "polling":
        executor.start_polling(dp, skip_updates=True)
        return

    webhook.set_dispatcher(dp)

    async def _run() -> None:
        webhook_setup_success = False
        try:
            # Try to setup webhook, but don't fail if it doesn't work initially
            try:
                await webhook.setup_webhook(bot)
                webhook_setup_success = True
                log_event("webhook_setup", message="Webhook configured successfully")
            except Exception as e:
                log_event("webhook_setup_failed", level="WARN",
                         message=f"Initial webhook setup failed: {e}. Server will start anyway.")
                print(f"Warning: Webhook setup failed: {e}")
                print("The server will start anyway. You can manually set the webhook later.")
            
            # Start the server regardless of webhook setup status
            config = uvicorn.Config(
                webhook.app,
                host="0.0.0.0",
                port=settings.WEBAPP_PORT,
                log_level="info",
            )
            server = uvicorn.Server(config)
            log_event(
                "server_start",
                message=f"Starting server on 0.0.0.0:{settings.WEBAPP_PORT}",
            )
            await server.serve()
        finally:
            # Only try to remove webhook if it was successfully set
            if webhook_setup_success:
                try:
                    await webhook.remove_webhook(bot)
                except Exception as e:
                    log_event("webhook_cleanup_failed", level="WARN", message=f"Failed to cleanup webhook: {e}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
