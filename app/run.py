from __future__ import annotations

import asyncio
import os

import aiogram
import aiohttp
import uvicorn
from aiogram import Dispatcher, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

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
    log_event,
    set_audit_sink,
    setup_logging,
)
from .utils.telegram_logging import LoggedBot


def create_dispatcher() -> Dispatcher:
    setup_logging()

    bot = LoggedBot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher(bot, storage=MemoryStorage())

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
