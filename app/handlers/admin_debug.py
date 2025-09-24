from __future__ import annotations

import os
import time

from aiogram import Dispatcher, types

from app.config import settings
from app.utils.admins import is_admin


def _is_admin(message: types.Message) -> bool:
    user_id = getattr(message.from_user, "id", None)
    return bool(user_id and is_admin(user_id))


async def cmd_ping(message: types.Message) -> None:
    if not _is_admin(message):
        return

    start = time.perf_counter()
    reply = await message.reply("🏓 ping…")
    await reply.edit_text("pong")
    elapsed_ms = (time.perf_counter() - start) * 1000
    await reply.edit_text(f"pong ({elapsed_ms:.1f} ms)")


async def cmd_whoami(message: types.Message) -> None:
    if not _is_admin(message):
        return

    user = message.from_user
    chat = message.chat
    text = (
        "<b>whoami</b>\n"
        f"user_id: <code>{user.id}</code>\n"
        f"username: @{user.username or '-'}\n"
        f"name: {user.full_name or '-'}\n"
        f"chat_id: <code>{chat.id}</code>"
    )
    await message.reply(text, parse_mode="HTML")


async def cmd_runtime(message: types.Message) -> None:
    if not _is_admin(message):
        return

    port_env = os.getenv("PORT")
    port = port_env or str(settings.WEBAPP_PORT)
    text = (
        "<b>runtime</b>\n"
        f"MODE: <code>{settings.MODE}</code>\n"
        f"PORT: <code>{port}</code>\n"
        f"WEBHOOK_URL: {settings.WEBHOOK_URL or '-'}"
    )
    await message.reply(text, parse_mode="HTML")


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")
    dp.register_message_handler(cmd_whoami, commands=["whoami"], state="*")
    dp.register_message_handler(cmd_runtime, commands=["rt"], state="*")
