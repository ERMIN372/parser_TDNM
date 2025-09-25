from __future__ import annotations

import os
import platform
import shutil
import sys
import time
from pathlib import Path

import aiohttp
from aiogram import Dispatcher, types
from aiogram.types import InputFile

from app.config import settings
from app.services.diagnostics import get_last_bundle
from app.utils.admins import is_admin
from app.utils.logging import log_event


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


async def cmd_last_error(message: types.Message) -> None:
    if not _is_admin(message):
        return

    bundle = get_last_bundle(getattr(message.chat, "id", None))
    if not bundle or not bundle.exists():
        await message.reply("Нет сохранённых диагностических бандлов для этого чата.")
        return

    stem = bundle.stem
    correlation = stem.split("bundle_")[-1] if "bundle_" in stem else stem
    await message.reply_document(InputFile(bundle), caption=f"Последний бандл #{correlation}")
    log_event("diagnostic_bundle_sent", action="last_error_command", correlation_id=correlation)


async def cmd_diag(message: types.Message) -> None:
    if not _is_admin(message):
        return

    pipeline_path = Path(os.getenv("PARSER_PIPELINE", "vendor/parser_tdnm/run_pipeline.py"))
    pipeline_exists = pipeline_path.exists()
    pipeline_exec = os.access(pipeline_path, os.X_OK)

    usage = shutil.disk_usage(Path("."))
    total_gb = usage.total / (1024**3)
    free_gb = usage.free / (1024**3)

    env_lines = [
        f"MODE={os.getenv('MODE', '-')}",
        f"WEBHOOK_URL={os.getenv('WEBHOOK_URL', '-')}",
        f"RETURN_URL_BASE={os.getenv('RETURN_URL_BASE', '-')}",
        f"PARSER_TIMEOUT={os.getenv('PARSER_TIMEOUT', '-')}",
        f"PARSER_TIMEOUT_LARGE={os.getenv('PARSER_TIMEOUT_LARGE', '-')}",
    ]

    hh_head = "not tested"
    hh_get = "not tested"
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.head("https://hh.ru", allow_redirects=True) as resp:
                    hh_head = f"{resp.status}"
            except Exception as exc:  # pragma: no cover - network best effort
                hh_head = f"error: {exc}"
            try:
                async with session.get("https://hh.ru", allow_redirects=True) as resp:
                    hh_get = f"{resp.status}"
            except Exception as exc:  # pragma: no cover - network best effort
                hh_get = f"error: {exc}"
    except Exception as exc:  # pragma: no cover - network best effort
        hh_head = hh_head or f"session error: {exc}"
        hh_get = hh_get or f"session error: {exc}"

    issues: list[str] = []
    if not pipeline_exists:
        issues.append("pipeline_missing")
    if not pipeline_exec:
        issues.append("pipeline_not_executable")
    if not hh_head.startswith("2"):
        issues.append("hh_head")
    if not hh_get.startswith("2"):
        issues.append("hh_get")

    status = "OK" if not issues else "ISSUES"

    lines = [
        "<b>diag</b>",
        f"python: <code>{sys.version.split()[0]}</code> ({platform.platform()})",
        f"pipeline: {pipeline_path} (exists={pipeline_exists}, executable={pipeline_exec})",
        f"disk: free={free_gb:.2f}GB / total={total_gb:.2f}GB",
        "env:",
        *(f" • {line}" for line in env_lines),
        "network hh.ru:",
        f" • HEAD → {hh_head}",
        f" • GET → {hh_get}",
        "",
        f"status: <b>{status}</b>",
    ]
    if issues:
        lines.append(f"issues: {', '.join(issues)}")

    log_event(
        "diag_report",
        issues=issues,
        pipeline_exists=pipeline_exists,
        pipeline_exec=pipeline_exec,
        hh_head=hh_head,
        hh_get=hh_get,
    )

    await message.reply("\n".join(lines), parse_mode="HTML")


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")
    dp.register_message_handler(cmd_whoami, commands=["whoami"], state="*")
    dp.register_message_handler(cmd_runtime, commands=["rt"], state="*")
    dp.register_message_handler(cmd_last_error, commands=["last_error"], state="*")
    dp.register_message_handler(cmd_diag, commands=["diag"], state="*")
