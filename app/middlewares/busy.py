from __future__ import annotations

import time
from typing import Dict, Set

from aiogram import types
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware

from app.utils.logging import log_event

# Текст уведомления
BUSY_TEXT = "Идёт выгрузка, подожди пожалуйста…"

# Глобальный реестр «занятых» пользователей
BUSY_USERS: Set[int] = set()

# Чтобы не спамить BUSY_TEXT каждый апдейт — шлём не чаще, чем раз в N секунд
_last_ping: Dict[int, float] = {}
PING_EVERY = 8.0  # сек

# ——— API для хендлеров ———
def is_busy(uid: int) -> bool:
    return uid in BUSY_USERS

def set_busy(uid: int) -> bool:
    """Вернёт False, если уже занят; True — если заняли впервые."""
    if uid in BUSY_USERS:
        return False
    BUSY_USERS.add(uid)
    return True

def clear_busy(uid: int) -> None:
    BUSY_USERS.discard(uid)

# ——— Middleware ———
class BusyMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: types.Message, data: dict):
        uid = getattr(message.from_user, "id", None)
        if uid is None:
            return
        if is_busy(uid):
            now = time.time()
            if now - _last_ping.get(uid, 0.0) >= PING_EVERY:
                _last_ping[uid] = now
                await message.answer(BUSY_TEXT)
                log_event(
                    "busy_reject",
                    message="User is busy (message)",
                    ok=False,
                )
            raise CancelHandler()

    async def on_pre_process_callback_query(self, call: types.CallbackQuery, data: dict):
        uid = getattr(call.from_user, "id", None)
        if uid is None:
            return
        if is_busy(uid):
            # короткий ответ без алерта, чтобы не мешать UX
            dedup_payload = {"callback_data": call.data, "user_id": uid}
            try:
                await call.answer(BUSY_TEXT, show_alert=False)
                log_event(
                    "callback_ack_sent",
                    callback="busy_middleware",
                    status="ok",
                    latency_ms=0,
                )
            except Exception as exc:  # pragma: no cover - best effort logging
                log_event(
                    "callback_ack_failed",
                    callback="busy_middleware",
                    err=str(exc),
                )
            log_event("job.deduped", **dedup_payload)
            log_event(
                "busy_reject",
                message="User is busy (callback)",
                ok=False,
            )
            raise CancelHandler()
