from __future__ import annotations

import uuid
from typing import Dict, Tuple

from aiogram import types
from aiogram.dispatcher.middlewares import BaseMiddleware

from app.utils import admins
from app.utils.logging import (
    complete_operation,
    get_operation_context,
    log_event,
    reset_operation_context,
    start_operation,
)


def _extract_user(update: types.Update) -> Tuple[types.User | None, int | None]:
    if update.message:
        return update.message.from_user, update.message.chat.id
    if update.edited_message:
        return update.edited_message.from_user, update.edited_message.chat.id
    if update.callback_query:
        user = update.callback_query.from_user
        chat_id = update.callback_query.message.chat.id if update.callback_query.message else None
        return user, chat_id
    if update.shipping_query:
        return update.shipping_query.from_user, None
    if update.pre_checkout_query:
        return update.pre_checkout_query.from_user, None
    if update.message is None and update.inline_query:
        return update.inline_query.from_user, None
    return None, None


def _detect_update_type(update: types.Update) -> str:
    if update.message:
        if update.message.text and update.message.text.startswith("/"):
            return "command"
        return "message"
    if update.callback_query:
        return "callback"
    if update.shipping_query:
        return "shipping_query"
    if update.pre_checkout_query:
        return "pre_checkout_query"
    if update.inline_query:
        return "inline_query"
    if update.chosen_inline_result:
        return "chosen_inline_result"
    if update.poll:
        return "poll"
    if update.poll_answer:
        return "poll_answer"
    return "update"


def _extract_user_message(update: types.Update) -> Tuple[str | None, str | None]:
    if update.message:
        text = update.message.text or update.message.caption
        return text, text
    if update.callback_query:
        return update.callback_query.data, update.callback_query.data
    if update.shipping_query:
        return update.shipping_query.invoice_payload, update.shipping_query.invoice_payload
    if update.pre_checkout_query:
        return update.pre_checkout_query.invoice_payload, update.pre_checkout_query.invoice_payload
    if update.inline_query:
        return update.inline_query.query, update.inline_query.query
    return None, None


class OperationLoggerMiddleware(BaseMiddleware):
    async def on_pre_process_update(self, update: types.Update, data: Dict) -> None:
        user, chat_id = _extract_user(update)
        correlation = str(uuid.uuid4())
        ctx = start_operation(correlation_id=correlation)

        username = getattr(user, "username", None)
        full_name = getattr(user, "full_name", None)
        user_id = getattr(user, "id", None)

        update_type = _detect_update_type(update)
        raw_text, preview = _extract_user_message(update)

        ctx.user_id = user_id
        ctx.chat_id = chat_id
        ctx.username = username
        ctx.full_name = full_name
        ctx.is_admin = admins.is_admin(user_id) if user_id else False
        ctx.update_type = update_type
        ctx.user_message_raw = raw_text

        data["correlation_id"] = correlation
        data["operation_started_at"] = ctx.started_at

        log_event(
            "update_received",
            message=f"{update_type} received",
            correlation_id=correlation,
            user_message_raw=raw_text,
            user_message_preview=preview,
        )

    async def on_post_process_update(self, update: types.Update, result, data: Dict) -> None:  # noqa: ANN001
        ctx = get_operation_context()
        if ctx and ctx.ok is None:
            complete_operation(ok=True)
        reset_operation_context()
