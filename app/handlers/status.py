import os

import aiogram
from aiogram import Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.storage.repo import free_used_this_month, get_credits, is_unlimited_active
from app.utils.logging import log_event, update_context

FREE_PER_MONTH = int(os.getenv("FREE_PER_MONTH", "3"))

async def cmd_status(message: types.Message):
    update_context(command="/status")
    log_event("request_parsed", message="/status", command="/status")
    uid = message.from_user.id
    free_used = free_used_this_month(uid)
    free_left = max(0, FREE_PER_MONTH - free_used)
    credits = get_credits(uid)
    active, until = is_unlimited_active(uid)

    update_context(
        quota={
            "free_used": free_used,
            "free_limit": FREE_PER_MONTH,
            "credits": credits,
            "unlimited": bool(active),
        }
    )

    lines = [
        "🧭 Статус:",
        f"• Бесплатные в этом месяце: {FREE_PER_MONTH - free_left}/{FREE_PER_MONTH} использовано → осталось {free_left}",
        f"• Платные кредиты: {credits}",
        f"• Безлимит: {'до ' + until.strftime('%Y-%m-%d %H:%M') + ' UTC' if active and until else 'нет'}",
        "",
        f"aiogram: {aiogram.__version__}",
    ]
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("💳 Купить", callback_data="buy:open"))
    await message.reply("\n".join(lines), reply_markup=kb)

def register(dp: Dispatcher):
    dp.register_message_handler(cmd_status, commands=["status"])
    # кнопка в главном меню
    dp.register_message_handler(cmd_status, lambda m: m.text in {"🧭 Статус", "Статус"}, state="*")
