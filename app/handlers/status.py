from aiogram import types, Dispatcher
import aiogram
import os
from app.storage.repo import free_used_this_month, get_credits, is_unlimited_active

FREE_PER_MONTH = int(os.getenv("FREE_PER_MONTH", "3"))

async def cmd_status(message: types.Message):
    uid = message.from_user.id
    free_used = free_used_this_month(uid)
    free_left = max(0, FREE_PER_MONTH - free_used)
    credits = get_credits(uid)
    active, until = is_unlimited_active(uid)

    lines = [
        "🧭 Статус:",
        f"• Бесплатные в этом месяце: {FREE_PER_MONTH - free_left}/{FREE_PER_MONTH} использовано → осталось {free_left}",
        f"• Платные кредиты: {credits}",
        f"• Безлимит: {'до ' + until.strftime('%Y-%m-%d %H:%M') + ' UTC' if active and until else 'нет'}",
        "",
        f"aiogram: {aiogram.__version__}",
    ]
    await message.reply("\n".join(lines))

def register(dp: Dispatcher):
    dp.register_message_handler(cmd_status, commands=["status"])
    # кнопка в главном меню
    dp.register_message_handler(cmd_status, lambda m: m.text in {"🧭 Статус", "Статус"}, state="*")
