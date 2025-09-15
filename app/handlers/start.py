from aiogram import types, Dispatcher

from ..keyboards import main_kb


async def cmd_start(message: types.Message):
    text = (
        "Привет! Используй команду /parse для сбора вакансий.\n"
        "Короткая форма: /parse кассир; Москва"
    )
    await message.answer(text, reply_markup=main_kb)


def register(dp: Dispatcher):
    dp.register_message_handler(cmd_start, commands=["start", "help"])
