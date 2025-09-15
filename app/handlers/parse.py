import logging
from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InputFile, ReplyKeyboardRemove

from ..services import parser_adapter


class ParseForm(StatesGroup):
    waiting_query = State()
    waiting_city = State()


async def cmd_parse(message: types.Message, state: FSMContext):
    args = message.get_args()
    if args:
        parts = [p.strip() for p in args.split(";", 1)]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            await message.reply("Используй формат: /parse должность; город")
            return
        await _run_parser(message, parts[0], parts[1])
        return
    await message.answer("Введите должность:", reply_markup=ReplyKeyboardRemove())
    await ParseForm.waiting_query.set()


async def process_query(message: types.Message, state: FSMContext):
    await state.update_data(query=message.text.strip())
    await message.answer("Город?")
    await ParseForm.waiting_city.set()


async def process_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    query = data.get("query")
    city = message.text.strip()
    await state.finish()
    await _run_parser(message, query, city)


async def _run_parser(message: types.Message, query: str, city: str):
    await message.answer("Собираю вакансии, это может занять до 1–2 минут…")
    try:
        path = await parser_adapter.run_report(message.from_user.id, query, city)
    except Exception as e:  # pragma: no cover - log path
        logging.exception("parser failed")
        await message.answer(f"Не удалось получить отчёт: {e}")
        return
    if path.exists():
        await message.answer_document(InputFile(path))
    else:
        await message.answer("Отчёт не найден. Проверьте логи.")


def register(dp: Dispatcher):
    dp.register_message_handler(cmd_parse, commands=["parse"], state="*")
    dp.register_message_handler(cmd_parse, lambda m: m.text == "🔎 Поиск", state="*")
    dp.register_message_handler(process_query, state=ParseForm.waiting_query)
    dp.register_message_handler(process_city, state=ParseForm.waiting_city)
