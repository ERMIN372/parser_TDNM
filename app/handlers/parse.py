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
        raw_parts = [p.strip() for p in args.split(";") if p.strip()]
        if len(raw_parts) < 2:
            await message.reply("Используй формат: /parse должность; город; pages=1")
            return
        query, city, *rest = raw_parts
        overrides: dict[str, object] = {}
        if rest:
            try:
                overrides = _parse_overrides(rest)
            except ValueError as exc:
                await message.reply(str(exc))
                return
        await _run_parser(message, query, city, overrides)
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
    await _run_parser(message, query, city, {})


def _parse_overrides(parts: list[str]) -> dict[str, object]:
    overrides: dict[str, object] = {}
    for part in parts:
        if "=" not in part:
            raise ValueError("Опции указываются в формате key=value")
        key, value = [p.strip() for p in part.split("=", 1)]
        key = key.lower()
        if key == "pages":
            overrides["pages"] = int(value)
        elif key in {"per_page", "per-page"}:
            overrides["per_page"] = int(value)
        elif key == "pause":
            overrides["pause"] = float(value)
        elif key == "site":
            overrides["site"] = value.lower()
        else:
            raise ValueError(f"Неизвестная опция: {key}")
    return overrides


async def _run_parser(message: types.Message, query: str, city: str, overrides: dict[str, object]):
    await message.answer("Собираю вакансии, это может занять до 1–2 минут…")
    try:
        path = await parser_adapter.run_report(
            message.from_user.id,
            query,
            city,
            role=query,
            **overrides,
        )
    except Exception as e:  # pragma: no cover - log path
        logging.exception("parser failed")
        await message.answer("Не удалось, попробуйте позже.")
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
