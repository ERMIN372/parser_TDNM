from __future__ import annotations
import logging
from typing import Dict, Tuple

from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InputFile,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from ..services import parser_adapter
from ..services import validator  # валидация запроса

log = logging.getLogger(__name__)

# Кеш последнего «сомнительного» запроса на подтверждение: user_id -> (query, city, overrides)
_WARN_CACHE: Dict[int, Tuple[str, str, dict]] = {}


class ParseForm(StatesGroup):
    waiting_query = State()
    waiting_city = State()


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
        elif key == "area":
            overrides["area"] = int(value)
        else:
            raise ValueError(f"Неизвестная опция: {key}")
    return overrides


async def _run_parser_bypass_validation(message: types.Message, query: str, city: str, overrides: dict):
    """Запуск парсера без доп. проверок (по нажатию «Всё равно искать»)."""
    await message.answer("Окей, запускаю поиск. Это может занять 1–2 минуты…")
    try:
        path = await parser_adapter.run_report(
            message.from_user.id,
            query,
            city,
            role=query,
            **overrides,
        )
    except Exception as e:  # pragma: no cover
        logging.exception("parser failed")
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        await message.answer(err_text)
        return

    if path.exists():
        await message.answer_document(InputFile(path))
    else:
        await message.answer("Отчёт не найден. Проверьте логи.")


async def _run_parser(message: types.Message, query: str, city: str, overrides: dict[str, object]):
    # 1) Валидация: если что-то не так — предупреждаем и даём выбор
    ok, norm_title, area_id, bad_msg = validator.validate_request(query, city)
    if not ok:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("✅ Всё равно искать (списать 1 кредит)", callback_data="parse_force"),
            InlineKeyboardButton("✏️ Исправить запрос", callback_data="parse_fix"),
        )
        # Сохраняем «сомнительный» запрос пользователя до его клика
        _WARN_CACHE[message.from_user.id] = (query, city, overrides)
        await message.answer(
            bad_msg
            + "\n\nЕсли ты уверен(а) — могу всё равно запустить поиск. "
              "Это может списать 1 кредит/лимит.",
            reply_markup=kb,
        )
        # возвращаем к вводу должности, если выберет «Исправить»
        await ParseForm.waiting_query.set()
        return

    await message.answer("Собираю вакансии, это может занять до 1–2 минут…")
    try:
        if "area" not in overrides:
            overrides["area"] = area_id

        path = await parser_adapter.run_report(
            message.from_user.id,
            norm_title,
            city,
            role=norm_title,
            **overrides,
        )
    except Exception as e:  # pragma: no cover
        logging.exception("parser failed")
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        await message.answer(err_text)
        return

    if path.exists():
        await message.answer_document(InputFile(path))
    else:
        await message.answer("Отчёт не найден. Проверьте логи.")


# ---------- /parse ----------
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


# ---------- callbacks из предупреждения ----------
async def cb_parse_force(call: types.CallbackQuery, state: FSMContext):
    """Пользователь подтвердил запуск несмотря на предупреждение."""
    payload = _WARN_CACHE.pop(call.from_user.id, None)
    await call.answer()
    if not payload:
        await call.message.answer("Не нашёл последний запрос. Введи должность ещё раз:")
        await ParseForm.waiting_query.set()
        return
    query, city, overrides = payload
    # сбрасываем возможное состояние вопрос/город
    try:
        await state.finish()
    except Exception:
        pass
    await _run_parser_bypass_validation(call.message, query, city, overrides)


async def cb_parse_fix(call: types.CallbackQuery):
    """Пользователь выбрал «исправить запрос» — просто просим ввести заново должность."""
    _WARN_CACHE.pop(call.from_user.id, None)
    await call.answer()
    await call.message.answer("Окей! Введи должность ещё раз:", reply_markup=ReplyKeyboardRemove())
    await ParseForm.waiting_query.set()


def register(dp: Dispatcher):
    # команды и диалог
    dp.register_message_handler(cmd_parse, commands=["parse"], state="*")
    dp.register_message_handler(cmd_parse, lambda m: m.text == "🔎 Поиск", state="*")
    dp.register_message_handler(process_query, state=ParseForm.waiting_query)
    dp.register_message_handler(process_city, state=ParseForm.waiting_city)

    # подтверждение/отмена из предупреждения
    dp.register_callback_query_handler(cb_parse_force, lambda c: c.data == "parse_force", state="*")
    dp.register_callback_query_handler(cb_parse_fix,   lambda c: c.data == "parse_fix",   state="*")
