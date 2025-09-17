from __future__ import annotations
import logging
import math
import os
from typing import Dict, Tuple, List

from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InputFile,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# анти-спам / занятость пользователя
from ..middlewares.busy import is_busy, set_busy, clear_busy, BUSY_TEXT

from ..services import parser_adapter
from ..services import validator  # валидация запроса

log = logging.getLogger(__name__)

# Кеш последнего «сомнительного» запроса: user_id -> (query, city, overrides)
_WARN_CACHE: Dict[int, Tuple[str, str, dict]] = {}
# Кеш шага выбора объёма: user_id -> (norm_title, city, area_id, overrides, max_total)
_PENDING_QTY: Dict[int, Tuple[str, str, int, dict, int]] = {}

# верхний лимит для «Всё»
MAX_EXPORT = int(os.getenv("MAX_EXPORT", "500"))
BIG_PER_PAGE = 100  # HH допускает до 100


class ParseForm(StatesGroup):
    waiting_query = State()
    waiting_city = State()
    waiting_kw_include = State()
    waiting_kw_exclude = State()


# ---------- utils ----------
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
        elif key == "include":
            overrides["include"] = _split_kw(value)
        elif key == "exclude":
            overrides["exclude"] = _split_kw(value)
        else:
            raise ValueError(f"Неизвестная опция: {key}")
    return overrides


def _split_kw(s: str) -> List[str]:
    return [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]


async def _run_parser_bypass_validation(message: types.Message, query: str, city: str, overrides: dict):
    """Запуск парсера без доп. проверок (по кнопке «Всё равно искать»)."""
    uid = message.from_user.id
    if not set_busy(uid):
        await message.answer(BUSY_TEXT)
        return
    try:
        await message.answer("Окей, запускаю поиск. Это может занять 1–2 минуты…")
        path = await parser_adapter.run_report(
            uid,
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
    finally:
        clear_busy(uid)

    if path.exists():
        await message.answer_document(InputFile(path))
    else:
        await message.answer("Отчёт не найден. Проверьте логи.")


async def _run_with_amount(message: types.Message, title: str, city: str, area_id: int, overrides: dict, total: int):
    """Считает pages/per_page под нужный объём total и запускает парсер с блокировкой пользователя."""
    uid = message.from_user.id
    if not set_busy(uid):
        await message.answer(BUSY_TEXT)
        return

    per_page = max(1, min(100, total))
    pages = max(1, math.ceil(total / per_page))

    ov = dict(overrides or {})
    ov.setdefault("area", area_id)
    ov["per_page"] = per_page
    ov["pages"] = pages

    # ⚡ быстрый режим для больших объёмов
    if total > 200:
        ov.setdefault("site", "hh")
        ov.setdefault("pause", 0.3)
        timeout = int(os.getenv("PARSER_TIMEOUT_LARGE", "1200"))
    else:
        timeout = None

    await message.answer(f"Окей, выгружаю ~{min(total, per_page*pages)} вакансий… это может занять несколько минут.")
    try:
        path = await parser_adapter.run_report(
            uid,
            title,
            city,
            role=title,
            timeout=timeout,
            **ov,
        )
    except Exception as e:
        logging.exception("parser failed")
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        await message.answer(err_text)
        return
    finally:
        clear_busy(uid)

    if path.exists():
        await message.answer_document(InputFile(path))
    else:
        await message.answer("Отчёт не найден. Проверьте логи.")


# ---------- core ----------
async def _run_parser(message: types.Message, query: str, city: str, overrides: dict[str, object]):
    # если пользователь занят — мягко отшьём сразу
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return

    # 1) Мягкая валидация
    ok, norm_title, area_id, bad_msg = validator.validate_request(query, city)
    if not ok:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("✅ Всё равно искать (списать 1 кредит)", callback_data="parse_force"),
            InlineKeyboardButton("✏️ Исправить запрос", callback_data="parse_fix"),
        )
        _WARN_CACHE[message.from_user.id] = (query, city, overrides)
        await message.answer(
            bad_msg
            + "\n\nЕсли ты уверен(а) — могу всё равно запустить поиск. "
              "Это может списать 1 кредит/лимит.",
            reply_markup=kb,
        )
        await ParseForm.waiting_query.set()
        return

    # 2) Если юзер сам задал pages/per_page — запускаем без шага объёма (и блокируем пользователя)
    if "pages" in overrides or "per_page" in overrides:
        uid = message.from_user.id
        if not set_busy(uid):
            await message.answer(BUSY_TEXT)
            return
        try:
            await message.answer("Собираю вакансии, это может занять несколько минут…")
            if "area" not in overrides:
                overrides["area"] = area_id
            path = await parser_adapter.run_report(
                uid,
                norm_title,
                city,
                role=norm_title,
                **overrides,
            )
        except Exception as e:
            logging.exception("parser failed")
            err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
            await message.answer(err_text)
            return
        finally:
            clear_busy(uid)

        if path.exists():
            await message.answer_document(InputFile(path))
        else:
            await message.answer("Отчёт не найден. Проверьте логи.")
        return

    # 3) Шаг выбора объёма (число найденных НЕ показываем)
    ok_probe, found = validator.probe_hh_found(norm_title, area_id)
    max_total = min(found, MAX_EXPORT) if ok_probe and isinstance(found, int) else MAX_EXPORT

    kb = InlineKeyboardMarkup(row_width=3)
    kb.row(
        InlineKeyboardButton("60", callback_data="qty:60"),
        InlineKeyboardButton("200", callback_data="qty:200"),
        InlineKeyboardButton(f"Всё (до {MAX_EXPORT})", callback_data="qty:all"),
    )
    # предпросмотр первых 5 — лёгкий запрос; тоже с блокировкой
    kb.row(InlineKeyboardButton("👀 Превью (5)", callback_data="preview:5"))

    _PENDING_QTY[message.from_user.id] = (norm_title, city, area_id, overrides, max_total)
    await message.answer("Выбери объём выгрузки:", reply_markup=kb)


# ---------- /parse ----------
async def cmd_parse(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return

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
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    await state.update_data(query=message.text.strip())
    await message.answer("Город?")
    await ParseForm.waiting_city.set()


async def process_city(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    data = await state.get_data()
    query = data.get("query")
    city = message.text.strip()
    kb = InlineKeyboardMarkup().row(
        InlineKeyboardButton("➕ Добавить ключевые", callback_data="kw_yes"),
        InlineKeyboardButton("Пропустить", callback_data="kw_no"),
    )
    await state.update_data(city=city)
    await message.answer("Хочешь уточнить поиск ключевыми словами (включить/исключить)?", reply_markup=kb)


# ---------- ключевые слова (include/exclude) ----------
async def cb_kw_yes(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return
    await call.answer()
    await call.message.answer(
        "Введи слова, которые ДОЛЖНЫ встречаться (через запятую). Пример: электроника, b2b, pcb.\n"
        "Если не нужно — пришли пусто или «-».",
        reply_markup=ReplyKeyboardRemove(),
    )
    await ParseForm.waiting_kw_include.set()


async def cb_kw_no(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return
    await call.answer()
    data = await state.get_data()
    query = data.get("query")
    city = data.get("city")
    await state.finish()
    await _run_parser(call.message, query, city, {})  # без уточнений


async def process_kw_include(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    txt = (message.text or "").strip()
    include = [] if txt in {"", "-"} else _split_kw(txt)
    await state.update_data(include=include)
    await message.answer(
        "Теперь слова, которые НУЖНО исключить (через запятую). Пример: стажёр, помощник.\n"
        "Если не нужно — пришли пусто или «-».",
    )
    await ParseForm.waiting_kw_exclude.set()


async def process_kw_exclude(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    txt = (message.text or "").strip()
    exclude = [] if txt in {"", "-"} else _split_kw(txt)
    data = await state.get_data()
    query = data.get("query")
    city = data.get("city")
    include = data.get("include", [])
    await state.finish()
    await _run_parser(message, query, city, {"include": include, "exclude": exclude})


# ---------- callbacks из предупреждения / объём / превью ----------
async def cb_parse_force(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return
    payload = _WARN_CACHE.pop(call.from_user.id, None)
    await call.answer()
    if not payload:
        await call.message.answer("Не нашёл последний запрос. Введи должность ещё раз:")
        await ParseForm.waiting_query.set()
        return
    query, city, overrides = payload
    try:
        await state.finish()
    except Exception:
        pass
    await _run_parser_bypass_validation(call.message, query, city, overrides)


async def cb_parse_fix(call: types.CallbackQuery):
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return
    _WARN_CACHE.pop(call.from_user.id, None)
    await call.answer()
    await call.message.answer("Окей! Введи должность ещё раз:", reply_markup=ReplyKeyboardRemove())
    await ParseForm.waiting_query.set()


async def cb_qty(call: types.CallbackQuery):
    # если занят — просто подсказка и выходим
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return

    payload = _PENDING_QTY.get(call.from_user.id)
    await call.answer()
    if not payload:
        await call.message.answer("Не нашёл предыдущий запрос. Введи должность ещё раз:")
        await ParseForm.waiting_query.set()
        return

    title, city, area_id, overrides, max_total = payload
    choice = call.data.split(":", 1)[1]
    if choice == "60":
        total = 60
    elif choice == "200":
        total = 200
    else:  # "all"
        total = max_total

    # фикс: после старта выгрузки «забываем» pending, чтобы старые кнопки не плодили ошибки
    _PENDING_QTY.pop(call.from_user.id, None)
    await _run_with_amount(call.message, title, city, area_id, overrides, total)


async def cb_preview(call: types.CallbackQuery):
    """Показать 5 первых совпадений без уничтожения кеша запроса."""
    await call.answer("Готовлю превью…", show_alert=False)

    uid = call.from_user.id
    payload = _PENDING_QTY.get(uid)   # ВАЖНО: .get(), НЕ .pop()!
    if not payload:
        await call.message.answer("Не нашёл предыдущий запрос. Введи должность ещё раз:")
        await ParseForm.waiting_query.set()
        return

    title, city, area_id, overrides, _max_total = payload
    include = (overrides or {}).get("include") or []
    exclude = (overrides or {}).get("exclude") or []

    try:
        rows = await parser_adapter.preview_rows(
            title, city, area_id=area_id, include=include, exclude=exclude
        )
    except Exception:
        logging.exception("preview failed")
        await call.message.answer("⏳ Превью не успело загрузиться. Попробуй ещё раз.")
        return

    if not rows:
        await call.message.answer("Совпадений не нашлось по текущим критериям.")
        return

    # аккуратный текст превью
    lines = []
    for r in rows:
        t = r.get("title") or "—"
        c = r.get("company") or "—"
        s = r.get("salary") or "—"
        link = r.get("link")
        if link:
            lines.append(f"• <a href=\"{link}\">{t}</a> — {c} — {s}")
        else:
            lines.append(f"• {t} — {c} — {s}")

    txt = "<b>Предпросмотр (первые совпадения):</b>\n" + "\n".join(lines)
    await call.message.answer(txt, disable_web_page_preview=True)


def register(dp: Dispatcher):
    # команды и диалог
    dp.register_message_handler(cmd_parse, commands=["parse"], state="*")
    dp.register_message_handler(cmd_parse, lambda m: m.text == "🔎 Поиск", state="*")
    dp.register_message_handler(process_query, state=ParseForm.waiting_query)
    dp.register_message_handler(process_city, state=ParseForm.waiting_city)

    # ключевые слова
    dp.register_callback_query_handler(cb_kw_yes, lambda c: c.data == "kw_yes", state="*")
    dp.register_callback_query_handler(cb_kw_no,  lambda c: c.data == "kw_no",  state="*")
    dp.register_message_handler(process_kw_include, state=ParseForm.waiting_kw_include)
    dp.register_message_handler(process_kw_exclude, state=ParseForm.waiting_kw_exclude)

    # подтверждение/исправление сомнительного запроса
    dp.register_callback_query_handler(cb_parse_force, lambda c: c.data == "parse_force", state="*")
    dp.register_callback_query_handler(cb_parse_fix,   lambda c: c.data == "parse_fix",   state="*")

    # выбор объёма и превью
    dp.register_callback_query_handler(cb_qty,     lambda c: c.data and c.data.startswith("qty:"),     state="*")
    dp.register_callback_query_handler(cb_preview, lambda c: c.data and c.data.startswith("preview:"), state="*")
