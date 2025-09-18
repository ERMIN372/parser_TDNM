from __future__ import annotations
import logging
import asyncio
import math
import os
from typing import Dict, List, Tuple

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    ReplyKeyboardRemove,
)

# анти-спам / занятость пользователя
from ..middlewares.busy import BUSY_TEXT, clear_busy, is_busy, set_busy

from ..services import parser_adapter
from ..services import validator  # валидация запроса
from ..services.mini_analytics import register_context, render_mini_analytics
from ..services.quota import FREE_PER_MONTH, check_and_consume
from app import keyboards
from app.utils.admins import is_admin
from app.utils.logging import complete_operation, log_event, update_context

# Кеш последнего «сомнительного» запроса: user_id -> (query, city, overrides)
_WARN_CACHE: Dict[int, Tuple[str, str, dict]] = {}
# Кеш шага выбора объёма: user_id -> (norm_title, city, area_id, overrides, max_total)
_PENDING_QTY: Dict[int, Tuple[str, str, int, dict, int]] = {}


async def _ensure_quota(
    message: types.Message,
    uid: int,
    *,
    user: types.User | None = None,
) -> bool:
    """Проверяет и списывает лимит перед запуском выгрузки."""

    person = user or getattr(message, "from_user", None)
    username = getattr(person, "username", None) if person else None
    full_name = getattr(person, "full_name", None) if person else None

    decision = check_and_consume(uid, username, full_name)
    quota_info = {
        "free_limit": FREE_PER_MONTH,
        "free_used": max(0, FREE_PER_MONTH - decision.free_left),
        "credits": decision.credits,
        "unlimited": decision.mode == "unlimited",
    }
    credits_delta = -1 if decision.mode == "paid" else 0
    update_context(quota=quota_info, credits_delta=credits_delta)
    if not decision.allowed:
        await message.answer(
            decision.message or "Лимит исчерпан — попробуйте позже 🙏",
            reply_markup=_main_menu_kb(message, user=user),
        )
        complete_operation(ok=False, err="quota_exceeded")
        return False

    if decision.mode == "paid":
        await message.answer(f"💳 Списан 1 кредит. Осталось: {decision.credits}")
    elif decision.mode == "free" and decision.free_left == 0:
        await message.answer(
            "Бесплатные запросы в этом месяце закончились — дальше будут списываться кредиты."
        )

    return True

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


def _ensure_str_list(values) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        val = values.strip()
        return [val] if val else []
    result = []
    for item in values:
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _build_args(
    title: str | None,
    city: str | None,
    overrides: dict | None = None,
    *,
    qty: int | None = None,
) -> dict[str, object]:
    args: dict[str, object] = {}
    if title:
        args["title"] = title
    if city:
        args["city"] = city
    if qty is not None:
        args["qty"] = qty
    overrides = overrides or {}
    for key in ("include", "exclude"):
        if key in overrides:
            args[key] = _ensure_str_list(overrides[key])
    for key in ("pages", "per_page", "pause", "site", "area"):
        if key in overrides:
            args[key] = overrides[key]
    return args


def _dialog_step(step: str, value: str | None = None) -> dict[str, str]:
    preview = (value or "").strip()
    if len(preview) > 60:
        preview = preview[:57] + "…"
    return {"step": step, "value": preview}


def _resolve_requester_id(message: types.Message, uid: int | None = None) -> int:
    if uid is not None:
        return uid
    if getattr(message, "chat", None) is not None:
        return message.chat.id
    if getattr(message, "from_user", None) is not None:
        return message.from_user.id
    raise ValueError("Cannot determine requester id")


def _main_menu_kb(message: types.Message, *, user: types.User | None = None):
    person = user or getattr(message, "from_user", None)
    user_id = getattr(person, "id", None)
    return keyboards.main_kb(is_admin=is_admin(user_id))


def _log_parse_start(title: str, city: str, overrides: dict | None = None, *, approx_total: int | None = None) -> None:
    args = _build_args(title, city, overrides, qty=approx_total)
    update_context(args=args)
    details = [f"title='{title}'", f"city='{city}'"]
    if approx_total is not None:
        details.append(f"qty={approx_total}")
    if overrides and overrides.get("site"):
        details.append(f"site={overrides['site']}")
    log_event("parse_start", message="parse_start " + " ".join(details), args=args)


def _log_parse_ready(title: str, city: str, overrides: dict | None = None, *, approx_total: int | None = None) -> None:
    args = _build_args(title, city, overrides, qty=approx_total)
    update_context(args=args)
    log_event("parse_ready", message=f"parse_ready title='{title}' city='{city}'", args=args)


def _log_preview_start(title: str, city: str, overrides: dict | None = None) -> None:
    args = _build_args(title, city, overrides)
    log_event("preview_start", message=f"preview_start title='{title}' city='{city}'", args=args)


def _log_preview_ready(title: str, city: str, rows: int, overrides: dict | None = None) -> None:
    args = _build_args(title, city, overrides)
    log_event(
        "preview_ready",
        message=f"preview_ready rows={rows} title='{title}' city='{city}'",
        args=args,
    )


def _log_preview_timeout(title: str, city: str, overrides: dict | None = None, err: str | None = None) -> None:
    args = _build_args(title, city, overrides)
    log_event(
        "preview_timeout",
        level="WARN",
        message=f"preview_timeout title='{title}' city='{city}'",
        args=args,
        err=err,
    )


async def _send_report_with_analytics(
    message: types.Message,
    path,
    *,
    title: str,
    city: str,
    approx_total: int | None = None,
    include=None,
    exclude=None,
    reply_markup=None,
) -> None:
    register_context(path, title=title, city=city)
    await message.answer_document(InputFile(path), reply_markup=reply_markup)
    text = render_mini_analytics(
        path,
        approx_total=approx_total,
        include=_ensure_str_list(include),
        exclude=_ensure_str_list(exclude),
    )
    if text:
        await message.answer(text, disable_web_page_preview=True)


async def _run_parser_bypass_validation(
    message: types.Message,
    query: str,
    city: str,
    overrides: dict,
    *,
    uid: int | None = None,
    user: types.User | None = None,
):
    """Запуск парсера без доп. проверок (по кнопке «Всё равно искать»)."""
    uid = _resolve_requester_id(message, uid)
    if not set_busy(uid):
        await message.answer(BUSY_TEXT)
        return
    try:
        if not await _ensure_quota(message, uid, user=user):
            return
        await message.answer("Окей, запускаю поиск. Это может занять 1–2 минуты…")
        _log_parse_start(query, city, overrides)
        path = await parser_adapter.run_report(
            uid,
            query,
            city,
            role=query,
            **overrides,
        )
    except Exception as e:  # pragma: no cover
        event = "parse_timeout" if isinstance(e, asyncio.TimeoutError) else "parse_error"
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        log_event(event, level="ERROR", err=err_text)
        await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
        complete_operation(ok=False, err=err_text)
        return
    finally:
        clear_busy(uid)

    if path.exists():
        _log_parse_ready(query, city, overrides)
        await _send_report_with_analytics(
            message,
            path,
            title=query,
            city=city,
            include=overrides.get("include"),
            exclude=overrides.get("exclude"),
            reply_markup=_main_menu_kb(message, user=user),
        )
    else:
        await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
        log_event("parse_error", level="ERROR", err="report_missing")
        complete_operation(ok=False, err="report_missing")


async def _run_with_amount(
    message: types.Message,
    title: str,
    city: str,
    area_id: int,
    overrides: dict,
    total: int,
    *,
    uid: int | None = None,
    user: types.User | None = None,
):
    """Считает pages/per_page под нужный объём total и запускает парсер с блокировкой пользователя."""
    uid = _resolve_requester_id(message, uid)
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

    if not await _ensure_quota(message, uid, user=user):
        return

    await message.answer(
        f"Окей, выгружаю ~{min(total, per_page*pages)} вакансий… это может занять несколько минут."
    )
    _log_parse_start(title, city, ov, approx_total=total)
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
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        event = "parse_timeout" if isinstance(e, asyncio.TimeoutError) else "parse_error"
        log_event(event, level="ERROR", err=err_text)
        await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
        complete_operation(ok=False, err=err_text)
        return
    finally:
        clear_busy(uid)

    if path.exists():
        _log_parse_ready(title, city, ov, approx_total=total)
        await _send_report_with_analytics(
            message,
            path,
            title=title,
            city=city,
            approx_total=total,
            include=ov.get("include"),
            exclude=ov.get("exclude"),
            reply_markup=_main_menu_kb(message, user=user),
        )
    else:
        await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
        log_event("parse_error", level="ERROR", err="report_missing")
        complete_operation(ok=False, err="report_missing")


# ---------- core ----------
async def _run_parser(
    message: types.Message,
    query: str,
    city: str,
    overrides: dict[str, object],
    *,
    uid: int | None = None,
    user: types.User | None = None,
):
    args_payload = _build_args(query, city, overrides)
    update_context(command="/parse", args=args_payload)
    log_event(
        "request_parsed",
        message=f"/parse {query}; {city}",
        command="/parse",
        args=args_payload,
    )

    # если пользователь занят — мягко отшьём сразу
    requester_id = _resolve_requester_id(message, uid)
    if is_busy(requester_id):
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
        _WARN_CACHE[requester_id] = (query, city, overrides)
        log_event("validation_warning", level="WARN", message=bad_msg, args=args_payload)
        await message.answer(
            bad_msg
            + "\n\nЕсли ты уверен(а) — могу всё равно запустить поиск. "
              "Это может списать 1 кредит/лимит.",
            reply_markup=kb,
        )
        await ParseForm.waiting_query.set()
        complete_operation(ok=False, err="validation_warning")
        return

    # 2) Если юзер сам задал pages/per_page — запускаем без шага объёма (и блокируем пользователя)
    if "pages" in overrides or "per_page" in overrides:
        if not set_busy(requester_id):
            await message.answer(BUSY_TEXT)
            return
        try:
            if not await _ensure_quota(message, requester_id, user=user):
                return
            await message.answer("Собираю вакансии, это может занять несколько минут…")
            if "area" not in overrides:
                overrides["area"] = area_id
            _log_parse_start(norm_title, city, overrides)
            path = await parser_adapter.run_report(
                requester_id,
                norm_title,
                city,
                role=norm_title,
                **overrides,
            )
        except Exception as e:
            err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
            event = "parse_timeout" if isinstance(e, asyncio.TimeoutError) else "parse_error"
            log_event(event, level="ERROR", err=err_text)
            await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
            complete_operation(ok=False, err=err_text)
            return
        finally:
            clear_busy(requester_id)

        if path.exists():
            _log_parse_ready(norm_title, city, overrides)
            await _send_report_with_analytics(
                message,
                path,
                title=norm_title,
                city=city,
                include=overrides.get("include"),
                exclude=overrides.get("exclude"),
                reply_markup=_main_menu_kb(message, user=user),
            )
        else:
            await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
            log_event("parse_error", level="ERROR", err="report_missing")
            complete_operation(ok=False, err="report_missing")
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

    _PENDING_QTY[requester_id] = (norm_title, city, area_id, overrides, max_total)
    update_context(dialog_step=_dialog_step("choose_qty", str(max_total)))
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
            log_event(
                "validation_warning",
                level="WARN",
                message="parse command missing city",
                command="/parse",
            )
            await message.reply("Используй формат: /parse должность; город; pages=1")
            complete_operation(ok=False, err="invalid_arguments")
            return
        query, city, *rest = raw_parts
        overrides: dict[str, object] = {}
        if rest:
            try:
                overrides = _parse_overrides(rest)
            except ValueError as exc:
                await message.reply(str(exc))
                log_event(
                    "validation_warning",
                    level="WARN",
                    message=str(exc),
                    command="/parse",
                )
                complete_operation(ok=False, err=str(exc))
                return
        await _run_parser(message, query, city, overrides)
        return

    update_context(command="parse_dialog")
    log_event("request_parsed", message="parse dialog start", command="parse_dialog")
    await message.answer("Введите должность:", reply_markup=ReplyKeyboardRemove())
    await ParseForm.waiting_query.set()


async def process_query(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    text = (message.text or "").strip()
    update_context(dialog_step=_dialog_step("query", text))
    await state.update_data(query=text)
    await message.answer("Город?")
    await ParseForm.waiting_city.set()


async def process_city(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    data = await state.get_data()
    query = data.get("query")
    city = (message.text or "").strip()
    update_context(dialog_step=_dialog_step("city", city))
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
    update_context(dialog_step=_dialog_step("kw_prompt", "include"))
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
    update_context(dialog_step=_dialog_step("kw_skip", ""))
    data = await state.get_data()
    query = data.get("query")
    city = data.get("city")
    await state.finish()
    await _run_parser(
        call.message,
        query,
        city,
        {},
        uid=call.from_user.id,
        user=call.from_user,
    )  # без уточнений


async def process_kw_include(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    txt = (message.text or "").strip()
    include = [] if txt in {"", "-"} else _split_kw(txt)
    update_context(dialog_step=_dialog_step("kw_include", ", ".join(include)))
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
    update_context(dialog_step=_dialog_step("kw_exclude", ", ".join(exclude)))
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
    update_context(dialog_step=_dialog_step("force_parse", f"{query}; {city}"))
    try:
        await state.finish()
    except Exception:
        pass
    await _run_parser_bypass_validation(
        call.message,
        query,
        city,
        overrides,
        uid=call.from_user.id,
        user=call.from_user,
    )


async def cb_parse_fix(call: types.CallbackQuery):
    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return
    _WARN_CACHE.pop(call.from_user.id, None)
    await call.answer()
    update_context(dialog_step=_dialog_step("fix_query", ""))
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
    update_context(dialog_step=_dialog_step("qty", str(total)))
    await _run_with_amount(
        call.message,
        title,
        city,
        area_id,
        overrides,
        total,
        uid=call.from_user.id,
        user=call.from_user,
    )


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

    _log_preview_start(title, city, overrides)
    try:
        rows = await parser_adapter.preview_rows(
            uid,
            title,
            city,
            area=area_id,
            include=include,
            exclude=exclude,
        )
    except asyncio.TimeoutError as exc:
        _log_preview_timeout(title, city, overrides, err=str(exc))
        await call.message.answer("⏳ Превью не успело загрузиться. Попробуй ещё раз.")
        return
    except Exception as exc:
        _log_preview_timeout(title, city, overrides, err=str(exc))
        await call.message.answer("⏳ Превью не успело загрузиться. Попробуй ещё раз.")
        return

    if not rows:
        await call.message.answer("Совпадений не нашлось по текущим критериям.")
        _log_preview_ready(title, city, 0, overrides)
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
    _log_preview_ready(title, city, len(rows), overrides)


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
