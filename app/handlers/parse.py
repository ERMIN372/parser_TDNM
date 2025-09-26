from __future__ import annotations
import logging
import asyncio
import math
import os
from pathlib import Path
from typing import Dict, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    ReplyKeyboardRemove,
)
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified

# анти-спам / занятость пользователя
from ..middlewares.busy import BUSY_TEXT, clear_busy, is_busy, set_busy

from ..services import parser_adapter
from ..services import referrals
from ..services import validator  # валидация запроса
from ..services import chips
try:
    from ..services import mini_analytics as _ma
    get_summary = getattr(_ma, "get_summary", lambda *a, **k: None)
    register_context = getattr(_ma, "register_context", lambda *a, **k: None)
    render_mini_analytics = getattr(_ma, "render_mini_analytics", lambda *a, **k: None)
except Exception:
    def get_summary(*a, **k):
        return None

    def register_context(*a, **k):
        return None

    def render_mini_analytics(*a, **k):
        return None
from ..services import report_share
from ..services import paywall
from ..services.quota import FREE_PER_MONTH, QuotaDecision, check_quota, commit_usage
from app import keyboards
from app.utils.admins import is_admin
from app.utils.callbacks import safe_answer
from app.utils.logging import complete_operation, log_event, update_context
from app.utils.progress import ProgressReporter, create_preview_progress, create_report_progress
from app.utils.normalize import normalize_city, normalize_role

log = logging.getLogger(__name__)

# Кеш последнего «сомнительного» запроса: user_id -> (query, city, overrides)
_WARN_CACHE: Dict[int, Tuple[str, str, dict]] = {}
# Кеш шага выбора объёма: user_id -> (norm_title, city, area_id, overrides, max_total)
_PENDING_QTY: Dict[int, Tuple[str, str, int, dict, int]] = {}

DONE_TEXT = "Готово ✅"
FAIL_TEXT = "❌ Не получилось (ошибка/таймаут). Попробуй позже."


async def _start_report_progress(
    message: types.Message,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> ProgressReporter:
    return create_report_progress(message.bot, message.chat.id)


async def _close_progress_success(
    tracker: ProgressReporter | None,
    *,
    delete_after: float | None = 45.0,
) -> None:
    if tracker:
        await tracker.close(True, message=DONE_TEXT, delete_after=delete_after)


async def _close_progress_fail(
    tracker: ProgressReporter | None,
    text: str = FAIL_TEXT,
) -> None:
    if tracker:
        await tracker.close(False, message=text)


def _is_timeout_error(exc: BaseException) -> bool:
    return isinstance(exc, asyncio.TimeoutError) or isinstance(
        getattr(exc, "__cause__", None),
        asyncio.TimeoutError,
    )


async def _ensure_quota(
    message: types.Message,
    uid: int,
    *,
    user: types.User | None = None,
    snapshot: paywall.SavedRequest | None = None,
    reason: str = "parse",
) -> QuotaDecision | None:
    """Проверяет лимиты перед запуском тяжёлой операции."""

    person = user or getattr(message, "from_user", None)
    username = getattr(person, "username", None) if person else None
    full_name = getattr(person, "full_name", None) if person else None

    decision = check_quota(uid, username, full_name)
    quota_info: dict[str, object] = {
        "free_limit": FREE_PER_MONTH,
        "free_used": decision.free_used,
        "free_left": decision.free_left,
        "credits": decision.credits,
        "mode": decision.mode,
    }
    if decision.unlimited_until:
        quota_info["unlimited_until"] = decision.unlimited_until.isoformat()
        quota_info["unlimited"] = True
    update_context(quota=quota_info)

    if decision.allowed:
        return decision

    if snapshot:
        paywall.save_request(uid, snapshot)

    log_event(
        "limit_reached_shown",
        level="INFO",
        message="quota limit reached",
        quota=quota_info,
        args={"reason": reason, "snapshot": snapshot.to_log() if snapshot else None},
    )

    await message.answer(paywall.paywall_text(), reply_markup=paywall.paywall_keyboard())
    complete_operation(ok=False, err="quota_exceeded")
    return None


async def _finalize_quota_usage(
    message: types.Message,
    uid: int,
    decision: QuotaDecision,
) -> None:
    outcome = commit_usage(uid, decision)
    if outcome is None:
        return

    quota_info: dict[str, object] = {
        "free_limit": FREE_PER_MONTH,
        "free_used": outcome.free_used,
        "free_left": outcome.free_left,
        "credits": outcome.credits,
        "mode": outcome.mode,
    }
    if outcome.unlimited_until:
        quota_info["unlimited_until"] = outcome.unlimited_until.isoformat()
        quota_info["unlimited"] = True

    credits_delta = outcome.credits_delta if outcome.credits_delta else None
    update_context(quota=quota_info, credits_delta=credits_delta)

    if outcome.mode == "paid":
        if outcome.credits_delta:
            await message.answer(f"💳 Списан 1 кредит. Осталось: {outcome.credits}")
        else:
            log_event(
                "quota_consume_warning",
                level="WARN",
                message="expected to consume paid credit but balance unchanged",
                quota=quota_info,
            )
    elif outcome.mode == "free" and outcome.free_left == 0:
        await message.answer(
            "Бесплатные запросы в этом месяце закончились — дальше будут списываться кредиты."
        )

# верхний лимит для «Всё»
MAX_EXPORT = int(os.getenv("MAX_EXPORT", "500"))
BIG_PER_PAGE = 100  # HH допускает до 100
ALLOW_FREE_PREVIEW = os.getenv("ALLOW_FREE_PREVIEW", "true").strip().lower() in {"1", "true", "yes", "on"}


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


def _report_actions_keyboard(*, allow_share: bool = True) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if allow_share:
        kb.add(InlineKeyboardButton("📬 Поделиться отчётом", callback_data="report_share"))
    kb.add(InlineKeyboardButton("🔁 Ещё раз", callback_data="report_again"))
    kb.add(InlineKeyboardButton("🏠 Меню", callback_data="report_menu"))
    return kb


def _keywords_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("➕ Добавить ключевые", callback_data="kw_yes"),
        InlineKeyboardButton("Пропустить", callback_data="kw_no"),
    )
    return kb


async def _handle_role_value(
    message: types.Message,
    state: FSMContext,
    raw_value: str,
    *,
    user_id: int,
) -> str:
    chips.finish_session(user_id, "role")
    role = normalize_role(raw_value)
    update_context(dialog_step=_dialog_step("query", role))
    await state.update_data(query=role)
    prompt = await message.answer("Город?")
    await state.set_state(ParseForm.waiting_city.state)
    await chips.render_city_chips(prompt, user_id)
    return role


async def _handle_city_value(
    message: types.Message,
    state: FSMContext,
    raw_value: str,
    *,
    user_id: int,
) -> str:
    chips.finish_session(user_id, "city")
    city = normalize_city(raw_value)
    update_context(dialog_step=_dialog_step("city", city))
    await state.update_data(city=city)
    await message.answer(
        "Хочешь уточнить поиск ключевыми словами (включить/исключить)?",
        reply_markup=_keywords_keyboard(),
    )
    return city


def _format_user_mention(user: types.User | None) -> str:
    if not user:
        return "приглашённый"
    if user.username:
        return f"@{user.username}"
    if user.full_name:
        return user.full_name
    return str(getattr(user, "id", "приглашённый"))


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
    path = Path(path)
    register_context(path, title=title, city=city)
    allow_share = path.suffix.lower() != ".xlsx"
    share_kb = _report_actions_keyboard(allow_share=allow_share)
    await message.answer_document(InputFile(path), reply_markup=share_kb)
    person = getattr(message, "from_user", None)
    if person:
        chips.record_success(person.id, title, city)
    include_list = _ensure_str_list(include)
    exclude_list = _ensure_str_list(exclude)

    report_ref = str(path)
    analytics_text: str | None = None
    try:
        analytics_text = render_mini_analytics(
            path,
            approx_total=approx_total,
            include=include_list,
            exclude=exclude_list,
        )
        if not analytics_text:
            log.warning("mini_analytics: analytics unavailable for %s", report_ref)
    except Exception as exc:
        log.warning(
            "mini_analytics: failed to render analytics for %s: %s",
            report_ref,
            exc,
            exc_info=True,
        )
        analytics_text = None

    summary = None
    summary_error = False
    try:
        summary = get_summary(path)
    except Exception as exc:
        summary_error = True
        log.warning(
            "mini_analytics: failed to build summary for %s: %s",
            report_ref,
            exc,
            exc_info=True,
        )
    if summary is None and not summary_error:
        log.warning("mini_analytics: summary unavailable for %s", report_ref)
    if person:
        report_share.save_last_report(
            person.id,
            role=title,
            city=city,
            include=include_list,
            exclude=exclude_list,
            volume=approx_total,
            path=path,
            median=getattr(summary, "median", None),
            low=getattr(summary, "low", None),
            high=getattr(summary, "high", None),
            top_companies=getattr(summary, "top_companies", None),
        )
    if analytics_text:
        await message.answer(
            analytics_text,
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
    elif reply_markup is not None:
        await message.answer("Готово ✅", reply_markup=reply_markup)
    activation = referrals.handle_activation_trigger(message.from_user.id, "report")
    if activation and activation.inviter_id:
        mention = _format_user_mention(message.from_user)
        if activation.granted and activation.bonus:
            notify_text = f"🔥 Реферал {mention} активирован — +{activation.bonus} кредит начислен!"
        else:
            notify_text = f"Реферал {mention} активировал триггер, но бонус не начислен (достигнут лимит)."
        try:
            await message.bot.send_message(activation.inviter_id, notify_text)
        except Exception as exc:  # pragma: no cover
            log_event(
                "referral_notify_failed",
                level="WARN",
                inviter_id=activation.inviter_id,
                err=str(exc),
            )


async def cb_report_share(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    snapshot = report_share.get_last_report(call.from_user.id)
    if not snapshot:
        await call.message.answer("Сначала запусти поиск.")
        return
    log_event(
        "share_opened",
        user_id=call.from_user.id,
        role=snapshot.role,
        city=snapshot.city,
    )
    me = await call.bot.get_me()
    link = report_share.build_share_link(me.username or "", call.from_user.id)
    share_text = report_share.build_share_text(snapshot, link)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("Скопировать текст", callback_data="report_share_copy"))
    await call.message.answer(share_text, reply_markup=kb, disable_web_page_preview=True)


async def cb_report_share_copy(call: types.CallbackQuery):
    snapshot = report_share.get_last_report(call.from_user.id)
    if not snapshot:
        await safe_answer(call, "Сначала запусти поиск.", show_alert=True)
        return
    me = await call.bot.get_me()
    link = report_share.build_share_link(me.username or "", call.from_user.id)
    share_text = report_share.build_share_text(snapshot, link)
    if not await safe_answer(call, "Скопируй текст ниже 👇", show_alert=True):
        return
    await call.message.answer(share_text, disable_web_page_preview=True)


async def cb_report_again(call: types.CallbackQuery, state: FSMContext):
    if not await safe_answer(call):
        return
    await cmd_parse(call.message, state)


async def cb_report_menu(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    kb = keyboards.main_kb(is_admin=is_admin(call.from_user.id))
    await call.message.answer("Главное меню:", reply_markup=kb)


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
    snapshot = paywall.SavedRequest(
        kind="bypass",
        query=query,
        city=city,
        overrides=overrides,
    )
    tracker: ProgressReporter | None = None
    decision: QuotaDecision | None = None
    result: parser_adapter.ReportResult | None = None
    try:
        decision = await _ensure_quota(
            message,
            uid,
            user=user,
            snapshot=snapshot,
            reason="parse_bypass",
        )
        if not decision:
            return
        include_list = _ensure_str_list(overrides.get("include"))
        exclude_list = _ensure_str_list(overrides.get("exclude"))
        tracker = await _start_report_progress(message, include_list, exclude_list)
        _log_parse_start(query, city, overrides)

        async def _progress(kind: str, payload: dict) -> None:
            if tracker:
                try:
                    await tracker.handle_event(kind, payload)
                except Exception:  # pragma: no cover
                    log.warning("report progress callback failed", exc_info=True)

        result = await parser_adapter.run_report(
            uid,
            query,
            city,
            role=query,
            include=include_list,
            exclude=exclude_list,
            progress=_progress,
            **{k: v for k, v in overrides.items() if k not in {"include", "exclude"}},
        )
    except Exception as e:  # pragma: no cover
        await _close_progress_fail(tracker)
        event = "parse_timeout" if _is_timeout_error(e) else "parse_error"
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        log_event(event, level="ERROR", err=err_text)
        await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
        complete_operation(ok=False, err=err_text)
        return
    else:
        path = result.xlsx_path if result else None
        if path and path.exists():
            await _finalize_quota_usage(message, uid, decision)
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
            await _close_progress_success(tracker)
        else:
            await _close_progress_fail(tracker)
            await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
            log_event("parse_error", level="ERROR", err="report_missing")
            complete_operation(ok=False, err="report_missing")
    finally:
        clear_busy(uid)


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

    snapshot = paywall.SavedRequest(
        kind="amount",
        query=title,
        city=city,
        overrides=ov,
        area_id=area_id,
        total=total,
        approx_total=total,
    )
    decision: QuotaDecision | None = None
    tracker: ProgressReporter | None = None
    result: parser_adapter.ReportResult | None = None

    try:
        decision = await _ensure_quota(
            message,
            uid,
            user=user,
            snapshot=snapshot,
            reason="parse_amount",
        )
        if not decision:
            return

        include_list = _ensure_str_list(ov.get("include"))
        exclude_list = _ensure_str_list(ov.get("exclude"))
        tracker = await _start_report_progress(message, include_list, exclude_list)
        _log_parse_start(title, city, ov, approx_total=total)

        async def _progress(kind: str, payload: dict) -> None:
            if tracker:
                try:
                    await tracker.handle_event(kind, payload)
                except Exception:  # pragma: no cover
                    log.warning("report progress callback failed", exc_info=True)

        run_kwargs = {k: v for k, v in ov.items() if k not in {"include", "exclude"}}
        result = await parser_adapter.run_report(
            uid,
            title,
            city,
            role=title,
            timeout=timeout,
            include=include_list,
            exclude=exclude_list,
            progress=_progress,
            **run_kwargs,
        )
    except Exception as e:
        await _close_progress_fail(tracker)
        err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
        event = "parse_timeout" if _is_timeout_error(e) else "parse_error"
        log_event(event, level="ERROR", err=err_text)
        await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
        complete_operation(ok=False, err=err_text)
        return
    else:
        path = result.xlsx_path if result else None
        if path and path.exists():
            if decision:
                await _finalize_quota_usage(message, uid, decision)
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
            await _close_progress_success(tracker)
        else:
            await _close_progress_fail(tracker)
            await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
            log_event("parse_error", level="ERROR", err="report_missing")
            complete_operation(ok=False, err="report_missing")
    finally:
        clear_busy(uid)


# ---------- core ----------
async def _run_parser(
    message: types.Message,
    query: str,
    city: str,
    overrides: dict[str, object],
    *,
    uid: int | None = None,
    user: types.User | None = None,
    state: FSMContext | None = None,
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
    ok, norm_title, area_id, canonical_city, bad_msg = validator.validate_request(query, city)
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
        if state is not None:
            await state.set_state(ParseForm.waiting_query.state)
        else:
            log_event(
                "fsm_state_missing",
                level="WARN",
                message="FSM context unavailable when resetting to waiting_query",
            )
        complete_operation(ok=False, err="validation_warning")
        return

    # 2) Если юзер сам задал pages/per_page — запускаем без шага объёма (и блокируем пользователя)
    city_to_use = canonical_city or city
    city = city_to_use

    if "pages" in overrides or "per_page" in overrides:
        if not set_busy(requester_id):
            await message.answer(BUSY_TEXT)
            return
        ov = dict(overrides)
        if "area" not in ov:
            ov["area"] = area_id
        snapshot = paywall.SavedRequest(
            kind="direct",
            query=norm_title,
            city=city_to_use,
            overrides=ov,
        )
        decision: QuotaDecision | None = None
        tracker: ProgressReporter | None = None
        result: parser_adapter.ReportResult | None = None
        try:
            decision = await _ensure_quota(
                message,
                requester_id,
                user=user,
                snapshot=snapshot,
                reason="parse_direct",
            )
            if not decision:
                return
            include_list = _ensure_str_list(ov.get("include"))
            exclude_list = _ensure_str_list(ov.get("exclude"))
            tracker = await _start_report_progress(message, include_list, exclude_list)
            _log_parse_start(norm_title, city_to_use, ov)

            async def _progress(kind: str, payload: dict) -> None:
                if tracker:
                    try:
                        await tracker.handle_event(kind, payload)
                    except Exception:  # pragma: no cover
                        log.warning("report progress callback failed", exc_info=True)

            run_kwargs = {k: v for k, v in ov.items() if k not in {"include", "exclude"}}
            result = await parser_adapter.run_report(
                requester_id,
                norm_title,
                city_to_use,
                role=norm_title,
                include=include_list,
                exclude=exclude_list,
                progress=_progress,
                **run_kwargs,
            )
        except Exception as e:
            if tracker:
                await tracker.fail()
            err_text = (str(e) or "").strip() or "Не удалось получить отчёт: парсер вернул ошибку. Попробуйте позже"
            event = "parse_timeout" if _is_timeout_error(e) else "parse_error"
            log_event(event, level="ERROR", err=err_text)
            await message.answer(err_text, reply_markup=_main_menu_kb(message, user=user))
            complete_operation(ok=False, err=err_text)
            return
        else:
            path = result.xlsx_path if result else None
            if path and path.exists():
                if decision:
                    await _finalize_quota_usage(message, requester_id, decision)
                _log_parse_ready(norm_title, city_to_use, ov)
                await _send_report_with_analytics(
                    message,
                    path,
                    title=norm_title,
                    city=city_to_use,
                    include=ov.get("include"),
                    exclude=ov.get("exclude"),
                    reply_markup=_main_menu_kb(message, user=user),
                )
                await _close_progress_success(tracker)
            else:
                await _close_progress_fail(tracker)
                await message.answer("Отчёт не найден. Проверьте логи.", reply_markup=_main_menu_kb(message, user=user))
                log_event("parse_error", level="ERROR", err="report_missing")
                complete_operation(ok=False, err="report_missing")
        finally:
            clear_busy(requester_id)
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

    _PENDING_QTY[requester_id] = (norm_title, city_to_use, area_id, overrides, max_total)
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
        await _run_parser(message, query, city, overrides, state=state)
        return

    update_context(command="parse_dialog")
    log_event("request_parsed", message="parse dialog start", command="parse_dialog")
    prompt = await message.answer("Введите должность:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ParseForm.waiting_query.state)
    await chips.render_role_chips(prompt, message.from_user.id)


async def process_query(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    text = message.text or ""
    await _handle_role_value(message, state, text, user_id=message.from_user.id)


async def process_city(message: types.Message, state: FSMContext):
    if is_busy(message.from_user.id):
        await message.answer(BUSY_TEXT)
        return
    city = message.text or ""
    await _handle_city_value(message, state, city, user_id=message.from_user.id)


async def cb_chip(call: types.CallbackQuery, state: FSMContext):
    payload = chips.parse_callback_data(call.data or "")
    if not payload:
        return

    kind = payload.get("kind")
    if kind not in {"role", "city"}:
        await safe_answer(call)
        return

    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return

    token = payload.get("token")
    session = chips.get_session(token or "")
    if (
        not token
        or session is None
        or session.kind != kind
        or not chips.is_active(call.from_user.id, session.kind, session.token)
    ):
        await safe_answer(call, "Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    action = payload.get("action")
    if action == "more":
        markup = chips.change_page(session, 1)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "more", "control", position=None, action="more")
        await safe_answer(call)
        return

    if action == "prev":
        markup = chips.change_page(session, -1)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "prev", "control", position=None, action="prev")
        await safe_answer(call)
        return

    if action == "category":
        try:
            category_index = int(payload.get("value", "-1"))
        except ValueError:
            await safe_answer(call, "Эта клавиатура устарела. Начните заново.", show_alert=False)
            return
        markup = chips.show_category(session, category_index)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(
            session.kind,
            "category",
            "control",
            position=category_index + 1 if category_index >= 0 else None,
            action="category",
        )
        await safe_answer(call)
        return

    if action == "back":
        markup = chips.back_to_categories(session)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "back", "control", position=None, action="back")
        await safe_answer(call)
        return

    if action == "random":
        if session.kind != "role":
            await safe_answer(call)
            return
        value = chips.random_role()
        if not value:
            await safe_answer(call, "Нет доступных вариантов", show_alert=False)
            return
        chips.log_click("role", value, "base", position=None, action="random")
        chips.finish_session(call.from_user.id, "role")
        try:
            await call.message.edit_reply_markup()
        except (MessageCantBeEdited, MessageNotModified):
            pass
        if not await safe_answer(call, f"Выбрано: {value}"):
            return
        await _handle_role_value(call.message, state, value, user_id=call.from_user.id)
        return

    if action != "pick":
        await safe_answer(call)
        return

    try:
        index = int(payload.get("value", "-1"))
    except ValueError:
        await safe_answer(call, "Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    candidate = chips.resolve_candidate(session, index)
    if not candidate:
        await safe_answer(call, "Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    chips.log_click(session.kind, candidate.value, candidate.source, position=index + 1)
    chips.finish_session(call.from_user.id, session.kind)
    try:
        await call.message.edit_reply_markup()
    except (MessageCantBeEdited, MessageNotModified):
        pass
    if not await safe_answer(call, f"Выбрано: {candidate.value}"):
        return

    if session.kind == "role":
        await _handle_role_value(call.message, state, candidate.value, user_id=call.from_user.id)
    else:
        await _handle_city_value(call.message, state, candidate.value, user_id=call.from_user.id)


# ---------- ключевые слова (include/exclude) ----------
async def cb_kw_yes(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return
    if not await safe_answer(call):
        return
    update_context(dialog_step=_dialog_step("kw_prompt", "include"))
    await call.message.answer(
        "Введи слова, которые ДОЛЖНЫ встречаться (через запятую). Пример: электроника, b2b, pcb.\n"
        "Если не нужно — пришли пусто или «-».",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(ParseForm.waiting_kw_include.state)


async def cb_kw_no(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return
    if not await safe_answer(call):
        return
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
        state=state,
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
    await state.set_state(ParseForm.waiting_kw_exclude.state)


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
    await _run_parser(
        message,
        query,
        city,
        {"include": include, "exclude": exclude},
        state=state,
    )


# ---------- callbacks из предупреждения / объём / превью ----------
async def cb_parse_force(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return
    payload = _WARN_CACHE.pop(call.from_user.id, None)
    if not await safe_answer(call):
        return
    if not payload:
        await call.message.answer("Не нашёл последний запрос. Введи должность ещё раз:")
        await state.set_state(ParseForm.waiting_query.state)
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


async def cb_parse_fix(call: types.CallbackQuery, state: FSMContext):
    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return
    _WARN_CACHE.pop(call.from_user.id, None)
    if not await safe_answer(call):
        return
    update_context(dialog_step=_dialog_step("fix_query", ""))
    await call.message.answer("Окей! Введи должность ещё раз:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ParseForm.waiting_query.state)


async def cb_qty(call: types.CallbackQuery, state: FSMContext):
    # если занят — просто подсказка и выходим
    if is_busy(call.from_user.id):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return

    payload = _PENDING_QTY.get(call.from_user.id)
    if not await safe_answer(call):
        return
    if not payload:
        await call.message.answer("Не нашёл предыдущий запрос. Введи должность ещё раз:")
        await state.set_state(ParseForm.waiting_query.state)
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


async def cb_preview(call: types.CallbackQuery, state: FSMContext):
    """Показать 5 первых совпадений без уничтожения кеша запроса."""
    uid = call.from_user.id
    if not set_busy(uid):
        await safe_answer(call, BUSY_TEXT, show_alert=False)
        return

    if not await safe_answer(call, "Готовлю превью…", show_alert=False):
        return

    tracker: ProgressReporter | None = None
    try:
        payload = _PENDING_QTY.get(uid)   # ВАЖНО: .get(), НЕ .pop()!
        if not payload:
            await call.message.answer("Не нашёл предыдущий запрос. Введи должность ещё раз:")
            await state.set_state(ParseForm.waiting_query.state)
            return

        title, city, area_id, overrides, _max_total = payload
        include = (overrides or {}).get("include") or []
        exclude = (overrides or {}).get("exclude") or []

        if not ALLOW_FREE_PREVIEW:
            snapshot = paywall.SavedRequest(
                kind="preview",
                query=title,
                city=city,
                overrides=overrides or {},
                area_id=area_id,
            )
            decision = await _ensure_quota(
                call.message,
                uid,
                user=call.from_user,
                snapshot=snapshot,
                reason="preview",
            )
            if not decision:
                return

        tracker = create_preview_progress(call.message.bot, call.message.chat.id)
        _log_preview_start(title, city, overrides)

        async def _progress(kind: str, payload: dict) -> None:
            if tracker:
                try:
                    await tracker.handle_event(kind, payload)
                except Exception:  # pragma: no cover
                    log.warning("preview progress callback failed", exc_info=True)

        try:
            preview_result = await parser_adapter.preview_rows(
                uid,
                title,
                city,
                area=area_id,
                include=include,
                exclude=exclude,
                progress=_progress,
            )
        except parser_adapter.DiagnosticError as exc:
            _log_preview_timeout(title, city, overrides, err=str(exc))
            await _close_progress_fail(tracker)
            await call.message.answer(
                "Не получилось подготовить превью 😔\n"
                "Попробуйте ещё раз позже.\n"
                f"ID: `{exc.bundle_id}`",
                parse_mode="Markdown",
            )
            return
        except Exception as exc:
            _log_preview_timeout(title, city, overrides, err=str(exc))
            await _close_progress_fail(tracker)
            await call.message.answer("⏳ Превью не успело загрузиться. Попробуй ещё раз.")
            return

        rows = preview_result.rows
        if not rows:
            await call.message.answer("Совпадений не нашлось по текущим критериям.")
            _log_preview_ready(title, city, 0, overrides)
            await _close_progress_success(tracker)
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
        await _close_progress_success(tracker)
    finally:
        clear_busy(uid)


async def prompt_resume(bot: Bot, user_id: int) -> None:
    request = paywall.get_request(user_id)
    if not request:
        log_event("resume_prompt_skipped", message="resume cache empty", args={"user_id": user_id})
        try:
            await bot.send_message(
                user_id,
                "Оплата прошла ✅ Лимит обновлён — начни поиск с «🔎 Поиск».",
                reply_markup=keyboards.main_kb(is_admin=is_admin(user_id)),
            )
        except Exception as exc:  # pragma: no cover
            log_event(
                "resume_prompt_failed",
                level="WARN",
                err=str(exc),
                args={"user_id": user_id},
            )
        return

    text = f"Оплата прошла ✅ Запустить прошлый запрос: «{request.summary()}»?"
    kb = paywall.resume_keyboard()
    try:
        await bot.send_message(user_id, text, reply_markup=kb)
        log_event("resume_prompt_shown", message="resume prompt shown", args={"request": request.to_log()})
    except Exception as exc:  # pragma: no cover
        log_event(
            "resume_prompt_failed",
            level="WARN",
            err=str(exc),
            args={"request": request.to_log()},
        )


async def cb_resume_yes(call: types.CallbackQuery, state: FSMContext):
    if not await safe_answer(call):
        return
    request = paywall.consume_request(call.from_user.id)
    if not request:
        await call.message.answer(
            "Не нашёл сохранённый запрос. Начни заново с «🔎 Поиск».",
            reply_markup=_main_menu_kb(call.message, user=call.from_user),
        )
        log_event("resume_prompt_skipped", message="resume cache empty on confirm", args={"user_id": call.from_user.id})
        return

    log_event("resume_confirmed", message="resume confirmed", args={"request": request.to_log()})

    if request.kind == "amount" and request.area_id is not None and request.total is not None:
        await _run_with_amount(
            call.message,
            request.query,
            request.city,
            request.area_id,
            request.overrides,
            request.total,
            uid=call.from_user.id,
            user=call.from_user,
        )
    elif request.kind == "direct":
        await _run_parser(
            call.message,
            request.query,
            request.city,
            request.overrides,
            uid=call.from_user.id,
            user=call.from_user,
            state=state,
        )
    elif request.kind == "bypass":
        await _run_parser_bypass_validation(
            call.message,
            request.query,
            request.city,
            request.overrides,
            uid=call.from_user.id,
            user=call.from_user,
        )
    else:
        await call.message.answer(
            "Не удалось восстановить параметры запроса. Начни заново с «🔎 Поиск».",
            reply_markup=_main_menu_kb(call.message, user=call.from_user),
        )
        log_event(
            "resume_prompt_skipped",
            message="resume unsupported kind",
            args={"request": request.to_log()},
        )


async def cb_resume_skip(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    request = paywall.get_request(call.from_user.id)
    paywall.clear_request(call.from_user.id)
    log_event("resume_skipped", message="resume skipped", args={"request": request.to_log() if request else None})
    await call.message.answer(
        "Хорошо! Когда будешь готов(а) — запусти поиск через «🔎 Поиск».",
        reply_markup=_main_menu_kb(call.message, user=call.from_user),
    )


def register(dp: Dispatcher):
    # команды и диалог
    dp.register_message_handler(cmd_parse, commands=["parse"], state="*")
    dp.register_message_handler(cmd_parse, lambda m: m.text == "🔎 Поиск", state="*")
    dp.register_message_handler(process_query, state=ParseForm.waiting_query)
    dp.register_message_handler(process_city, state=ParseForm.waiting_city)
    dp.register_callback_query_handler(cb_chip, lambda c: c.data and c.data.startswith("chip:"), state="*")

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
    dp.register_callback_query_handler(cb_report_share, lambda c: c.data == "report_share", state="*")
    dp.register_callback_query_handler(cb_report_share_copy, lambda c: c.data == "report_share_copy", state="*")
    dp.register_callback_query_handler(cb_report_again, lambda c: c.data == "report_again", state="*")
    dp.register_callback_query_handler(cb_report_menu, lambda c: c.data == "report_menu", state="*")
    dp.register_callback_query_handler(cb_resume_yes,  lambda c: c.data == "resume:last", state="*")
    dp.register_callback_query_handler(cb_resume_skip, lambda c: c.data == "resume:skip", state="*")
