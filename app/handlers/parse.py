from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import shutil
import time
import traceback
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
from aiogram.utils.exceptions import BadRequest, MessageCantBeEdited, MessageNotModified

# анти-спам / занятость пользователя
from ..middlewares.busy import BUSY_TEXT, clear_busy, is_busy, set_busy

from ..services import parser_adapter
from ..services import referrals
from ..services import validator  # валидация запроса
from ..services import chips
from ..services.mini_analytics import register_context, render_mini_analytics
from ..services import report_share
from ..services import paywall
from ..services.quota import FREE_PER_MONTH, QuotaDecision, check_quota, commit_usage
from ..services.diagnostics import get_bundle_by_correlation, remember_bundle
from app.services.deliver_diagnostics import (
    DeliverDiagContext,
    build_diag_bundle,
    build_stack,
)
from app import keyboards
from app.utils.admins import admin_ids, is_admin
from app.utils.diag import make_diag_dir, save_text, zip_dir
from app.utils.logging import complete_operation, log_event, update_context
from app.utils.errors import (
    classify_error,
    is_retryable,
    message_for_code,
    user_message_for_invalid_args,
    user_message_for_no_data,
)
from app.utils.progress import Progress, ProgressStep
from app.utils.report_sender import SendReportResult, send_report
from app.utils.xlsx_diagnostics import collect_xlsx_diagnostics
from app.utils.normalize import normalize_city, normalize_role

# Кеш последнего «сомнительного» запроса: user_id -> (query, city, overrides)
_WARN_CACHE: Dict[int, Tuple[str, str, dict]] = {}
# Кеш шага выбора объёма: user_id -> (norm_title, city, area_id, overrides, max_total)
_PENDING_QTY: Dict[int, Tuple[str, str, int, dict, int]] = {}
# Активные задачи выгрузки: user_id -> asyncio.Task
_ACTIVE_REPORT_JOBS: dict[int, asyncio.Task] = {}


def _is_report_job_active(uid: int) -> bool:
    task = _ACTIVE_REPORT_JOBS.get(uid)
    return bool(task) and not task.done()


def _register_report_job(uid: int, task: asyncio.Task) -> None:
    _ACTIVE_REPORT_JOBS[uid] = task

    def _cleanup(_task: asyncio.Task, *, user_id: int = uid) -> None:
        existing = _ACTIVE_REPORT_JOBS.get(user_id)
        if existing is _task:
            _ACTIVE_REPORT_JOBS.pop(user_id, None)

    task.add_done_callback(_cleanup)

PROGRESS_STEPS = (
    ProgressStep("fetch", "парсинг страниц hh…"),
    ProgressStep("normalize", "нормализация…"),
    ProgressStep("write_xlsx", "сбор XLSX…"),
)
PROGRESS_SUCCESS_TEXT = "Готово ✅"
PROGRESS_FAILURE_PREFIX = "❌ Не получилось…"

_DIAG_ENV_KEYS = [
    "PYBIN",
    "PARSER_PIPELINE",
    "REPORT_DIR",
    "AREA",
    "PAGES",
    "PER_PAGE",
    "SITE",
    "PAUSE",
]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


SEND_DIAG_BUNDLES = _env_bool("SEND_DIAG_BUNDLES", True)
_DIAG_ENABLED = _env_bool("DIAG", False)
_ADMIN_FORWARD_IDS = tuple(admin_ids())


def _collect_diag_env() -> str:
    lines = []
    for key in _DIAG_ENV_KEYS:
        value = os.getenv(key)
        lines.append(f"{key}={value if value is not None else ''}")
    return "\n".join(lines)


def _safe_document_name(path: Path) -> tuple[str, bool]:
    original = path.name
    cleaned = original.strip()
    if not cleaned or cleaned in {".", ".."}:
        return _fallback_name(path), False
    if any(sep in cleaned for sep in ("/", "\\", "\n", "\r", "\t")):
        return _fallback_name(path), False
    if len(cleaned) > 120:
        return _fallback_name(path), False
    return cleaned, True


def _fallback_name(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "report.csv"
    return "report.xlsx"


def _extract_correlation_from_bundle(path: Path | None) -> str | None:
    if not path:
        return None
    stem = path.stem
    if stem.startswith("bundle_"):
        remainder = stem[len("bundle_") :].strip()
        return remainder or None
    return None


def _save_parser_diag(
    user_id: int,
    *,
    exc: parser_adapter.ParserRunError,
    query: str,
    city: str,
    area_id: int | None,
    include: list[str] | None,
    exclude: list[str] | None,
    amount: int | None,
    mode: str,
    retries: int,
    error_code: str | None,
    progress_last_step: str | None,
) -> Path:
    diag_dir = make_diag_dir(user_id)
    command_text = " ".join(str(part) for part in exc.cmd)
    save_text(diag_dir, "command.txt", command_text)
    save_text(diag_dir, "stdout.txt", exc.stdout or "")
    save_text(diag_dir, "stderr.txt", exc.stderr or "")
    save_text(diag_dir, "env.txt", _collect_diag_env())

    meta = {
        "query": query,
        "city": city,
        "area_id": area_id,
        "include": include or [],
        "exclude": exclude or [],
        "amount": amount,
        "mode": mode,
        "error": repr(exc),
        "returncode": exc.returncode,
        "retries": retries,
        "error_code": error_code,
        "progress_last_step": progress_last_step,
    }
    save_text(diag_dir, "meta.json", json.dumps(meta, ensure_ascii=False, indent=2))
    try:
        zip_dir(diag_dir)
    except Exception:
        pass

    log_event("ERROR", "parse.diag_saved", dir=str(diag_dir))
    return diag_dir


def _format_failure_message(
    *,
    invalid_arguments: str | None,
    diag_dir: Path | None,
    user: types.User | None,
    default_message: str,
) -> str:
    text = invalid_arguments or default_message
    if diag_dir and user and is_admin(user.id):
        text += f"\nДиагностика сохранена: {diag_dir.name}"
    return text


async def _send_diagnostic_bundle_if_needed(
    bot: Bot,
    *,
    bundle_path: Path | None,
    correlation: str | None,
    user_chat_id: int | None,
    user_id: int | None,
) -> None:
    if not SEND_DIAG_BUNDLES or not bundle_path or not bundle_path.exists():
        return

    caption = f"diag {correlation}" if correlation else "diagnostic bundle"
    targets: list[tuple[int, str, str]] = []
    seen: set[int] = set()

    if user_chat_id is not None and user_chat_id not in seen:
        targets.append((user_chat_id, caption, "user"))
        seen.add(user_chat_id)

    for admin_id in _ADMIN_FORWARD_IDS:
        if admin_id in seen or admin_id == user_id:
            continue
        targets.append((admin_id, caption, "admin"))
        seen.add(admin_id)

    for chat_id, doc_caption, target in targets:
        try:
            await bot.send_document(chat_id, InputFile(bundle_path), caption=doc_caption)
            log_event(
                "diagnostic_bundle_sent",
                action="auto_send",
                target=target,
                chat_id=chat_id,
                correlation_id=correlation,
            )
        except Exception as exc:
            log_event(
                "diagnostic_bundle_send_failed",
                level="WARN",
                target=target,
                chat_id=chat_id,
                correlation_id=correlation,
                err=str(exc),
            )


async def _dispatch_deliver_bundle(
    bot: Bot,
    *,
    bundle_path: Path,
    correlation_id: str | None,
    user_chat_id: int | None,
    user_id: int | None,
) -> None:
    if not bundle_path.exists():
        return

    caption = (
        "⚠️ Ошибка доставки отчёта. Диагностика во вложении. Мы уже смотрим"
    )
    if correlation_id:
        caption += f" (ID: {correlation_id})"
    targets: list[tuple[int, str]] = []
    delivered: set[int] = set()

    if user_chat_id is not None:
        targets.append((user_chat_id, "user"))
        delivered.add(user_chat_id)

    for admin_id in _ADMIN_FORWARD_IDS:
        if admin_id == user_id or admin_id in delivered:
            continue
        targets.append((admin_id, "admin"))
        delivered.add(admin_id)

    for chat_id, target in targets:
        try:
            await bot.send_document(chat_id, InputFile(bundle_path), caption=caption)
            event_name = (
                "diagnostic_bundle_sent_user" if target == "user" else "diagnostic_bundle_sent_admin"
            )
            log_event(
                event_name,
                chat_id=chat_id,
                correlation_id=correlation_id,
                path=str(bundle_path),
            )
        except Exception as exc:
            log_event(
                "diagnostic_bundle_send_failed",
                level="WARN",
                target=target,
                chat_id=chat_id,
                correlation_id=correlation_id,
                err=str(exc),
            )


def _update_ui_meta(
    result: parser_adapter.RunReportResult | None,
    *,
    ack_sent: bool | None,
    progress_strategy: str | None,
    report_path: Path | None,
    send_error: str | None = None,
) -> None:
    if not result or not result.meta:
        return

    ui_payload: dict[str, object] = {
        "ack_sent": bool(ack_sent) if ack_sent is not None else False,
        "progress_strategy": progress_strategy or "edit",
        "report_path": str(report_path) if report_path else None,
    }
    if send_error:
        ui_payload["send_error"] = send_error

    result.meta["ui"] = {k: v for k, v in ui_payload.items() if v is not None}

    bundle_path = getattr(result, "bundle_path", None)
    if not bundle_path:
        return
    bundle_dir = bundle_path.with_suffix("")
    meta_path = bundle_dir / "meta.json"
    if not meta_path.exists():
        return
    try:
        try:
            meta_data = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta_data = {}
        meta_data["ui"] = result.meta["ui"]
        meta_path.write_text(
            json.dumps(meta_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        shutil.make_archive(str(bundle_dir), "zip", root_dir=bundle_dir, base_dir=".")
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        log_event(
            "diagnostic_meta_update_failed",
            level="WARN",
            bundle=str(bundle_path),
            err=str(exc),
        )


async def _cleanup_inline_message(message: types.Message) -> None:
    try:
        await message.edit_reply_markup(reply_markup=None)
    except MessageNotModified:
        pass
    except (MessageCantBeEdited, BadRequest) as exc:
        log_event(
            "ui.cleanup_failed",
            level="WARN",
            err=str(exc),
            reason="remove_inline_markup",
        )
    except Exception as exc:  # pragma: no cover - UI cleanup best effort
        log_event(
            "ui.cleanup_failed",
            level="WARN",
            err=str(exc),
            reason="remove_inline_markup",
        )


class ReportProgressTracker:
    def __init__(self, progress: Progress, *, has_filter: bool):
        self._progress = progress
        self._has_filter = has_filter

    @property
    def progress(self) -> Progress:
        return self._progress

    @property
    def ui_strategy(self) -> str:
        return self._progress.ui_strategy

    @property
    def last_percent(self) -> int | None:
        return getattr(self._progress, "last_percent", None)

    async def mark_command_ready(self) -> None:
        await self._progress.set("fetch", 10, force=True)

    async def mark_process_started(self) -> None:
        await self._progress.set("fetch", 30, force=True)

    async def handle_event(self, kind: str, payload: dict) -> None:
        if kind == "status":
            status = str(payload.get("status") or "").lower()
            if status == "page":
                extra = self._page_hint(payload)
                await self._progress.set("fetch", 30, extra_text=extra)
            elif status == "csv":
                extra = self._page_hint(payload)
                await self._progress.set("normalize", 60, extra_text=extra)
            elif status == "report" and str(payload.get("format") or "").lower() == "xlsx":
                await self._progress.set("write_xlsx", 90)
        elif kind == "filter_start" and self._has_filter:
            await self._progress.set("normalize", 60)

    async def finish_success(self, *, delete_after: float | None = 8.0) -> None:
        await self._progress.close(ok=True, text=PROGRESS_SUCCESS_TEXT, delete_after=delete_after)

    async def fail(self, message: str) -> None:
        await self._progress.close(ok=False, text=message)

    async def show_retry(self, attempt: int, total: int) -> None:
        await self._progress.show_retry(attempt, total)

    async def clear_retry(self) -> None:
        await self._progress.clear_retry()

    def _page_hint(self, payload: dict) -> str | None:
        page = payload.get("page")
        total = payload.get("pages")
        if page is None:
            page = payload.get("current")
        if total is None:
            total = payload.get("total")
        if page is None:
            page = payload.get("current_page")
        if total is None:
            total = payload.get("total_pages")
        try:
            page_int = int(page)
            total_int = int(total)
        except (TypeError, ValueError):
            return None
        if total_int <= 0:
            return None
        if page_int < 1:
            page_int += 1
        page_int = max(1, min(page_int, total_int))
        return f"страница {page_int}/{total_int}"


async def _start_report_progress(
    message: types.Message,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> ReportProgressTracker:
    progress = await Progress.create(
        message.bot,
        message.chat.id,
        PROGRESS_STEPS,
        mode="report",
        initial_step=PROGRESS_STEPS[0].name,
    )
    return ReportProgressTracker(progress, has_filter=bool(include or exclude))


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


_REPORT_OVERRIDE_KEYS = {"pages", "per_page", "pause", "site", "area"}


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


def _report_actions_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
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
    await ParseForm.waiting_city.set()
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
    log_event(
        "report_ready",
        message=f"report_ready title='{title}' city='{city}'",
        args=args,
    )


def _log_preview_start(title: str, city: str, overrides: dict | None = None) -> None:
    args = _build_args(title, city, overrides)
    log_event(
        "preview_requested",
        message=f"preview_requested title='{title}' city='{city}'",
        args=args,
    )


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


def _error_message_for_result(result: parser_adapter.RunReportResult) -> str:
    if not result:
        return message_for_code("UNKNOWN")
    code = result.err_code or ""
    if code == "E_TIMEOUT":
        return result.user_message or message_for_code("TIMEOUT")
    if code == "E_NONZERO_RC":
        return result.user_message or message_for_code("PIPELINE_ERROR")
    if code == "E_NO_FILE":
        return "Файл отчёта не сформировался. Попробуй ещё раз."
    if code == "E_BAD_ARGS":
        return result.user_message or user_message_for_invalid_args()
    return result.user_message or message_for_code("UNKNOWN")


def _remember_bundle_for_chat(
    message: types.Message,
    result: parser_adapter.RunReportResult,
) -> str | None:
    if not result.bundle_path or not result.meta:
        return None
    correlation = str(result.meta.get("correlation_id") or "").strip()
    chat_id = getattr(message.chat, "id", None)
    if not correlation or chat_id is None:
        return None
    remember_bundle(chat_id=chat_id, correlation_id=correlation, bundle_path=result.bundle_path)
    return correlation


async def _send_failure_response(
    message: types.Message,
    result: parser_adapter.RunReportResult,
    *,
    user: types.User | None = None,
    tracker: ReportProgressTracker | None = None,
) -> None:
    text = _error_message_for_result(result)
    if tracker:
        await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} {text}")

    correlation = _remember_bundle_for_chat(message, result)
    menu_markup = _main_menu_kb(message, user=user)
    user_id = getattr(user, "id", None)
    if is_admin(user_id) and result.bundle_path and correlation:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("📎 Логи (zip)", callback_data=f"diag_bundle:{correlation}"))
        await message.answer(text, reply_markup=kb)
        await message.answer("Главное меню:", reply_markup=menu_markup)
    else:
        await message.answer(text, reply_markup=menu_markup)

    chat_id = getattr(message.chat, "id", None)
    await _send_diagnostic_bundle_if_needed(
        message.bot,
        bundle_path=result.bundle_path if result else None,
        correlation=correlation,
        user_chat_id=chat_id,
        user_id=user_id,
    )

    complete_operation(ok=False, err=result.err_code or result.err_message)


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
    diagnostic_path: Path | None = None,
    diagnostic_caption: str | None = None,
    file_name_override: str | None = None,
) -> SendReportResult:
    register_context(path, title=title, city=city)
    share_kb = _report_actions_keyboard()
    send_result = await send_report(
        message.bot,
        message.chat.id,
        path,
        reply_markup=share_kb,
        diagnostic_path=diagnostic_path,
        diagnostic_caption=diagnostic_caption,
        file_name=file_name_override,
    )
    if not send_result.ok:
        return send_result

    person = getattr(message, "from_user", None)
    if person:
        chips.record_success(person.id, title, city)
    include_list = _ensure_str_list(include)
    exclude_list = _ensure_str_list(exclude)
    text = render_mini_analytics(
        path,
        approx_total=approx_total,
        include=include_list,
        exclude=exclude_list,
    )
    summary = get_summary(path)
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
    if text:
        await message.answer(text, disable_web_page_preview=True, reply_markup=reply_markup)
    elif reply_markup is not None:
        await message.answer("Готово ✅", reply_markup=reply_markup)
    activation = referrals.handle_activation_trigger(message.from_user.id, "report")
    if activation and activation.inviter_id:
        mention = _format_user_mention(message.from_user)
        if activation.granted and activation.bonus:
            notify_text = f"🔥 Реферал {mention} активирован — +{activation.bonus} кредит начислен!"
        else:
            notify_text = (
                f"Реферал {mention} активировал триггер, но бонус не начислен (достигнут лимит)."
            )
        try:
            await message.bot.send_message(activation.inviter_id, notify_text)
        except Exception as exc:  # pragma: no cover
            log_event(
                "referral_notify_failed",
                level="WARN",
                inviter_id=activation.inviter_id,
                err=str(exc),
            )
    return send_result


async def cb_report_share(call: types.CallbackQuery):
    await call.answer()
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
        await call.answer("Сначала запусти поиск.", show_alert=True)
        return
    me = await call.bot.get_me()
    link = report_share.build_share_link(me.username or "", call.from_user.id)
    share_text = report_share.build_share_text(snapshot, link)
    await call.answer("Скопируй текст ниже 👇", show_alert=True)
    await call.message.answer(share_text, disable_web_page_preview=True)


async def cb_report_again(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await cmd_parse(call.message, state)


async def cb_report_menu(call: types.CallbackQuery):
    await call.answer()
    kb = keyboards.main_kb(is_admin=is_admin(call.from_user.id))
    await call.message.answer("Главное меню:", reply_markup=kb)


async def cb_diag_bundle(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    if not call.data:
        await call.answer("Некорректный запрос", show_alert=True)
        return
    try:
        _, correlation = call.data.split(":", 1)
    except ValueError:
        await call.answer("Некорректный запрос", show_alert=True)
        return
    bundle = get_bundle_by_correlation(correlation)
    if not bundle or not bundle.exists():
        await call.answer("Бандл не найден", show_alert=True)
        return

    await call.answer()
    log_event("diagnostic_bundle_sent", action="send_bundle", correlation_id=correlation)
    await call.message.answer_document(InputFile(bundle), caption=f"diag {correlation}")


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
        if tracker:
            try:
                await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} Уже выполняю другой запрос")
            except Exception:
                pass
        await message.answer(BUSY_TEXT)
        return
    snapshot = paywall.SavedRequest(
        kind="bypass",
        query=query,
        city=city,
        overrides=overrides,
    )
    tracker: ReportProgressTracker | None = None
    decision: QuotaDecision | None = None
    result: parser_adapter.RunReportResult | None = None
    diag_dir: Path | None = None
    include_list: list[str] = []
    exclude_list: list[str] = []
    retries_done = 0
    backoffs = (2, 6)
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
        await tracker.mark_command_ready()
        _log_parse_start(query, city, overrides)
        log_event(
            "INFO",
            "parse.start",
            user_id=uid,
            mode="report",
            query=query,
            city=city,
            overrides=overrides,
        )

        async def _progress(kind: str, payload: dict) -> None:
            if tracker:
                await tracker.handle_event(kind, payload)

        allowed_kwargs = {k: v for k, v in overrides.items() if k in _REPORT_OVERRIDE_KEYS}
        extra_keys = sorted(
            k for k in overrides.keys() if k not in _REPORT_OVERRIDE_KEYS | {"include", "exclude"}
        )
        if extra_keys:
            dropped_flags = [f"--{key.replace('_', '-')}" for key in extra_keys]
            log_event(
                "parser_cli_args_dropped",
                level="WARN",
                dropped_flags=dropped_flags,
                query=query,
                city=city,
                user_id=uid,
            )

        while True:
            try:
                if tracker:
                    if retries_done > 0:
                        await tracker.clear_retry()
                        await tracker.mark_command_ready()
                    await tracker.mark_process_started()
                result = await parser_adapter.run_report(
                    uid,
                    query,
                    city,
                    role=query,
                    include=include_list,
                    exclude=exclude_list,
                    progress=_progress,
                    **allowed_kwargs,
                )
                break
            except parser_adapter.ParserRunError as exc:
                error_info = classify_error(exc, exc.stdout, exc.stderr)
                log_event(
                    "ERROR",
                    "parse.error",
                    user_id=uid,
                    mode="report",
                    query=query,
                    city=city,
                    error_code=error_info.code,
                    hint=error_info.hint_for_log,
                    attempt=retries_done + 1,
                )
                if is_retryable(error_info.code) and retries_done < len(backoffs):
                    retries_done += 1
                    if tracker:
                        await tracker.show_retry(retries_done, len(backoffs))
                    await asyncio.sleep(backoffs[retries_done - 1])
                    continue

                progress_last_step = tracker.progress.last_step if tracker else None
                if tracker:
                    await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}")
                diag_dir = _save_parser_diag(
                    uid,
                    exc=exc,
                    query=query,
                    city=city,
                    area_id=overrides.get("area") if isinstance(overrides.get("area"), int) else None,
                    include=include_list,
                    exclude=exclude_list,
                    amount=None,
                    mode="report",
                    retries=retries_done,
                    error_code=error_info.code,
                    progress_last_step=progress_last_step,
                )
                failure_text = _format_failure_message(
                    invalid_arguments=None,
                    diag_dir=diag_dir,
                    user=user,
                    default_message=error_info.message_for_user,
                )
                await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
                await _send_diagnostic_bundle_if_needed(
                    message.bot,
                    bundle_path=diag_dir.with_suffix(".zip") if diag_dir else None,
                    correlation=None,
                    user_chat_id=getattr(message.chat, "id", None),
                    user_id=getattr(user, "id", None),
                )
                complete_operation(ok=False, err=error_info.code.lower())
                return
    except Exception as e:  # pragma: no cover
        error_info = classify_error(e, getattr(e, "stdout", ""), getattr(e, "stderr", ""))
        log_event(
            "ERROR",
            "parse.error",
            user_id=uid,
            mode="report",
            query=query,
            city=city,
            error_code=error_info.code,
            hint=error_info.hint_for_log,
        )
        if tracker:
            await tracker.fail(
                f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}"
            )
        failure_text = _format_failure_message(
            invalid_arguments=None,
            diag_dir=diag_dir,
            user=user,
            default_message=error_info.message_for_user,
        )
        await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
        complete_operation(ok=False, err=error_info.code.lower())
        return
    else:
        if not result or not result.ok or not (result.xlsx_path and result.xlsx_path.exists()):
            await _send_failure_response(message, result, user=user, tracker=tracker)
        else:
            await _finalize_quota_usage(message, uid, decision)
            _log_parse_ready(query, city, overrides)
            log_event(
                "INFO",
                "parse.ok",
                user_id=uid,
                mode="report",
                query=query,
                city=city,
                path=str(result.xlsx_path),
                duration_ms=result.duration_ms if result else None,
            )
            correlation = None
            if result and result.meta:
                correlation = str(result.meta.get("correlation_id") or "").strip() or None
            report_path = Path(result.xlsx_path)
            chat_id = getattr(message.chat, "id", None)
            user_id = getattr(user, "id", None)
            report_size: int | None = None
            send_error_message: str | None = None
            ack_sent = False
            deliver_status = "fail"
            safe_name, name_ok = _safe_document_name(report_path)
            progress_strategy = tracker.ui_strategy if tracker else "edit"
            diag_snapshot = collect_xlsx_diagnostics(report_path)
            diag_payload = diag_snapshot.to_event_payload()
            if diag_snapshot.size_bytes is not None:
                report_size = diag_snapshot.size_bytes
            send_result: SendReportResult | None = None

            try:
                started_payload = {
                    "correlation_id": correlation,
                    "chat_id": chat_id,
                    "path": str(report_path),
                    "file_name": safe_name if safe_name else report_path.name,
                }
                if report_size is not None:
                    started_payload["size_bytes"] = report_size
                started_payload["diagnostics"] = diag_payload
                if _DIAG_ENABLED:
                    started_payload["cmd_line"] = (result.meta or {}).get("command_line") if result else None
                    started_payload["progress_last_percent"] = (
                        tracker.last_percent if tracker else None
                    )
                log_event(
                    "deliver.started",
                    **{k: v for k, v in started_payload.items() if v is not None},
                )

                if not report_path.exists():
                    raise FileNotFoundError("report_file_missing")

                if report_size is None:
                    report_size = report_path.stat().st_size
                    diag_snapshot = collect_xlsx_diagnostics(report_path)
                    diag_payload = diag_snapshot.to_event_payload()
                    if diag_snapshot.size_bytes is not None:
                        report_size = diag_snapshot.size_bytes

                if report_size <= 0:
                    raise ValueError("report_file_empty")

                if not name_ok:
                    log_event(
                        "deliver.file_name_adjusted",
                        correlation_id=correlation,
                        chat_id=chat_id,
                        original=report_path.name,
                        new=safe_name,
                    )

                diagnostic_path = result.bundle_path if SEND_DIAG_BUNDLES else None
                diagnostic_caption = f"diag {correlation}" if correlation else "diagnostic bundle"

                send_result = await _send_report_with_analytics(
                    message,
                    result.xlsx_path,
                    title=query,
                    city=city,
                    include=overrides.get("include"),
                    exclude=overrides.get("exclude"),
                    reply_markup=_main_menu_kb(message, user=user),
                    diagnostic_path=diagnostic_path,
                    diagnostic_caption=diagnostic_caption if diagnostic_path else None,
                    file_name_override=safe_name,
                )
                ack_sent = True
                send_error_message = send_result.error_message
                if send_result.size is not None:
                    report_size = send_result.size
                if send_result.diagnostics:
                    diag_snapshot = send_result.diagnostics
                    diag_payload = diag_snapshot.to_event_payload()

                if not send_result.ok:
                    log_event(
                        "deliver.send_report_failed",
                        level="ERROR",
                        correlation_id=correlation,
                        chat_id=chat_id,
                        path=str(report_path),
                        size_bytes=send_result.size,
                        error_type=type(send_result.error).__name__
                        if send_result.error
                        else None,
                        error_message=send_result.error_message,
                        diagnostics=diag_payload,
                    )
                    raise RuntimeError(send_result.error_message or "send_report_failed")

                await _cleanup_inline_message(message)
                if tracker:
                    await tracker.finish_success()
                complete_operation(ok=True)
                deliver_status = "ok"
            except Exception as exc:
                deliver_status = "fail"
                if send_result and send_result.diagnostics:
                    diag_snapshot = send_result.diagnostics
                else:
                    diag_snapshot = collect_xlsx_diagnostics(report_path)
                diag_payload = diag_snapshot.to_event_payload()
                stack_full = build_stack(exc)
                stack_lines = [line for line in stack_full.strip().splitlines() if line.strip()]
                stack_short = stack_lines[-1] if stack_lines else type(exc).__name__
                fail_payload = {
                    "correlation_id": correlation,
                    "chat_id": chat_id,
                    "path": str(report_path),
                    "size_bytes": report_size,
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "stack_short": stack_short,
                    "diagnostics": diag_payload,
                }
                if _DIAG_ENABLED:
                    fail_payload["cmd_line"] = (result.meta or {}).get("command_line") if result else None
                    fail_payload["progress_last_percent"] = (
                        tracker.last_percent if tracker else None
                    )
                log_event(
                    "deliver.fail",
                    level="ERROR",
                    **{k: v for k, v in fail_payload.items() if v is not None},
                )

                if send_error_message is None:
                    send_error_message = str(exc)

                if tracker:
                    await tracker.fail(
                        f"{PROGRESS_FAILURE_PREFIX} Ошибка доставки файла"
                    )

                failure_text = "Не получилось… прикладываю диагностику, команда уже уведомлена"
                await message.answer(
                    failure_text,
                    reply_markup=_main_menu_kb(message, user=user),
                )

                bundle_path: Path | None = None
                bundle_correlation: str | None = None
                try:
                    context = DeliverDiagContext(
                        user_id=user_id,
                        username=getattr(user, "username", None),
                        chat_id=chat_id,
                        exception_type=type(exc).__name__,
                        exception_message=str(exc),
                        stack=stack_full,
                        xlsx_path=report_path,
                        xlsx_size=report_size,
                        csv_path=result.csv_path if result else None,
                        stdout_path=result.stdout_path if result else None,
                        stderr_path=result.stderr_path if result else None,
                        cmd_line=(result.meta or {}).get("command_line") if result else None,
                        progress_last_percent=tracker.last_percent if tracker else None,
                        xlsx_diagnostics=diag_payload,
                    )
                    bundle_path = build_diag_bundle(correlation, context)
                    bundle_correlation = _extract_correlation_from_bundle(bundle_path) or correlation
                    remember_bundle(
                        chat_id=chat_id,
                        correlation_id=bundle_correlation,
                        bundle_path=bundle_path,
                    )
                    if bundle_correlation:
                        correlation = bundle_correlation
                except Exception as bundle_exc:
                    log_event(
                        "diagnostic_bundle_failed",
                        level="ERROR",
                        correlation_id=correlation,
                        path=str(report_path),
                        err=str(bundle_exc),
                    )
                else:
                    await _dispatch_deliver_bundle(
                        message.bot,
                        bundle_path=bundle_path,
                        correlation_id=bundle_correlation or correlation,
                        user_chat_id=chat_id,
                        user_id=user_id,
                    )

                complete_operation(ok=False, err="send_report_failed")
            finally:
                _update_ui_meta(
                    result,
                    ack_sent=ack_sent,
                    progress_strategy=progress_strategy,
                    report_path=result.xlsx_path,
                    send_error=send_error_message,
                )
                log_event(
                    "deliver.done",
                    correlation_id=correlation,
                    status="ok" if deliver_status == "ok" else "fail",
                    chat_id=chat_id,
                    diagnostics=diag_payload,
                )
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
    ui_ack_sent: bool | None = None,
    tracker: ReportProgressTracker | None = None,
):
    """Считает pages/per_page под нужный объём total и запускает парсер с блокировкой пользователя."""
    uid = _resolve_requester_id(message, uid)
    if not set_busy(uid):
        if tracker:
            try:
                await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} Уже выполняю другой запрос")
            except Exception:
                pass
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
    tracker_obj: ReportProgressTracker | None = tracker
    result: parser_adapter.RunReportResult | None = None
    diag_dir: Path | None = None
    include_list: list[str] = []
    exclude_list: list[str] = []
    retries_done = 0
    backoffs = (2, 6)

    try:
        decision = await _ensure_quota(
            message,
            uid,
            user=user,
            snapshot=snapshot,
            reason="parse_amount",
        )
        if not decision:
            if tracker_obj:
                try:
                    await tracker_obj.fail(f"{PROGRESS_FAILURE_PREFIX} Недостаточно лимитов")
                except Exception:
                    pass
            return

        include_list = _ensure_str_list(ov.get("include"))
        exclude_list = _ensure_str_list(ov.get("exclude"))
        if tracker_obj is None:
            tracker_obj = await _start_report_progress(message, include_list, exclude_list)
        await tracker_obj.mark_command_ready()
        _log_parse_start(title, city, ov, approx_total=total)
        log_event(
            "INFO",
            "parse.start",
            user_id=uid,
            mode="report",
            query=title,
            city=city,
            amount=total,
        )
        log_event(
            "parse.started",
            user_id=uid,
            query=title,
            city=city,
            amount=total,
            overrides=ov,
        )

        async def _progress(kind: str, payload: dict) -> None:
            if tracker_obj:
                await tracker_obj.handle_event(kind, payload)

        run_kwargs = {k: v for k, v in ov.items() if k not in {"include", "exclude"}}
        while True:
            try:
                if tracker_obj:
                    if retries_done > 0:
                        await tracker_obj.clear_retry()
                        await tracker_obj.mark_command_ready()
                    await tracker_obj.mark_process_started()
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
                log_event(
                    "parse.finished",
                    user_id=uid,
                    ok=result.ok if result else False,
                    duration_ms=result.duration_ms if result else None,
                    cmd_line=(result.meta or {}).get("command_line") if result else None,
                    rows=(result.meta or {}).get("result", {}).get("rows") if result else None,
                    xlsx_path=str(result.xlsx_path) if result and result.xlsx_path else None,
                )
                break
            except parser_adapter.ParserRunError as exc:
                error_info = classify_error(exc, exc.stdout, exc.stderr)
                log_event(
                    "ERROR",
                    "parse.error",
                    user_id=uid,
                    mode="report",
                    query=title,
                    city=city,
                    amount=total,
                    error_code=error_info.code,
                    hint=error_info.hint_for_log,
                    attempt=retries_done + 1,
                )
                if is_retryable(error_info.code) and retries_done < len(backoffs):
                    retries_done += 1
                    if tracker_obj:
                        await tracker_obj.show_retry(retries_done, len(backoffs))
                    await asyncio.sleep(backoffs[retries_done - 1])
                    continue

                progress_last_step = (
                    tracker_obj.progress.last_step if tracker_obj else None
                )
                if tracker_obj:
                    await tracker_obj.fail(
                        f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}"
                    )
                diag_dir = _save_parser_diag(
                    uid,
                    exc=exc,
                    query=title,
                    city=city,
                    area_id=area_id,
                    include=include_list,
                    exclude=exclude_list,
                    amount=total,
                    mode="report",
                    retries=retries_done,
                    error_code=error_info.code,
                    progress_last_step=progress_last_step,
                )
                failure_text = _format_failure_message(
                    invalid_arguments=None,
                    diag_dir=diag_dir,
                    user=user,
                    default_message=error_info.message_for_user,
                )
                await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
                await _send_diagnostic_bundle_if_needed(
                    message.bot,
                    bundle_path=diag_dir.with_suffix(".zip") if diag_dir else None,
                    correlation=None,
                    user_chat_id=getattr(message.chat, "id", None),
                    user_id=getattr(user, "id", None),
                )
                log_event(
                    "parse.finished",
                    user_id=uid,
                    ok=False,
                    duration_ms=None,
                    cmd_line=" ".join(str(part) for part in getattr(exc, "cmd", []) or []).strip() or None,
                    rows=None,
                    xlsx_path=None,
                )
                complete_operation(ok=False, err=error_info.code.lower())
                return
    except Exception as e:
        error_info = classify_error(e, getattr(e, "stdout", ""), getattr(e, "stderr", ""))
        log_event(
            "ERROR",
            "parse.error",
            user_id=uid,
            mode="report",
            query=title,
            city=city,
            amount=total,
            error_code=error_info.code,
            hint=error_info.hint_for_log,
        )
        if tracker_obj:
            await tracker_obj.fail(
                f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}"
            )
        failure_text = _format_failure_message(
            invalid_arguments=None,
            diag_dir=None,
            user=user,
            default_message=error_info.message_for_user,
        )
        await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
        log_event(
            "parse.finished",
            user_id=uid,
            ok=False,
            duration_ms=None,
            cmd_line=" ".join(str(part) for part in getattr(e, "cmd", []) or []).strip() or None,
            rows=None,
            xlsx_path=None,
        )
        complete_operation(ok=False, err=error_info.code.lower())
        return
    else:
        if not result or not result.ok or not (result.xlsx_path and result.xlsx_path.exists()):
            await _send_failure_response(message, result, user=user, tracker=tracker)
        else:
            if decision:
                await _finalize_quota_usage(message, uid, decision)
            _log_parse_ready(title, city, ov, approx_total=total)
            log_event(
                "INFO",
                "parse.ok",
                user_id=uid,
                mode="report",
                query=title,
                city=city,
                amount=total,
                path=str(result.xlsx_path),
                duration_ms=result.duration_ms if result else None,
            )
            correlation = None
            if result and result.meta:
                correlation = str(result.meta.get("correlation_id") or "").strip() or None
            diagnostic_path = result.bundle_path if SEND_DIAG_BUNDLES else None
            diagnostic_caption = f"diag {correlation}" if correlation else "diagnostic bundle"
            chat_id = getattr(message.chat, "id", None)
            send_result = await _send_report_with_analytics(
                message,
                result.xlsx_path,
                title=title,
                city=city,
                approx_total=total,
                include=ov.get("include"),
                exclude=ov.get("exclude"),
                reply_markup=_main_menu_kb(message, user=user),
                diagnostic_path=diagnostic_path,
                diagnostic_caption=diagnostic_caption if diagnostic_path else None,
            )
            progress_strategy = tracker_obj.ui_strategy if tracker_obj else "edit"
            ack_for_meta = ui_ack_sent if ui_ack_sent is not None else True
            _update_ui_meta(
                result,
                ack_sent=ack_for_meta,
                progress_strategy=progress_strategy,
                report_path=result.xlsx_path,
                send_error=send_result.error_message,
            )
            diag_payload = (
                send_result.diagnostics.to_event_payload()
                if send_result.diagnostics
                else collect_xlsx_diagnostics(Path(result.xlsx_path)).to_event_payload()
            )
            log_event(
                "deliver.done",
                correlation_id=correlation,
                status="ok" if send_result.ok else "fail",
                chat_id=chat_id,
                diagnostics=diag_payload,
                size_bytes=send_result.size,
            )
            if send_result.ok:
                await _cleanup_inline_message(message)
                if tracker_obj:
                    await tracker_obj.finish_success()
                complete_operation(ok=True)
            else:
                log_event(
                    "deliver.send_report_failed",
                    level="ERROR",
                    correlation_id=correlation,
                    chat_id=chat_id,
                    path=str(result.xlsx_path),
                    size_bytes=send_result.size,
                    error_type=type(send_result.error).__name__
                    if send_result.error
                    else None,
                    error_message=send_result.error_message,
                    diagnostics=diag_payload,
                )
                failure_text = (
                    "Не получилось отправить файл. Я приложил диагностический ZIP и уведомил поддержку."
                )
                if tracker_obj:
                    await tracker_obj.fail(
                        f"{PROGRESS_FAILURE_PREFIX} Не удалось отправить файл"
                    )
                await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
                await _send_diagnostic_bundle_if_needed(
                    message.bot,
                    bundle_path=result.bundle_path,
                    correlation=correlation,
                    user_chat_id=getattr(message.chat, "id", None),
                    user_id=getattr(user, "id", None),
                )
                complete_operation(ok=False, err="send_report_failed")
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

    overrides = overrides or {}
    _, normalized_overrides, _, _ = (
        parser_adapter.normalize_and_validate_overrides(overrides)
    )

    clean_overrides: dict[str, object] = {}
    for key in ("include", "exclude", "pages", "per_page", "pause", "site", "area"):
        if key in normalized_overrides:
            value = normalized_overrides[key]
            if value is not None or key in {"include", "exclude"}:
                clean_overrides[key] = value
    overrides = clean_overrides

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
        await ParseForm.waiting_query.set()
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
        tracker: ReportProgressTracker | None = None
        result: parser_adapter.RunReportResult | None = None
        diag_dir: Path | None = None
        retries_done = 0
        backoffs = (2, 6)
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
            await tracker.mark_command_ready()
            _log_parse_start(norm_title, city_to_use, ov)

            async def _progress(kind: str, payload: dict) -> None:
                if tracker:
                    await tracker.handle_event(kind, payload)

            run_kwargs = {k: v for k, v in ov.items() if k not in {"include", "exclude"}}
            while True:
                try:
                    if tracker:
                        if retries_done > 0:
                            await tracker.clear_retry()
                            await tracker.mark_command_ready()
                        await tracker.mark_process_started()
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
                    break
                except parser_adapter.ParserRunError as exc:
                    error_info = classify_error(exc, exc.stdout, exc.stderr)
                    log_event(
                        "ERROR",
                        "parse.error",
                        user_id=requester_id,
                        mode="report",
                        query=norm_title,
                        city=city_to_use,
                        error_code=error_info.code,
                        hint=error_info.hint_for_log,
                        attempt=retries_done + 1,
                    )
                    if is_retryable(error_info.code) and retries_done < len(backoffs):
                        retries_done += 1
                        if tracker:
                            await tracker.show_retry(retries_done, len(backoffs))
                        await asyncio.sleep(backoffs[retries_done - 1])
                        continue

                    progress_last_step = tracker.progress.last_step if tracker else None
                    if tracker:
                        await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}")
                    diag_dir = _save_parser_diag(
                        requester_id,
                        exc=exc,
                        query=norm_title,
                        city=city_to_use,
                        area_id=ov.get("area") if isinstance(ov.get("area"), int) else None,
                        include=include_list,
                        exclude=exclude_list,
                        amount=None,
                        mode="report",
                        retries=retries_done,
                        error_code=error_info.code,
                        progress_last_step=progress_last_step,
                    )
                    failure_text = _format_failure_message(
                        invalid_arguments=None,
                        diag_dir=diag_dir,
                        user=user,
                        default_message=error_info.message_for_user,
                    )
                    await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
                    await _send_diagnostic_bundle_if_needed(
                        message.bot,
                        bundle_path=diag_dir.with_suffix(".zip") if diag_dir else None,
                        correlation=None,
                        user_chat_id=getattr(message.chat, "id", None),
                        user_id=getattr(user, "id", None),
                    )
                    complete_operation(ok=False, err=error_info.code.lower())
                    return
        except Exception as e:
            error_info = classify_error(e, getattr(e, "stdout", ""), getattr(e, "stderr", ""))
            log_event(
                "ERROR",
                "parse.error",
                user_id=requester_id,
                mode="report",
                query=norm_title,
                city=city_to_use,
                error_code=error_info.code,
                hint=error_info.hint_for_log,
            )
            if tracker:
                await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}")
            await message.answer(
                error_info.message_for_user,
                reply_markup=_main_menu_kb(message, user=user),
            )
            complete_operation(ok=False, err=error_info.code.lower())
            return
        else:
            if not result or not result.ok or not (result.xlsx_path and result.xlsx_path.exists()):
                await _send_failure_response(message, result, user=user, tracker=tracker)
            else:
                if decision:
                    await _finalize_quota_usage(message, requester_id, decision)
                _log_parse_ready(norm_title, city_to_use, ov)
                correlation = None
                if result and result.meta:
                    correlation = str(result.meta.get("correlation_id") or "").strip() or None
                diagnostic_path = result.bundle_path if SEND_DIAG_BUNDLES else None
                diagnostic_caption = f"diag {correlation}" if correlation else "diagnostic bundle"
                chat_id = getattr(message.chat, "id", None)
                send_result = await _send_report_with_analytics(
                    message,
                    result.xlsx_path,
                    title=norm_title,
                    city=city_to_use,
                    include=ov.get("include"),
                    exclude=ov.get("exclude"),
                    reply_markup=_main_menu_kb(message, user=user),
                    diagnostic_path=diagnostic_path,
                    diagnostic_caption=diagnostic_caption if diagnostic_path else None,
                )
                progress_strategy = tracker.ui_strategy if tracker else "edit"
                _update_ui_meta(
                    result,
                    ack_sent=True,
                    progress_strategy=progress_strategy,
                    report_path=result.xlsx_path,
                    send_error=send_result.error_message,
                )
                diag_payload = (
                    send_result.diagnostics.to_event_payload()
                    if send_result.diagnostics
                    else collect_xlsx_diagnostics(Path(result.xlsx_path)).to_event_payload()
                )
                log_event(
                    "deliver.done",
                    correlation_id=correlation,
                    status="ok" if send_result.ok else "fail",
                    chat_id=chat_id,
                    diagnostics=diag_payload,
                    size_bytes=send_result.size,
                )
                if send_result.ok:
                    await _cleanup_inline_message(message)
                    if tracker:
                        await tracker.finish_success()
                    complete_operation(ok=True)
                else:
                    log_event(
                        "deliver.send_report_failed",
                        level="ERROR",
                        correlation_id=correlation,
                        chat_id=chat_id,
                        path=str(result.xlsx_path),
                        size_bytes=send_result.size,
                        error_type=type(send_result.error).__name__
                        if send_result.error
                        else None,
                        error_message=send_result.error_message,
                        diagnostics=diag_payload,
                    )
                    failure_text = (
                        "Не получилось отправить файл. Я приложил диагностический ZIP и уведомил поддержку."
                    )
                    if tracker:
                        await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} Не удалось отправить файл")
                    await message.answer(failure_text, reply_markup=_main_menu_kb(message, user=user))
                    await _send_diagnostic_bundle_if_needed(
                        message.bot,
                        bundle_path=result.bundle_path,
                        correlation=correlation,
                        user_chat_id=getattr(message.chat, "id", None),
                        user_id=getattr(user, "id", None),
                    )
                    complete_operation(ok=False, err="send_report_failed")
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
        log_event(
            "parse_dialog_start",
            command="/parse",
            args=_build_args(query, city, overrides),
        )
        await _run_parser(message, query, city, overrides)
        return

    update_context(command="parse_dialog")
    log_event("parse_dialog_start", command="parse_dialog")
    prompt = await message.answer("Введите должность:", reply_markup=ReplyKeyboardRemove())
    await ParseForm.waiting_query.set()
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
        await call.answer()
        return

    if is_busy(call.from_user.id):
        await call.answer(BUSY_TEXT, show_alert=False)
        return

    token = payload.get("token")
    session = chips.get_session(token or "")
    if (
        not token
        or session is None
        or session.kind != kind
        or not chips.is_active(call.from_user.id, session.kind, session.token)
    ):
        await call.answer("Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    action = payload.get("action")
    if action == "more":
        markup = chips.change_page(session, 1)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "more", "control", position=None, action="more")
        await call.answer()
        return

    if action == "prev":
        markup = chips.change_page(session, -1)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "prev", "control", position=None, action="prev")
        await call.answer()
        return

    if action == "category":
        try:
            category_index = int(payload.get("value", "-1"))
        except ValueError:
            await call.answer("Эта клавиатура устарела. Начните заново.", show_alert=False)
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
        await call.answer()
        return

    if action == "back":
        markup = chips.back_to_categories(session)
        try:
            await call.message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            pass
        chips.log_click(session.kind, "back", "control", position=None, action="back")
        await call.answer()
        return

    if action == "random":
        if session.kind != "role":
            await call.answer()
            return
        value = chips.random_role()
        if not value:
            await call.answer("Нет доступных вариантов", show_alert=False)
            return
        chips.log_click("role", value, "base", position=None, action="random")
        chips.finish_session(call.from_user.id, "role")
        try:
            await call.message.edit_reply_markup()
        except (MessageCantBeEdited, MessageNotModified):
            pass
        await call.answer(f"Выбрано: {value}")
        await _handle_role_value(call.message, state, value, user_id=call.from_user.id)
        return

    if action != "pick":
        await call.answer()
        return

    try:
        index = int(payload.get("value", "-1"))
    except ValueError:
        await call.answer("Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    candidate = chips.resolve_candidate(session, index)
    if not candidate:
        await call.answer("Эта клавиатура устарела. Начните заново.", show_alert=False)
        return

    chips.log_click(session.kind, candidate.value, candidate.source, position=index + 1)
    chips.finish_session(call.from_user.id, session.kind)
    try:
        await call.message.edit_reply_markup()
    except (MessageCantBeEdited, MessageNotModified):
        pass
    await call.answer(f"Выбрано: {candidate.value}")

    if session.kind == "role":
        await _handle_role_value(call.message, state, candidate.value, user_id=call.from_user.id)
    else:
        await _handle_city_value(call.message, state, candidate.value, user_id=call.from_user.id)


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
    uid = call.from_user.id

    if _is_report_job_active(uid):
        ack_started = time.monotonic()
        try:
            await call.answer(BUSY_TEXT, show_alert=False, cache_time=1)
            log_event(
                "callback_ack_sent",
                callback="cb_qty_dedup",
                status="ok",
                latency_ms=int((time.monotonic() - ack_started) * 1000),
            )
        except Exception as exc:  # pragma: no cover - best effort logging
            stack = traceback.format_exc()
            log_event(
                "callback_ack_sent",
                callback="cb_qty_dedup",
                status="err",
                latency_ms=int((time.monotonic() - ack_started) * 1000),
                err=str(exc),
                stack=stack,
            )
            log_event("callback_ack_failed", callback="cb_qty_dedup", err=str(exc), stack=stack)
        log_event("job.deduped", callback="cb_qty", user_id=uid)
        return

    # если занят — просто подсказка и выходим
    if is_busy(uid):
        await call.answer(BUSY_TEXT, show_alert=False)
        return

    ack_started = time.monotonic()
    ack_sent = False
    ack_stack: str | None = None
    try:
        await call.answer(
            "Запускаю выгрузку… это займёт пару минут",
            show_alert=False,
            cache_time=1,
        )
        ack_sent = True
        log_event(
            "callback_ack_sent",
            callback="cb_qty",
            status="ok",
            latency_ms=int((time.monotonic() - ack_started) * 1000),
        )
    except Exception as exc:
        ack_stack = traceback.format_exc()
        log_event(
            "callback_ack_sent",
            callback="cb_qty",
            status="err",
            latency_ms=int((time.monotonic() - ack_started) * 1000),
            err=str(exc),
            stack=ack_stack,
        )
        log_event("callback_ack_failed", callback="cb_qty", err=str(exc), stack=ack_stack)

    payload = _PENDING_QTY.get(uid)
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

    include_list = _ensure_str_list((overrides or {}).get("include"))
    exclude_list = _ensure_str_list((overrides or {}).get("exclude"))

    try:
        tracker = await _start_report_progress(call.message, include_list, exclude_list)
    except Exception as exc:
        log_event(
            "progress.start_failed",
            level="ERROR",
            err=str(exc),
            callback="cb_qty",
        )
        tracker = None

    # фикс: после старта выгрузки «забываем» pending, чтобы старые кнопки не плодили ошибки
    _PENDING_QTY.pop(uid, None)
    update_context(dialog_step=_dialog_step("qty", str(total)))
    log_event(
        "qty_chosen",
        action=f"qty_{total}",
        quantity=total,
        args=_build_args(title, city, overrides, qty=total),
    )

    async def _job() -> None:
        try:
            await _run_with_amount(
                call.message,
                title,
                city,
                area_id,
                overrides,
                total,
                uid=uid,
                user=call.from_user,
                ui_ack_sent=ack_sent,
                tracker=tracker,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            stack = traceback.format_exc()
            log_event(
                "report.job_failed",
                level="ERROR",
                err=str(exc),
                stack=stack,
                user_id=uid,
            )
            if tracker:
                try:
                    await tracker.fail(f"{PROGRESS_FAILURE_PREFIX} Внутренняя ошибка")
                except Exception:
                    pass

    task = asyncio.create_task(_job(), name=f"report:{uid}:{total}")
    _register_report_job(uid, task)


async def cb_preview(call: types.CallbackQuery):
    """Показать 5 первых совпадений без уничтожения кеша запроса."""
    uid = call.from_user.id
    if not set_busy(uid):
        await call.answer(BUSY_TEXT, show_alert=False)
        return

    ack_started = time.monotonic()
    try:
        await call.answer("Готовлю превью…", show_alert=False, cache_time=1)
        log_event(
            "callback_ack_sent",
            callback="cb_preview",
            status="ok",
            latency_ms=int((time.monotonic() - ack_started) * 1000),
        )
    except Exception as exc:
        log_event(
            "callback_ack_sent",
            callback="cb_preview",
            status="err",
            latency_ms=int((time.monotonic() - ack_started) * 1000),
            err=str(exc),
        )
        log_event("callback_ack_failed", callback="cb_preview", err=str(exc))

    progress: Progress | None = None
    invalid_arguments: str | None = None
    try:
        payload = _PENDING_QTY.get(uid)   # ВАЖНО: .get(), НЕ .pop()!
        if not payload:
            await call.message.answer("Не нашёл предыдущий запрос. Введи должность ещё раз:")
            await ParseForm.waiting_query.set()
            return

        title, city, area_id, overrides, _max_total = payload
        overrides = overrides or {}
        invalid_keys: list[str] = []
        if not (title or "").strip():
            invalid_keys.append("query")
        if not (city or "").strip():
            invalid_keys.append("city")

        override_payload: dict[str, object] = {
            "include": overrides.get("include"),
            "exclude": overrides.get("exclude"),
        }
        area_source = overrides.get("area", area_id)
        if area_source is not None:
            override_payload["area"] = area_source

        _ok_overrides, normalized_overrides, invalid_override_keys, _ = (
            parser_adapter.normalize_and_validate_overrides(override_payload)
        )
        invalid_keys.extend(invalid_override_keys)
        if invalid_keys:
            log_event(
                "preview_validation_failed",
                level="WARN",
                invalid_arguments=invalid_keys,
                args=_build_args(title, city, overrides),
            )
            invalid_arguments = parser_adapter.format_invalid_arguments(invalid_keys)
            await call.message.answer(invalid_arguments)
            return

        include = normalized_overrides.get("include", [])
        exclude = normalized_overrides.get("exclude", [])
        area_norm = normalized_overrides.get("area")
        area_id = area_norm if area_norm is not None else area_source

        clean_overrides = dict(overrides)
        clean_overrides["include"] = include
        clean_overrides["exclude"] = exclude
        if area_id is not None:
            clean_overrides["area"] = area_id
        elif "area" in clean_overrides:
            clean_overrides.pop("area", None)
        overrides = clean_overrides

        if not ALLOW_FREE_PREVIEW:
            snapshot = paywall.SavedRequest(
                kind="preview",
                query=title,
                city=city,
                overrides=overrides,
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

        progress = await Progress.create(
            call.message.bot,
            call.message.chat.id,
            PROGRESS_STEPS,
            mode="preview",
            initial_step=PROGRESS_STEPS[0].name,
        )
        await progress.set("fetch", 10, force=True)
        _log_preview_start(title, city, overrides)
        log_event(
            "INFO",
            "preview.start",
            user_id=uid,
            query=title,
            city=city,
            area_id=area_id,
        )

        backoffs = (2, 6)
        retries_done = 0
        diag_dir: Path | None = None
        rows: list[dict[str, str]] | None = None
        while True:
            try:
                if retries_done > 0:
                    await progress.clear_retry()
                    await progress.set("fetch", 10, force=True)
                await progress.set("fetch", 30, force=True)
                rows = await parser_adapter.preview_rows(
                    uid,
                    title,
                    city,
                    area=area_id,
                    include=include,
                    exclude=exclude,
                )
                break
            except parser_adapter.ParserRunError as exc:
                error_info = classify_error(exc, exc.stdout, exc.stderr)
                _log_preview_timeout(title, city, overrides, err=error_info.hint_for_log)
                log_event(
                    "ERROR",
                    "preview.error",
                    user_id=uid,
                    query=title,
                    city=city,
                    error_code=error_info.code,
                    hint=error_info.hint_for_log,
                    attempt=retries_done + 1,
                )
                if is_retryable(error_info.code) and retries_done < len(backoffs):
                    retries_done += 1
                    await progress.show_retry(retries_done, len(backoffs))
                    await asyncio.sleep(backoffs[retries_done - 1])
                    continue

                progress_last_step = progress.last_step
                diag_dir = _save_parser_diag(
                    uid,
                    exc=exc,
                    query=title,
                    city=city,
                    area_id=area_id if isinstance(area_id, int) else None,
                    include=include,
                    exclude=exclude,
                    amount=parser_adapter.PREVIEW_ROWS,
                    mode="preview",
                    retries=retries_done,
                    error_code=error_info.code,
                    progress_last_step=progress_last_step,
                )
                await progress.close(
                    ok=False,
                    text=f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}",
                )
                failure_text = _format_failure_message(
                    invalid_arguments=invalid_arguments,
                    diag_dir=diag_dir,
                    user=call.from_user,
                    default_message=error_info.message_for_user,
                )
                await call.message.answer(failure_text)
                await _send_diagnostic_bundle_if_needed(
                    call.message.bot,
                    bundle_path=diag_dir.with_suffix(".zip") if diag_dir else None,
                    correlation=None,
                    user_chat_id=getattr(call.message.chat, "id", None),
                    user_id=getattr(call.from_user, "id", None),
                )
                return
            except validator.ValidationError as exc:
                await progress.close(
                    ok=False,
                    text=f"{PROGRESS_FAILURE_PREFIX} {exc.user_message}",
                )
                await call.message.answer(exc.user_message)
                return
            except Exception as exc:
                error_info = classify_error(exc, getattr(exc, "stdout", ""), getattr(exc, "stderr", ""))
                _log_preview_timeout(title, city, overrides, err=error_info.hint_for_log)
                log_event(
                    "ERROR",
                    "preview.error",
                    user_id=uid,
                    query=title,
                    city=city,
                    error_code=error_info.code,
                    hint=error_info.hint_for_log,
                )
                await progress.close(
                    ok=False,
                    text=f"{PROGRESS_FAILURE_PREFIX} {error_info.message_for_user}",
                )
                failure_text = _format_failure_message(
                    invalid_arguments=invalid_arguments,
                    diag_dir=diag_dir,
                    user=call.from_user,
                    default_message=error_info.message_for_user,
                )
                await call.message.answer(failure_text)
                await _send_diagnostic_bundle_if_needed(
                    call.message.bot,
                    bundle_path=diag_dir.with_suffix(".zip") if diag_dir else None,
                    correlation=None,
                    user_chat_id=getattr(call.message.chat, "id", None),
                    user_id=getattr(call.from_user, "id", None),
                )
                return

        if not rows:
            user_msg = user_message_for_no_data()
            await progress.close(
                ok=False,
                text=f"{PROGRESS_FAILURE_PREFIX} {user_msg}",
            )
            await call.message.answer(user_msg)
            _log_preview_ready(title, city, 0, overrides)
            log_event(
                "INFO",
                "preview.ok",
                user_id=uid,
                query=title,
                city=city,
                count=0,
                error_code="NO_DATA",
            )
            return

        await progress.set("normalize", 60)

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

        await progress.set("write_xlsx", 90)

        txt = "<b>Предпросмотр (первые совпадения):</b>\n" + "\n".join(lines)
        await call.message.answer(txt, disable_web_page_preview=True)
        log_event(
            "INFO",
            "preview.ok",
            user_id=uid,
            query=title,
            city=city,
            count=len(rows),
        )
        _log_preview_ready(title, city, len(rows), overrides)
        await progress.close(ok=True, text=PROGRESS_SUCCESS_TEXT, delete_after=8.0)
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


async def cb_resume_yes(call: types.CallbackQuery):
    await call.answer()
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
            ui_ack_sent=True,
        )
    elif request.kind == "direct":
        await _run_parser(
            call.message,
            request.query,
            request.city,
            request.overrides,
            uid=call.from_user.id,
            user=call.from_user,
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
    await call.answer()
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
    dp.register_callback_query_handler(cb_diag_bundle, lambda c: c.data and c.data.startswith("diag_bundle:"), state="*")
    dp.register_callback_query_handler(cb_resume_yes,  lambda c: c.data == "resume:last", state="*")
    dp.register_callback_query_handler(cb_resume_skip, lambda c: c.data == "resume:skip", state="*")
