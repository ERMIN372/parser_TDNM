from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from aiogram import Bot, types
from aiogram.types import InputFile
from aiogram.utils.exceptions import NetworkError, TelegramAPIError

from app.utils.logging import log_event

MAX_TELEGRAM_FILE_SIZE = 45 * 1024 * 1024


@dataclass
class SendReportResult:
    ok: bool
    message: Optional[types.Message]
    error: Optional[BaseException]
    error_message: Optional[str]
    size: Optional[int]


async def send_report(
    bot: Bot,
    chat_id: int,
    path: Path | str,
    *,
    caption: str | None = None,
    reply_markup=None,
    diagnostic_path: Path | None = None,
    diagnostic_caption: str | None = None,
    file_name: str | None = None,
) -> SendReportResult:
    """Send a report file to Telegram with diagnostics and structured logging."""

    started = time.monotonic()
    report_path = Path(path)
    try:
        stat = report_path.stat()
        size = stat.st_size
    except OSError as exc:
        error_message = str(exc)
        log_event(
            "report.send_fail",
            level="ERROR",
            stage="stat",
            path=str(report_path),
            chat_id=chat_id,
            **_exception_payload(exc),
        )
        return SendReportResult(False, None, exc, error_message, None)

    log_event(
        "report.send_prepare",
        path=str(report_path),
        chat_id=chat_id,
        size=size,
        file_name=file_name or report_path.name,
        exists=True,
        suffix=report_path.suffix,
        modified_ts=int(stat.st_mtime),
    )

    if size <= 0:
        log_event(
            "report.send_fail",
            level="ERROR",
            stage="validate",
            error_type="EmptyFile",
            message="file_is_empty",
            size=size,
            path=str(report_path),
            chat_id=chat_id,
        )
        return SendReportResult(False, None, None, "file_is_empty", size)

    try:
        with report_path.open("rb") as src:
            resolved_name = _resolve_file_name(report_path, file_name)
            log_event(
                "report.send_attempt",
                path=str(report_path),
                chat_id=chat_id,
                file_name=resolved_name,
                size=size,
            )
            input_file = InputFile(src, filename=resolved_name)
            message = await bot.send_document(
                chat_id,
                input_file,
                caption=caption,
                reply_markup=reply_markup,
            )
    except (TelegramAPIError, NetworkError, TypeError, TimeoutError) as exc:
        error_message = str(exc)
        log_event(
            "report.send_fail",
            level="ERROR",
            stage="send",
            path=str(report_path),
            chat_id=chat_id,
            size=size,
            file_name=file_name or report_path.name,
            **_exception_payload(exc),
        )
        if size and size > MAX_TELEGRAM_FILE_SIZE:
            log_event(
                "report.send_too_big",
                level="WARN",
                path=str(report_path),
                chat_id=chat_id,
                size=size,
            )
            await _notify_file_too_big(
                bot,
                chat_id,
                diagnostic_path=diagnostic_path,
                diagnostic_caption=diagnostic_caption,
            )
        return SendReportResult(False, None, exc, error_message, size)
    except Exception as exc:  # pragma: no cover - defensive fallback
        error_message = str(exc)
        log_event(
            "report.send_fail",
            level="ERROR",
            stage="send",
            path=str(report_path),
            chat_id=chat_id,
            size=size,
            file_name=file_name or report_path.name,
            **_exception_payload(exc),
        )
        return SendReportResult(False, None, exc, error_message, size)

    duration_ms = int((time.monotonic() - started) * 1000)
    log_event(
        "report.send_ok",
        duration_ms=duration_ms,
        size=size,
        file_name=file_name or report_path.name,
        path=str(report_path),
        chat_id=chat_id,
    )
    return SendReportResult(True, message, None, None, size)


def _exception_payload(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }
    if isinstance(exc, TelegramAPIError):
        payload.setdefault("status_code", getattr(exc, "status_code", None))
        description = getattr(exc, "message", None) or getattr(exc, "description", None)
        if description:
            payload.setdefault("description", description)
        parameters = getattr(exc, "parameters", None)
        if parameters:
            parameters_data: Any
            if hasattr(parameters, "to_python"):
                parameters_data = parameters.to_python()
            elif hasattr(parameters, "model_dump"):
                parameters_data = parameters.model_dump()
            else:
                parameters_data = str(parameters)
            payload.setdefault("parameters", parameters_data)
    if isinstance(exc, NetworkError):
        payload.setdefault("timeout", getattr(exc, "timeout", None))
    args = getattr(exc, "args", None)
    if args:
        payload.setdefault("args", list(args))
    return {k: v for k, v in payload.items() if v not in (None, "")}


def _resolve_file_name(report_path: Path, override: str | None) -> str:
    candidate = (override or report_path.name or "").strip()
    if not candidate or candidate in {".", ".."}:
        return _fallback(report_path)
    if any(sep in candidate for sep in ("/", "\\", "\n", "\r", "\t")):
        return _fallback(report_path)
    if len(candidate) > 120:
        return _fallback(report_path)
    return candidate


def _fallback(path: Path) -> str:
    return "report.csv" if path.suffix.lower() == ".csv" else "report.xlsx"


async def _notify_file_too_big(
    bot: Bot,
    chat_id: int,
    *,
    diagnostic_path: Path | None,
    diagnostic_caption: str | None,
) -> None:
    try:
        await bot.send_message(
            chat_id,
            "Файл слишком большой для Telegram. Я приложил диагностический ZIP и передал информацию поддержке. Напиши нам, чтобы получить ссылку на скачивание отчёта.",
        )
    except Exception as exc:  # pragma: no cover - notification best effort
        log_event(
            "send_report.notify_failed",
            level="WARN",
            err=str(exc),
            reason="file_too_big",
        )

    if diagnostic_path and diagnostic_path.exists():
        try:
            with diagnostic_path.open("rb") as diag_src:
                diag_file = InputFile(diag_src, filename=diagnostic_path.name)
                await bot.send_document(chat_id, diag_file, caption=diagnostic_caption)
            log_event(
                "send_report.diag_shared",
                path=str(diagnostic_path),
            )
        except Exception as exc:  # pragma: no cover - diagnostics best effort
            log_event(
                "send_report.diag_failed",
                level="WARN",
                err=str(exc),
                path=str(diagnostic_path),
            )
