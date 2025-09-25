from __future__ import annotations

import asyncio
from dataclasses import dataclass

TIMEOUT_MESSAGE = "⏱️ Площадка отвечает медленно. Попробуйте ещё раз через пару минут."
REMOTE_502_MESSAGE = "⚠️ На стороне площадки временная ошибка. Обычно проходит быстро."
REMOTE_BLOCK_MESSAGE = "🛡️ Площадка временно ограничила запросы. Подождите 10–15 мин."
NO_DATA_MESSAGE = "Ничего не нашлось по такому запросу. Попробуйте изменить формулировку."
INVALID_ARGS_MESSAGE = "Не получилось запустить поиск: проверьте параметры. Их можно исправить кнопкой «Назад»."
PIPELINE_ERROR_MESSAGE = "Внутренняя ошибка формирования отчёта. Мы уже разбираемся."
UNKNOWN_MESSAGE = "Не получилось… Попробуйте позже."


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message_for_user: str
    hint_for_log: str


_CODE_TO_MESSAGE = {
    "TIMEOUT": TIMEOUT_MESSAGE,
    "REMOTE_502": REMOTE_502_MESSAGE,
    "REMOTE_BLOCK": REMOTE_BLOCK_MESSAGE,
    "NO_DATA": NO_DATA_MESSAGE,
    "INVALID_ARGS": INVALID_ARGS_MESSAGE,
    "PIPELINE_ERROR": PIPELINE_ERROR_MESSAGE,
    "UNKNOWN": UNKNOWN_MESSAGE,
}

_RETRYABLE_CODES = {"TIMEOUT", "REMOTE_502", "REMOTE_BLOCK"}
_REMOTE_502_MARKERS = ("502", "bad gateway", "upstream")
_REMOTE_BLOCK_MARKERS = ("captcha", "forbidden", "blocked", "too many requests")


def is_retryable(code: str) -> bool:
    return code in _RETRYABLE_CODES


def classify_error(exc: Exception, stdout: str | None, stderr: str | None) -> ErrorInfo:
    stdout = stdout or ""
    stderr = stderr or ""
    combined = f"{stdout}\n{stderr}".lower()
    rc = getattr(exc, "returncode", None)

    if isinstance(exc, asyncio.TimeoutError) or getattr(exc, "timeout", False):
        return ErrorInfo("TIMEOUT", TIMEOUT_MESSAGE, f"timeout rc={rc}")
    if rc == -1:
        return ErrorInfo("TIMEOUT", TIMEOUT_MESSAGE, f"rc=-1")

    for marker in _REMOTE_502_MARKERS:
        if marker in combined:
            return ErrorInfo("REMOTE_502", REMOTE_502_MESSAGE, f"rc={rc} marker={marker}")

    for marker in _REMOTE_BLOCK_MARKERS:
        if marker in combined:
            return ErrorInfo("REMOTE_BLOCK", REMOTE_BLOCK_MESSAGE, f"rc={rc} marker={marker}")

    if rc not in (0, None):
        return ErrorInfo("PIPELINE_ERROR", PIPELINE_ERROR_MESSAGE, f"rc={rc}")

    return ErrorInfo("UNKNOWN", UNKNOWN_MESSAGE, f"type={exc.__class__.__name__}")


def user_message_for_no_data() -> str:
    return NO_DATA_MESSAGE


def user_message_for_invalid_args() -> str:
    return INVALID_ARGS_MESSAGE


def message_for_code(code: str) -> str:
    return _CODE_TO_MESSAGE.get(code, UNKNOWN_MESSAGE)
