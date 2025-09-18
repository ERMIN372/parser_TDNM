from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import sys
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import Logger
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

EMAIL_RE = re.compile(r"(?P<user>[A-Z0-9._%+-]+)@(?P<domain>[A-Z0-9.-]+\.[A-Z]{2,})", re.I)
PHONE_RE = re.compile(r"\+?\d[\d .\-()]{5,}\d")

MAX_PREVIEW = 300
MAX_RAW_TEXT = 2048


def _mask_text(value: str | None) -> str | None:
    if not value:
        return value
    masked = EMAIL_RE.sub("[masked_email]", value)
    masked = PHONE_RE.sub("[masked_phone]", masked)
    return masked


def _truncate(value: str | None, *, limit: int) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _iso_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class OperationContext:
    correlation_id: str
    started_at: float = field(default_factory=time.perf_counter)
    user_id: int | None = None
    chat_id: int | None = None
    username: str | None = None
    full_name: str | None = None
    is_admin: bool | None = None
    update_type: str | None = None
    user_message_raw: str | None = None
    command: str | None = None
    args: Dict[str, Any] | None = None
    dialog_step: str | None = None
    bot_reply_type: str | None = None
    bot_reply_preview: str | None = None
    document_meta: Dict[str, Any] | None = None
    quota: Dict[str, Any] | None = None
    credits_delta: int | None = None
    payment: Dict[str, Any] | None = None
    ok: bool | None = None
    err: str | None = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for key in (
            "user_id",
            "chat_id",
            "username",
            "full_name",
            "is_admin",
            "update_type",
            "user_message_raw",
            "command",
            "args",
            "dialog_step",
            "bot_reply_type",
            "bot_reply_preview",
            "document_meta",
            "quota",
            "credits_delta",
            "payment",
            "ok",
            "err",
        ):
            value = getattr(self, key)
            if value is not None:
                payload[key] = value
        return payload

    def duration_ms(self) -> int:
        return int((time.perf_counter() - self.started_at) * 1000)


_context_var: ContextVar[OperationContext | None] = ContextVar("operation_context", default=None)
_queue_listener: QueueListener | None = None
_logger: Logger | None = None
_audit_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None


class DailySizeRotatingFileHandler(TimedRotatingFileHandler):
    """Rotate logs every midnight and/or when file exceeds max_bytes."""

    def __init__(self, filename: str, max_bytes: int, backup_count: int = 30):
        super().__init__(
            filename,
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self.max_bytes = max_bytes

    def shouldRollover(self, record):  # type: ignore[override]
        if super().shouldRollover(record):
            return 1
        if self.max_bytes <= 0:
            return 0
        if self.stream is None:
            self.stream = self._open()
        msg = f"{self.format(record)}\n"
        self.stream.seek(0, os.SEEK_END)
        if self.stream.tell() + len(msg.encode(self.encoding or "utf-8")) >= self.max_bytes:
            return 1
        return 0


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event: Dict[str, Any] = getattr(record, "event_data", {})
        event = dict(event)
        event.setdefault("ts", _iso_ts())
        event.setdefault("level", record.levelname)
        if record.exc_info:
            event.setdefault("err", self.formatException(record.exc_info))
            event.setdefault("ok", False)
        return json.dumps(event, ensure_ascii=False, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\x1b[38;5;245m",
        "INFO": "\x1b[38;5;46m",
        "WARN": "\x1b[38;5;214m",
        "WARNING": "\x1b[38;5;214m",
        "ERROR": "\x1b[38;5;196m",
        "RESET": "\x1b[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        event: Dict[str, Any] = getattr(record, "event_data", {})
        level = record.levelname
        color = self.COLORS.get(level, "")
        reset = self.COLORS["RESET"] if color else ""
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        correlation_id = event.get("correlation_id", "----")
        user_part = ""
        if event.get("username"):
            user_part = f" @{event['username']}"
        if event.get("full_name"):
            user_part += f" ({event.get('user_id', '-')}: {event['full_name']})"
        elif event.get("user_id"):
            user_part += f" ({event['user_id']})"
        summary = record.getMessage()
        return f"[{ts} {color}{level}{reset}] (#{correlation_id[:6]}){user_part}: {summary}"


def _ensure_logger() -> Logger:
    global _logger
    if _logger is None:
        _logger = logging.getLogger("bot")
    return _logger


def setup_logging() -> None:
    global _queue_listener
    if _queue_listener is not None:
        return

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue()
    queue_handler = QueueHandler(log_queue)
    root_logger.handlers = [queue_handler]

    handlers: list[logging.Handler] = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleFormatter())
    handlers.append(console_handler)

    if os.getenv("LOG_JSON", "true").lower() in {"1", "true", "yes"}:
        logs_dir = Path(os.getenv("LOG_DIR", "logs"))
        logs_dir.mkdir(parents=True, exist_ok=True)
        max_mb = float(os.getenv("LOG_MAX_MB", "50"))
        max_bytes = int(max_mb * 1024 * 1024)
        file_handler = DailySizeRotatingFileHandler(str(logs_dir / "bot.log"), max_bytes=max_bytes)
        file_handler.setFormatter(JsonFormatter())
        handlers.append(file_handler)

    _queue_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    _queue_listener.start()


def set_audit_sink(callback: Callable[[Dict[str, Any]], Awaitable[None]] | None) -> None:
    global _audit_callback
    _audit_callback = callback


def get_operation_context(create: bool = False) -> OperationContext | None:
    ctx = _context_var.get()
    if ctx is None and create:
        ctx = OperationContext(correlation_id=str(uuid.uuid4()))
        _context_var.set(ctx)
    return ctx


def reset_operation_context() -> None:
    _context_var.set(None)


def start_operation(*, correlation_id: str | None = None, **fields: Any) -> OperationContext:
    ctx = OperationContext(correlation_id=correlation_id or str(uuid.uuid4()))
    for key, value in fields.items():
        if hasattr(ctx, key):
            setattr(ctx, key, value)
    _context_var.set(ctx)
    return ctx


def update_context(**fields: Any) -> None:
    ctx = get_operation_context(create=True)
    if not ctx:
        return
    for key, value in fields.items():
        if hasattr(ctx, key):
            setattr(ctx, key, value)


def _prepare_payload(event: str, level: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    ctx = get_operation_context()
    payload: Dict[str, Any] = {
        "ts": _iso_ts(),
        "level": level,
        "event": event,
        "correlation_id": ctx.correlation_id if ctx else extra.get("correlation_id", str(uuid.uuid4())),
    }

    if ctx:
        payload.update(ctx.to_payload())

    payload.update(extra)

    for field in ("user_message_raw", "bot_reply_preview"):
        if field in payload and isinstance(payload[field], str):
            limit = MAX_RAW_TEXT if field == "user_message_raw" else MAX_PREVIEW
            payload[field] = _truncate(_mask_text(payload[field]), limit=limit)

    for field in ("user_message_preview", "bot_reply_preview"):
        if field in payload and isinstance(payload[field], str):
            payload[field] = _truncate(_mask_text(payload[field]), limit=MAX_PREVIEW)

    if "err" in payload and payload["err"]:
        payload["err"] = _truncate(_mask_text(str(payload["err"])), limit=MAX_RAW_TEXT)

    if ctx and event == "response_sent" and "duration_ms" not in payload:
        payload["duration_ms"] = ctx.duration_ms()

    return payload


def log_event(event: str, level: str = "INFO", message: str | None = None, **extra: Any) -> None:
    logger = _ensure_logger()
    payload = _prepare_payload(event, level, extra)
    record_message = message or extra.get("message") or event
    logger.log(getattr(logging, level, logging.INFO), record_message, extra={"event_data": payload})


def log_exception(event: str, err: Exception, message: str | None = None, **extra: Any) -> None:
    update_context(err=str(err), ok=False)
    log_event(event, level="ERROR", message=message or str(err), **extra)


def mark_response(
    *,
    reply_type: str,
    text: str | None = None,
    document_meta: Dict[str, Any] | None = None,
) -> None:
    preview = _truncate(_mask_text(text or ""), limit=MAX_PREVIEW) if text else None
    fields: Dict[str, Any] = {"bot_reply_type": reply_type}
    if preview is not None:
        fields["bot_reply_preview"] = preview
    if document_meta is not None:
        fields["document_meta"] = document_meta
    update_context(**fields)


def complete_operation(ok: bool, err: str | None = None, *, force: bool = False, **extra: Any) -> None:
    ctx = get_operation_context(create=True)
    if ctx.ok is not None and not force:
        return
    update_context(ok=ok, err=err)
    level = "INFO" if ok else "ERROR"
    payload = _prepare_payload("response_sent", level, {"err": err, "ok": ok, **extra})
    logger = _ensure_logger()
    logger.log(getattr(logging, level, logging.INFO), "response_sent", extra={"event_data": payload})
    if _audit_callback:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_audit_callback(payload))
        except RuntimeError:
            pass


def stop_logging() -> None:
    global _queue_listener
    if _queue_listener:
        _queue_listener.stop()
        _queue_listener = None


def build_audit_summary(payload: Dict[str, Any]) -> str:
    correlation = payload.get("correlation_id", "----")
    username = payload.get("username")
    full_name = payload.get("full_name")
    user_id = payload.get("user_id")
    header_parts = [f"[#{correlation[:6]}]"]
    user_bits = []
    if username:
        user_bits.append(f"@{username}")
    if user_id:
        if full_name:
            user_bits.append(f"({user_id}, {full_name})")
        else:
            user_bits.append(f"({user_id})")
    header_parts.append(" ".join(user_bits) or "(unknown)")

    command = payload.get("command") or payload.get("update_type")
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    arg_parts = []
    for key in ("title", "city", "qty", "site", "pages", "area"):
        if key in args and args[key] not in (None, ""):
            value = args[key]
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            arg_parts.append(f"{key}={value}")
    for key in ("include", "exclude"):
        if key in args and args[key]:
            value = args[key]
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            arg_parts.append(f"{key}={value}")
    request_text = command or "update"
    if arg_parts:
        request_text += " " + " ".join(arg_parts)

    header_parts.append(f"→ {request_text}")

    reply_type = payload.get("bot_reply_type") or "—"
    if reply_type == "document":
        doc = payload.get("document_meta") or {}
        file_name = doc.get("filename", "document")
        rows = doc.get("rows")
        size = doc.get("size_kb")
        reply_summary = f"XLSX {file_name}"
        if rows is not None:
            reply_summary += f" {rows} строк"
        if size is not None:
            reply_summary += f", {size}KB"
    else:
        reply_summary = reply_type

    duration_ms = payload.get("duration_ms")
    duration = f"{(duration_ms or 0)/1000:.1f}s"
    credits_delta = payload.get("credits_delta")
    if isinstance(credits_delta, (int, float)):
        credits_text = f"кредиты: {int(credits_delta):+d}"
    elif credits_delta is not None:
        credits_text = f"кредиты: {credits_delta}"
    else:
        credits_text = "кредиты: 0"
    ok = payload.get("ok")
    status_text = "OK" if ok else "ERR"

    second_line = f"Ответ: {reply_summary} • {duration} • {credits_text} • {status_text}"
    return " ".join(header_parts) + "\n" + second_line
