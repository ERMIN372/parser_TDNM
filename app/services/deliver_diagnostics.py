from __future__ import annotations

import json
import os
import shutil
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from aiogram import __version__ as aiogram_version

from app.utils.logging import log_event

STD_TAIL_BYTES = 4096
CSV_PREVIEW_LINES = 10


@dataclass
class DeliverDiagContext:
    user_id: int | None
    username: str | None
    chat_id: int | None
    exception_type: str
    exception_message: str
    stack: str
    xlsx_path: Path | None
    xlsx_size: int | None
    csv_path: Path | None
    stdout_path: Path | None
    stderr_path: Path | None
    cmd_line: str | None
    progress_last_percent: int | None
    xlsx_diagnostics: dict | None = None


def build_diag_bundle(correlation_id: str | None, context: DeliverDiagContext) -> Path:
    """Build diagnostic bundle for delivery failures and return archive path."""

    safe_correlation = _sanitize_correlation(correlation_id)
    user_dir = str(context.user_id) if context.user_id is not None else "unknown"
    base_dir = Path("reports") / user_dir / f"bundle_{safe_correlation}"

    bundle_dir = _ensure_unique_dir(base_dir)
    bundle_dir.mkdir(parents=True, exist_ok=False)

    meta_path = bundle_dir / "meta.json"
    env_path = bundle_dir / "env.txt"
    stdout_path = bundle_dir / "stdout.txt"
    stderr_path = bundle_dir / "stderr.txt"
    head_tail_path = bundle_dir / "head_tail.txt"

    ts_iso = datetime.now(timezone.utc).isoformat()

    meta_payload = {
        "correlation_id": safe_correlation,
        "step": "deliver",
        "exception_type": context.exception_type,
        "exception_message": context.exception_message,
        "stack": context.stack,
        "xlsx_path": str(context.xlsx_path) if context.xlsx_path else None,
        "xlsx_size": context.xlsx_size,
        "cmd_line": context.cmd_line,
        "progress_last_percent": context.progress_last_percent,
        "xlsx_diagnostics": context.xlsx_diagnostics,
        "user": {
            "id": context.user_id,
            "username": context.username,
            "chat_id": context.chat_id,
        },
        "ts": ts_iso,
    }

    meta_path.write_text(
        json.dumps(meta_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stdout_tail = _read_tail(context.stdout_path)
    stderr_tail = _read_tail(context.stderr_path)
    stdout_path.write_text(stdout_tail, encoding="utf-8", errors="replace")
    stderr_path.write_text(stderr_tail, encoding="utf-8", errors="replace")

    head_tail = _read_csv_head_tail(context.csv_path)
    head_tail_path.write_text(head_tail, encoding="utf-8", errors="replace")

    env_summary = _collect_env_summary()
    env_path.write_text(env_summary, encoding="utf-8", errors="replace")

    archive_path = Path(
        shutil.make_archive(str(bundle_dir), "zip", root_dir=bundle_dir, base_dir=".")
    )

    files_count = _count_files(bundle_dir)
    size_bytes = archive_path.stat().st_size if archive_path.exists() else None

    log_event(
        "diagnostic_bundle_ready",
        bundle_path=str(archive_path),
        size_bytes=size_bytes,
        files_count=files_count,
        correlation_id=safe_correlation,
    )

    return archive_path


def _sanitize_correlation(correlation_id: str | None) -> str:
    if correlation_id:
        safe = "".join(ch for ch in correlation_id if ch.isalnum() or ch in {"-", "_"})
        if safe:
            return safe[:64]
    return uuid.uuid4().hex


def _ensure_unique_dir(base_dir: Path) -> Path:
    if not base_dir.exists():
        return base_dir
    suffix = 1
    while True:
        candidate = base_dir.with_name(f"{base_dir.name}_{suffix}")
        if not candidate.exists():
            return candidate
        suffix += 1


def _read_tail(path: Path | None) -> str:
    if not path or not path.exists():
        return "нет данных"
    try:
        with path.open("rb") as src:
            src.seek(0, os.SEEK_END)
            file_size = src.tell()
            offset = max(file_size - STD_TAIL_BYTES, 0)
            src.seek(offset, os.SEEK_SET)
            data = src.read().decode("utf-8", errors="replace")
            if offset > 0:
                return f"…tail from byte {offset}\n" + data
            return data or "нет данных"
    except OSError as exc:
        return f"не удалось прочитать: {exc}"


def _read_csv_head_tail(path: Path | None) -> str:
    if not path or not path.exists():
        return "нет данных"

    try:
        with path.open("r", encoding="utf-8", errors="replace") as src:
            head: list[str] = []
            tail: list[str] = []
            for idx, line in enumerate(src):
                if idx < CSV_PREVIEW_LINES:
                    head.append(line.rstrip("\n"))
                tail.append(line.rstrip("\n"))
                if len(tail) > CSV_PREVIEW_LINES:
                    tail.pop(0)
    except OSError as exc:
        return f"не удалось прочитать csv: {exc}"

    lines: list[str] = []
    lines.append("=== HEAD ===")
    lines.extend(head or ["нет данных"])
    lines.append("=== TAIL ===")
    lines.extend(tail or ["нет данных"])
    return "\n".join(lines)


def _collect_env_summary() -> str:
    env_lines: list[str] = []
    env_lines.append(f"MODE={_safe_env_value(os.getenv('MODE'))}")
    env_lines.append(f"RETURN_URL_BASE={_safe_env_value(os.getenv('RETURN_URL_BASE'))}")
    webhook = _mask_webhook(os.getenv("WEBHOOK_URL"))
    env_lines.append(f"WEBHOOK_URL={webhook}")
    env_lines.append(f"PYTHON_VERSION={sys.version.split()[0]}")
    env_lines.append(f"AIROGRAM_VERSION={aiogram_version}")
    return "\n".join(env_lines)


def _safe_env_value(value: str | None) -> str:
    if not value:
        return ""
    if len(value) > 256:
        return value[:252] + "…"
    return value


def _mask_webhook(value: str | None) -> str:
    if not value:
        return ""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(value)
        if parsed.netloc:
            return parsed.netloc
        return parsed.path or value
    except Exception:
        return value


def _count_files(path: Path) -> int:
    count = 0
    for _ in _iter_files(path):
        count += 1
    return count


def _iter_files(path: Path) -> Iterable[Path]:
    for entry in path.iterdir():
        if entry.is_dir():
            yield from _iter_files(entry)
        else:
            yield entry


def build_stack(exception: BaseException) -> str:
    return "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )

