"""Diagnostic helpers for persistent bundles and runtime snapshots."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

try:  # pragma: no cover - optional dependency during tests
    from aiogram import Dispatcher  # type: ignore
    from aiogram.contrib.fsm_storage.memory import MemoryStorage  # type: ignore
    from aiogram.dispatcher.storage import BaseStorage  # type: ignore
except Exception:  # pragma: no cover - aiogram may be absent in tests
    Dispatcher = None  # type: ignore
    MemoryStorage = None  # type: ignore
    BaseStorage = None  # type: ignore

try:  # pragma: no cover - optional busy middleware during tests
    from app.middlewares.busy import BUSY_USERS  # type: ignore
except Exception:  # pragma: no cover - dependency chain may be missing
    BUSY_USERS = set()

ENV_WHITELIST: Sequence[str] = (
    "MODE",
    "PORT",
    "WEBAPP_PORT",
    "WEBHOOK_URL",
    "REPLIT_RELEASE",
    "PYTHON_VERSION",
    "PARSER_PIPELINE",
    "PARSER_TIMEOUT",
    "PARSER_TIMEOUT_LARGE",
    "PREVIEW_MODE",
    "PREVIEW_TIMEOUT",
    "PREVIEW_RETRIES",
    "PREVIEW_ROWS",
    "REPORT_DIR",
)


def _gather_env() -> str:
    rows: list[str] = []
    for key in ENV_WHITELIST:
        value = os.getenv(key)
        if value is not None:
            rows.append(f"{key}={value}")
    return "\n".join(rows)


@dataclass
class DiagnosticBundle:
    path: Path
    bundle_id: str

    @property
    def stack_path(self) -> Path:
        return self.path / "stack.txt"


def write_bundle(
    base_dir: Path,
    prefix: str,
    meta: dict,
    stdout_lines: Iterable[str],
    stderr_lines: Iterable[str],
    *,
    stack: str | None = None,
) -> DiagnosticBundle:
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    bundle_dir = base_dir / f"{prefix}_{ts}"
    counter = 1
    while bundle_dir.exists():
        counter += 1
        bundle_dir = base_dir / f"{prefix}_{ts}_{counter}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    bundle_id = bundle_dir.name
    meta = dict(meta)
    meta.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    meta.setdefault("bundle_id", bundle_id)

    (bundle_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (bundle_dir / "stdout.txt").write_text("\n".join(stdout_lines), encoding="utf-8")
    (bundle_dir / "stderr.txt").write_text("\n".join(stderr_lines), encoding="utf-8")
    (bundle_dir / "env.txt").write_text(_gather_env(), encoding="utf-8")
    if stack:
        (bundle_dir / "stack.txt").write_text(stack, encoding="utf-8")

    return DiagnosticBundle(path=bundle_dir, bundle_id=bundle_id)


@dataclass
class StorageDiagnostics:
    """Safe snapshot of FSM storage internals."""

    type: str
    user_count: int | None = None
    state_keys: int | None = None
    data_preview: Dict[str, Any] | None = None
    error: str | None = None


def _describe_memory_storage(storage: MemoryStorage) -> StorageDiagnostics:  # type: ignore[arg-type]
    """Return diagnostic info for :class:`MemoryStorage`."""

    try:
        raw_storage: Dict[Any, Dict[Any, Dict[str, Any]]] = getattr(storage, "storage", {})
        user_count = 0
        state_keys = 0
        preview: Dict[str, Any] = {}
        for chat_id, users in list(raw_storage.items()):
            if not isinstance(users, dict):
                continue
            for user_id, state_data in users.items():
                user_count += 1
                if isinstance(state_data, dict):
                    state_keys += len(state_data)
                    if len(preview) < 5:
                        key = f"{chat_id}:{user_id}"
                        preview[key] = {k: v for k, v in state_data.items() if k != "data"}
        return StorageDiagnostics(
            type=type(storage).__name__,
            user_count=user_count,
            state_keys=state_keys,
            data_preview=preview or None,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return StorageDiagnostics(type=type(storage).__name__, error=str(exc))


def describe_storage(storage: BaseStorage | None) -> StorageDiagnostics:  # type: ignore[type-arg]
    """Gather safe diagnostic information about the FSM storage."""

    if storage is None or BaseStorage is None:
        return StorageDiagnostics(type="<none>")
    if MemoryStorage is not None and isinstance(storage, MemoryStorage):
        return _describe_memory_storage(storage)
    return StorageDiagnostics(type=type(storage).__name__)


def collect_runtime_diagnostics(dp: Dispatcher | None) -> Dict[str, Any]:  # type: ignore[type-arg]
    """Produce a snapshot with useful runtime diagnostics for the webhook UI."""

    diag: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dispatcher_ready": dp is not None,
        "busy_users": len(BUSY_USERS),
    }

    if dp is None or Dispatcher is None:
        diag["storage"] = asdict(StorageDiagnostics(type="<none>"))
        return diag

    storage_diag = describe_storage(dp.storage)
    diag["storage"] = asdict(storage_diag)
    diag["middlewares"] = [type(m).__name__ for m in getattr(dp.middlewares, "middlewares", [])]
    diag["registered_handlers"] = {
        "message": len(dp.message_handlers.handlers),
        "callback": len(dp.callback_query_handlers.handlers),
        "errors": len(dp.errors_handlers),
    }
    return diag
