from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

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
