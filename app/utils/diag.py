from __future__ import annotations

import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional


def make_diag_dir(user_id: int) -> Path:
    base_dir = Path("reports") / str(user_id) / "_diag"
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    diag_dir = base_dir / f"{timestamp}_{suffix}"
    diag_dir.mkdir(parents=True, exist_ok=False)
    return diag_dir


def get_last_diag_dir(user_id: int) -> Optional[Path]:
    base_dir = Path("reports") / str(user_id) / "_diag"
    if not base_dir.exists():
        return None

    latest: tuple[datetime, Path] | None = None
    for entry in base_dir.iterdir():
        if not entry.is_dir():
            continue
        parts = entry.name.split("_")
        if len(parts) < 2:
            continue
        ts_str = f"{parts[0]}_{parts[1]}"
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        if latest is None or ts > latest[0] or (ts == latest[0] and entry.name > latest[1].name):
            latest = (ts, entry)

    return latest[1] if latest else None


def save_text(path: Path, name: str, content: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / name).write_text(content or "", encoding="utf-8")


def zip_dir(dir_path: Path) -> Path:
    archive_path = shutil.make_archive(str(dir_path), "zip", root_dir=dir_path)
    return Path(archive_path)
