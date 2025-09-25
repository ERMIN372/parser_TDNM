from __future__ import annotations

import shutil
import uuid
from datetime import datetime
from pathlib import Path


def make_diag_dir(user_id: int) -> Path:
    base_dir = Path("reports") / str(user_id) / "_diag"
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    diag_dir = base_dir / f"{timestamp}_{suffix}"
    diag_dir.mkdir(parents=True, exist_ok=False)
    return diag_dir


def save_text(path: Path, name: str, content: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / name).write_text(content or "", encoding="utf-8")


def zip_dir(dir_path: Path) -> Path:
    archive_path = shutil.make_archive(str(dir_path), "zip", root_dir=dir_path)
    return Path(archive_path)
