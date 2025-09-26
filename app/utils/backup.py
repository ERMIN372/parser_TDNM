from __future__ import annotations

from datetime import datetime
import sqlite3
import zipfile
from pathlib import Path

from app.storage.db import DB_FILE


_BACKUP_DIR_NAME = "backups"

def make_sqlite_backup(dst_zip: Path | str) -> Path:
    """
    Безопасный бэкап SQLite без остановки приложения:
    копируем БД через sqlite backup API -> упаковываем в ZIP.
    """
    dst_zip = Path(dst_zip)
    dst_zip.parent.mkdir(parents=True, exist_ok=True)

    # временный файл для копии
    tmp_db = dst_zip.with_suffix(".tmp.db")

    if not DB_FILE.exists():
        raise FileNotFoundError(f"Database file {DB_FILE} does not exist")

    src = sqlite3.connect(str(DB_FILE), check_same_thread=False)
    try:
        # делаем «горячий» бэкап
        dst = sqlite3.connect(tmp_db)
        with dst:
            src.backup(dst)
        dst.close()

        # пакуем в zip
        with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(tmp_db, arcname=DB_FILE.name)
    finally:
        try: src.close()
        except Exception: pass
        try: tmp_db.unlink(missing_ok=True)
        except Exception: pass

    return dst_zip


def create_timestamped_backup(*, max_backups: int = 10) -> Path:
    """Create a timestamped SQLite backup and keep only the newest ones."""

    backup_dir = DB_FILE.parent / _BACKUP_DIR_NAME
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_name = f"{DB_FILE.stem}_{ts}.zip"
    destination = backup_dir / archive_name

    make_sqlite_backup(destination)

    backups = sorted(
        (p for p in backup_dir.glob("*.zip") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in backups[max(0, max_backups) :]:
        try:
            old.unlink(missing_ok=True)
        except Exception:
            pass

    return destination
