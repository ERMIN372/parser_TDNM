from __future__ import annotations
import os
import sqlite3
import zipfile
from pathlib import Path

# можно переопределить в .env: DB_PATH=data/bot.db
DB_PATH = os.getenv("DB_PATH", "data/bot.db")

def make_sqlite_backup(dst_zip: Path | str) -> Path:
    """
    Безопасный бэкап SQLite без остановки приложения:
    копируем БД через sqlite backup API -> упаковываем в ZIP.
    """
    dst_zip = Path(dst_zip)
    dst_zip.parent.mkdir(parents=True, exist_ok=True)

    # временный файл для копии
    tmp_db = dst_zip.with_suffix(".tmp.db")

    src = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        # делаем «горячий» бэкап
        dst = sqlite3.connect(tmp_db)
        with dst:
            src.backup(dst)
        dst.close()

        # пакуем в zip
        with zipfile.ZipFile(dst_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(tmp_db, arcname="bot.db")
    finally:
        try: src.close()
        except Exception: pass
        try: tmp_db.unlink(missing_ok=True)
        except Exception: pass

    return dst_zip
