from __future__ import annotations

import os
import shutil
from pathlib import Path

from peewee import SqliteDatabase


_LEGACY_DB_DIR = Path("data")
_LEGACY_DB_NAME = "bot.db"
_DEFAULT_DB_PATH = Path("var/db") / _LEGACY_DB_NAME


def _resolve_db_path() -> Path:
    """Return the path to the SQLite database and migrate legacy locations."""

    env_value = os.getenv("DB_PATH")
    if env_value:
        db_path = Path(env_value)
    else:
        db_path = _DEFAULT_DB_PATH
        _migrate_legacy_db(db_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def _migrate_legacy_db(target_path: Path) -> None:
    """Move the old tracked DB files into the new untracked location if needed."""

    legacy_db = _LEGACY_DB_DIR / _LEGACY_DB_NAME
    if not legacy_db.exists():
        return

    target_path.parent.mkdir(parents=True, exist_ok=True)

    if not target_path.exists():
        shutil.move(str(legacy_db), target_path)

    for suffix in ("-wal", "-shm"):
        legacy_sidecar = legacy_db.parent / f"{_LEGACY_DB_NAME}{suffix}"
        if not legacy_sidecar.exists():
            continue

        destination = target_path.parent / f"{target_path.name}{suffix}"
        if destination.exists():
            continue

        shutil.move(str(legacy_sidecar), destination)


DB_FILE = _resolve_db_path()
DB_PATH = str(DB_FILE)

# SQLite тюн: WAL, FK, небольшой кэш. Разрешаем из разных потоков.
db = SqliteDatabase(
    DB_PATH,
    pragmas={
        "journal_mode": "wal",
        "foreign_keys": 1,
        "cache_size": -64 * 1024,  # ~64MB page cache
        "synchronous": 1,
    },
    check_same_thread=False,
)

def init_db() -> None:
    """Создать таблицы, если их ещё нет."""
    from .models import (
        User,
        Usage,
        Credit,
        Payment,
        Referral,
        ReferralStats,
        PromoCode,
        Ledger,
        ReferralBan,
        SearchQuery,
    )  # noqa: WPS347
    db.connect(reuse_if_open=True)
    db.create_tables([
        User,
        Usage,
        Credit,
        Payment,
        Referral,
        ReferralStats,
        PromoCode,
        Ledger,
        ReferralBan,
        SearchQuery,
    ])
    db.close()
