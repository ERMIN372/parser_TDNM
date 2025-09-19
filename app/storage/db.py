from __future__ import annotations
import os
from pathlib import Path
from peewee import SqliteDatabase

DB_PATH = os.getenv("DB_PATH", "data/bot.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

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
