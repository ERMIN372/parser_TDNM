from __future__ import annotations

import os
from typing import Iterable, Set


def _load_admins() -> Set[int]:
    raw = os.getenv("ADMIN_USER_IDS", "")
    cleaned = raw.replace(" ", "")
    return {int(x) for x in cleaned.split(",") if x.isdigit()}


_ADMINS = _load_admins()


def is_admin(user_id: int | None) -> bool:
    """Return True if the given Telegram user id is an admin."""
    if user_id is None:
        return False
    return user_id in _ADMINS


def admin_ids() -> Iterable[int]:
    """Expose the configured admin ids (read-only copy)."""
    return set(_ADMINS)
