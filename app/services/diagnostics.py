from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional


_last_bundle_by_chat: Dict[int, Path] = {}
_bundle_by_correlation: Dict[str, Path] = {}


def remember_bundle(*, chat_id: int | None, correlation_id: str | None, bundle_path: Path) -> None:
    if chat_id is not None:
        _last_bundle_by_chat[chat_id] = bundle_path
    if correlation_id:
        _bundle_by_correlation[correlation_id] = bundle_path


def get_last_bundle(chat_id: int | None) -> Optional[Path]:
    if chat_id is None:
        return None
    path = _last_bundle_by_chat.get(chat_id)
    if path and path.exists():
        return path
    return None


def get_bundle_by_correlation(correlation_id: str | None) -> Optional[Path]:
    if not correlation_id:
        return None
    path = _bundle_by_correlation.get(correlation_id)
    if path and path.exists():
        return path
    return None
