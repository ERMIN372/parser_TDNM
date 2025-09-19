from __future__ import annotations

import re
from typing import Dict

_WHITESPACE_RE = re.compile(r"\s+")

_CITY_ALIASES: Dict[str, str] = {
    "мск": "Москва",
    "москва": "Москва",
    "moscow": "Москва",
    "санкт-петербург": "Санкт-Петербург",
    "санкт петербург": "Санкт-Петербург",
    "спб": "Санкт-Петербург",
    "spb": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "saint petersburg": "Санкт-Петербург",
    "удаленка": "Удалёнка",
    "удалёнка": "Удалёнка",
    "удаленно": "Удалёнка",
    "удалённо": "Удалёнка",
    "удаленная работа": "Удалёнка",
    "удалённая работа": "Удалёнка",
    "remote": "Удалёнка",
}


def _clean(value: str | None) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip()


def normalize_role(value: str | None) -> str:
    return _clean(value)


def normalize_city(value: str | None) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return ""

    lowered = cleaned.lower()
    if lowered.startswith("г."):
        cleaned = cleaned[2:].strip()
        lowered = cleaned.lower()

    alias = _CITY_ALIASES.get(lowered)
    if alias:
        return alias

    # аккуратный регистр: первая буква — прописная, остальные как в title()
    normalized = cleaned.title()
    if "-" in cleaned:
        parts = [part.capitalize() for part in cleaned.split("-")]
        normalized = "-".join(parts)
    return normalized


def normalize_for_dedup(value: str) -> str:
    return normalize_role(value).casefold()
