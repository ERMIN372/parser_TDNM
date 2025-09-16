from __future__ import annotations
import os
import re
from typing import Optional, Tuple, List
import requests

UA = os.getenv("PARSER_USER_AGENT", "hr-assist/1.0")
HH_BASE = os.getenv("PARSER_HH_BASE", "https://api.hh.ru")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MIN_FOUND = int(os.getenv("MIN_FOUND", "5"))  # порог «есть смысл бежать парсер»


_allowed_re = re.compile(r"^[A-Za-zА-Яа-яЁё\s\-\.\,]+$")
_multi_same_re = re.compile(r"(.)\1{3,}", re.UNICODE)  # «аааа», «лллл» и т.п.
_has_letter_re = re.compile(r"[A-Za-zА-Яа-яЁё]")
_has_vowel_ru = re.compile(r"[аеёиоуыэюя]", re.IGNORECASE)
_has_vowel_lat = re.compile(r"[aeiouy]", re.IGNORECASE)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def validate_title(title: str) -> Tuple[bool, str]:
    t = _clean(title)
    if len(t) < 3:
        return False, "Слишком короткая должность. Напиши, например: «бариста», «продавец»."
    if len(t) > 60:
        return False, "Слишком длинная должность. Сформулируй короче."
    if not _allowed_re.match(t):
        return False, "Похоже, в должности есть лишние символы. Оставь только буквы, пробелы и дефисы."
    if _multi_same_re.search(t):
        return False, "Похоже на опечатку (слишком много одинаковых подряд). Попробуй ещё раз."
    if not _has_letter_re.search(t):
        return False, "Не похоже на должность. Введи название профессии."
    if not (_has_vowel_ru.search(t) or _has_vowel_lat.search(t)):
        return False, "В должности нет гласных — возможно, опечатка. Попробуй ещё раз."
    return True, ""


def resolve_city(city: str) -> Tuple[bool, Optional[int], str, List[str]]:
    """
    Возвращает (ok, area_id, canonical_city, suggestions)
    """
    c = _clean(city)
    if len(c) < 2:
        return False, None, "", []
    if not _allowed_re.match(c):
        return False, None, "", []

    try:
        r = requests.get(
            f"{HH_BASE}/suggests/areas",
            params={"text": c},
            headers={"User-Agent": UA},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        if not items:
            return False, None, "", []
        # Берём первое совпадение из РФ при наличии, иначе первое вообще
        best = None
        for it in items:
            if it.get("area_parent_name") in ("Россия", "Russian Federation", None):
                best = it
                break
        if not best:
            best = items[0]
        area_id = int(best["id"])
        name = best["text"]
        suggestions = [it.get("text") for it in items[:5] if it.get("text")]
        return True, area_id, name, suggestions
    except Exception:
        # сетевой сбой: считаем, что город не валиден (лучше переспросить)
        return False, None, "", []


def probe_hh_found(title: str, area_id: int) -> Tuple[bool, int]:
    """
    Лёгкая проверка рынка: смотрим поле 'found' на HH. Безопасно, 1 запрос.
    """
    t = _clean(title)
    try:
        r = requests.get(
            f"{HH_BASE}/vacancies",
            params={"text": t, "area": area_id, "per_page": 1},
            headers={"User-Agent": UA},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        found = int(r.json().get("found", 0))
        return True, found
    except Exception:
        return False, 0


def validate_request(title: str, city: str) -> Tuple[bool, str, int, str]:
    """
    Комплексная валидация:
    - должность
    - распознаём город -> area_id
    - проверяем 'found' на HH
    Возвращает: (ok, norm_title, area_id, message_if_not_ok)
    """
    ok, msg = validate_title(title)
    if not ok:
        return False, "", 0, msg

    ok_city, area_id, canonical_city, suggestions = resolve_city(city)
    if not ok_city or not area_id:
        hint = ""
        if suggestions:
            hint = "\nВозможно, имелось в виду: " + ", ".join(suggestions[:5])
        return False, "", 0, "Не нашёл такой город. Попробуй точнее." + hint

    ok_probe, found = probe_hh_found(title, area_id)
    if ok_probe and found < MIN_FOUND:
        return False, "", 0, (
            f"По запросу «{_clean(title)}» в «{canonical_city}» слишком мало вакансий ({found}). "
            "Проверь опечатки или укажи другое название."
        )
    # если пробу не удалось сделать — не блокируем, но это редкость
    return True, _clean(title), area_id, ""
