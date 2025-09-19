from __future__ import annotations

import math
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Literal, Optional, Sequence

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified

from app.storage import repo
from app.utils.logging import log_event
from app.utils.normalize import normalize_city, normalize_for_dedup, normalize_role

ChipKind = Literal["role", "city"]


@dataclass
class ChipCandidate:
    value: str
    source: str  # personal | trending | base


@dataclass
class ChipSession:
    token: str
    user_id: int
    kind: ChipKind
    candidates: List[ChipCandidate] = field(default_factory=list)
    page_size: int = 8
    page: int = 0

    @property
    def total_pages(self) -> int:
        if not self.candidates:
            return 1
        return max(1, math.ceil(len(self.candidates) / self.page_size))


CHIPS_ENABLED = os.getenv("CHIPS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
_PAGE_SIZE = max(1, min(8, int(os.getenv("CHIPS_PAGE_SIZE", "8"))))
_TREND_DAYS = max(1, int(os.getenv("CHIPS_TREND_DAYS", "7")))
_TREND_MIN = max(1, int(os.getenv("CHIPS_TREND_MIN_COUNT", "3")))
_PERSONAL_LIMIT = min(4, _PAGE_SIZE)

_BASE_ROLE_SEED: Sequence[str] = (
    "Продавец-консультант",
    "Маркетолог",
    "Дизайнер",
    "Рекрутер",
    "SMM-менеджер",
    "Продуктовый аналитик",
    "Бизнес-аналитик",
    "Разработчик Python",
    "Разработчик Java",
    "Frontend-разработчик",
    "QA инженер",
    "DevOps инженер",
    "Data Scientist",
    "Project Manager",
    "Product Manager",
    "HR-менеджер",
    "Копирайтер",
    "Контент-менеджер",
    "Таргетолог",
    "Продакт-аналитик",
    "Юрист",
    "Бухгалтер",
    "Финансовый аналитик",
    "Аналитик данных",
    "Специалист поддержки",
    "Менеджер по продажам",
    "Customer Success",
)

_BASE_CITY_SEED: Sequence[str] = (
    "Москва",
    "Санкт-Петербург",
    "Удалёнка",
    "Новосибирск",
    "Екатеринбург",
    "Казань",
    "Нижний Новгород",
    "Краснодар",
    "Самара",
    "Ростов-на-Дону",
    "Челябинск",
    "Уфа",
    "Пермь",
    "Воронеж",
    "Красноярск",
    "Тюмень",
    "Сочи",
    "Волгоград",
    "Ижевск",
    "Барнаул",
    "Калининград",
    "Омск",
)

_MANDATORY_CITIES = {normalize_city("Москва"), normalize_city("Санкт-Петербург"), normalize_city("Удалёнка")}

_BASE_ROLE_LIST = [normalize_role(v) for v in _BASE_ROLE_SEED if normalize_role(v)]
_BASE_CITY_LIST = []
for city in _BASE_CITY_SEED:
    norm_city = normalize_city(city)
    if norm_city and norm_city not in _BASE_CITY_LIST:
        _BASE_CITY_LIST.append(norm_city)
for mandatory in _MANDATORY_CITIES:
    if mandatory not in _BASE_CITY_LIST:
        _BASE_CITY_LIST.insert(0, mandatory)

_SESSIONS: Dict[str, ChipSession] = {}
_ACTIVE: Dict[tuple[int, ChipKind], str] = {}
_SESSION_LIMIT = 500


def _ensure_session_capacity() -> None:
    if len(_SESSIONS) <= _SESSION_LIMIT:
        return
    for token in list(_SESSIONS.keys())[: len(_SESSIONS) - _SESSION_LIMIT]:
        _SESSIONS.pop(token, None)


def _chunk(iterable: Sequence[InlineKeyboardButton], size: int) -> Iterable[Sequence[InlineKeyboardButton]]:
    for idx in range(0, len(iterable), size):
        yield iterable[idx : idx + size]


def _build_keyboard(session: ChipSession) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=4)
    start = session.page * session.page_size
    end = start + session.page_size
    page_items = session.candidates[start:end]
    buttons: List[InlineKeyboardButton] = []
    for offset, candidate in enumerate(page_items):
        index = start + offset
        buttons.append(
            InlineKeyboardButton(
                candidate.value,
                callback_data=f"chip:{session.kind}:{session.token}:pick:{index}",
            )
        )
    for chunk in _chunk(buttons, 4):
        markup.row(*chunk)

    control_buttons: List[InlineKeyboardButton] = []
    if session.total_pages > 1:
        control_buttons.append(
            InlineKeyboardButton(
                "Ещё…",
                callback_data=f"chip:{session.kind}:{session.token}:more",
            )
        )
    if session.kind == "role" and _BASE_ROLE_LIST:
        control_buttons.append(
            InlineKeyboardButton(
                "Случайная роль",
                callback_data=f"chip:{session.kind}:{session.token}:random",
            )
        )
    if control_buttons:
        markup.row(*control_buttons)
    return markup


def _log_render(session: ChipSession) -> None:
    start = session.page * session.page_size
    end = start + session.page_size
    current = session.candidates[start:end]
    payload = [
        {"value": candidate.value, "source": candidate.source}
        for candidate in current
    ]
    log_event(
        "chips_rendered",
        chips_type=session.kind,
        page=session.page,
        page_size=session.page_size,
        total_candidates=len(session.candidates),
        visible=payload,
    )


def _register_session(user_id: int, kind: ChipKind, candidates: List[ChipCandidate]) -> ChipSession:
    token = uuid.uuid4().hex[:12]
    session = ChipSession(
        token=token,
        user_id=user_id,
        kind=kind,
        candidates=candidates,
        page_size=_PAGE_SIZE,
    )
    _ensure_session_capacity()
    _SESSIONS[token] = session
    _ACTIVE[(user_id, kind)] = token
    return session


def _collect_personal(user_id: int, kind: ChipKind) -> List[ChipCandidate]:
    try:
        history = repo.get_recent_searches(user_id, limit=40)
    except Exception:
        return []
    seen: set[str] = set()
    result: List[ChipCandidate] = []
    for record in history:
        value = normalize_role(record.role) if kind == "role" else normalize_city(record.city)
        key = normalize_for_dedup(value)
        if not value or key in seen:
            continue
        seen.add(key)
        result.append(ChipCandidate(value=value, source="personal"))
        if len(result) >= _PERSONAL_LIMIT:
            break
    return result


def _collect_trending(kind: ChipKind, limit: int) -> List[ChipCandidate]:
    since = datetime.utcnow() - timedelta(days=_TREND_DAYS)
    fetch_limit = max(limit * 3, limit)
    try:
        if kind == "role":
            rows = repo.get_trending_roles(since, fetch_limit, _TREND_MIN)
        else:
            rows = repo.get_trending_cities(since, fetch_limit, _TREND_MIN)
    except Exception:
        return []
    result: List[ChipCandidate] = []
    seen: set[str] = set()
    for value, _count in rows:
        norm = normalize_role(value) if kind == "role" else normalize_city(value)
        key = normalize_for_dedup(norm)
        if not norm or key in seen:
            continue
        seen.add(key)
        result.append(ChipCandidate(value=norm, source="trending"))
    return result


def _collect_base(kind: ChipKind) -> List[ChipCandidate]:
    base_values = _BASE_ROLE_LIST if kind == "role" else _BASE_CITY_LIST
    return [ChipCandidate(value=value, source="base") for value in base_values]


def _merge_candidates(
    personal: Sequence[ChipCandidate],
    trending: Sequence[ChipCandidate],
    base: Sequence[ChipCandidate],
    *,
    kind: ChipKind,
) -> List[ChipCandidate]:
    result: List[ChipCandidate] = []
    seen: set[str] = set()

    def _extend(items: Sequence[ChipCandidate]) -> None:
        for item in items:
            key = normalize_for_dedup(item.value)
            if key in seen:
                continue
            seen.add(key)
            result.append(item)

    _extend(personal)
    _extend(trending)
    _extend(base)

    return result


def _prepare_candidates(user_id: int, kind: ChipKind) -> List[ChipCandidate]:
    personal = _collect_personal(user_id, kind)
    trending = _collect_trending(kind, _PAGE_SIZE)
    base = _collect_base(kind)
    return _merge_candidates(personal, trending, base, kind=kind)


async def _render_for_kind(message: Message, user_id: int, kind: ChipKind) -> None:
    if not CHIPS_ENABLED:
        return
    candidates = _prepare_candidates(user_id, kind)
    if not candidates:
        return
    session = _register_session(user_id, kind, candidates)
    markup = _build_keyboard(session)
    try:
        await message.edit_reply_markup(markup)
    except (MessageCantBeEdited, MessageNotModified):
        await message.answer("Популярные варианты:", reply_markup=markup)
    _log_render(session)


async def render_role_chips(message: Message, user_id: int) -> None:
    await _render_for_kind(message, user_id, "role")


async def render_city_chips(message: Message, user_id: int) -> None:
    await _render_for_kind(message, user_id, "city")


def finish_session(user_id: int, kind: ChipKind) -> None:
    token = _ACTIVE.pop((user_id, kind), None)
    if token:
        _SESSIONS.pop(token, None)


def get_session(token: str) -> Optional[ChipSession]:
    return _SESSIONS.get(token)


def is_active(user_id: int, kind: ChipKind, token: str) -> bool:
    return _ACTIVE.get((user_id, kind)) == token


def advance_page(session: ChipSession) -> InlineKeyboardMarkup:
    session.page = (session.page + 1) % session.total_pages
    markup = _build_keyboard(session)
    _log_render(session)
    return markup


def resolve_candidate(session: ChipSession, index: int) -> Optional[ChipCandidate]:
    if index < 0 or index >= len(session.candidates):
        return None
    return session.candidates[index]


def log_click(kind: ChipKind, value: str, source: str, *, position: Optional[int], action: str | None = None) -> None:
    payload = {
        "chips_type": kind,
        "value": value,
        "source": source,
    }
    if position is not None:
        payload["position"] = position
    if action:
        payload["action"] = action
    log_event("chip_clicked", **payload)


def random_role() -> Optional[str]:
    if not _BASE_ROLE_LIST:
        return None
    return random.choice(_BASE_ROLE_LIST)


def record_success(user_id: int, role: str, city: str) -> None:
    try:
        norm_role = normalize_role(role)
        norm_city = normalize_city(city)
        repo.record_successful_search(user_id, norm_role, norm_city)
    except Exception:
        # не блокируем основной сценарий на сбоях БД
        pass


def parse_callback_data(data: str) -> Optional[dict[str, str]]:
    parts = data.split(":")
    if len(parts) < 4 or parts[0] != "chip":
        return None
    kind = parts[1]
    token = parts[2]
    action = parts[3]
    payload = {"kind": kind, "token": token, "action": action}
    if len(parts) > 4:
        payload["value"] = parts[4]
    return payload
