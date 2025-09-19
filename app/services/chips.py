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
    page_limit: int = 8
    page: int = 0
    labels: List[str] = field(default_factory=list)
    view: Literal["items", "categories"] = "items"
    current_category: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    category_map: Dict[str, List[int]] = field(default_factory=dict)
    category_indices: Dict[str, int] = field(default_factory=dict)
    page_cache: Dict[Optional[str], int] = field(default_factory=dict)

    def current_items(self) -> Sequence[int | str]:
        if self.kind == "role" and self.view == "categories" and self.current_category is None:
            return self.categories
        if self.kind == "role" and self.current_category:
            return self.category_map.get(self.current_category, [])
        return list(range(len(self.candidates)))

    @property
    def total_pages(self) -> int:
        items = self.current_items()
        if not items:
            return 1
        limit = max(1, self.page_limit)
        return max(1, math.ceil(len(items) / limit))


CHIPS_ENABLED = os.getenv("CHIPS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
_PAGE_SIZE = max(8, min(12, int(os.getenv("CHIPS_PAGE_SIZE", "12"))))
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

_ROLE_ALIAS_MAP: Dict[str, str] = {
    normalize_role("Менеджер по продажам"): "Продажи",
    normalize_role("SMM-менеджер"): "SMM",
    normalize_role("Продуктовый менеджер"): "Продакт",
    normalize_role("Бизнес-аналитик"): "Бизн-аналитик",
    normalize_role("Аналитик данных"): "Data Analyst",
    normalize_role("Офис-менеджер"): "Офис-мен",
    normalize_role("Специалист по закупкам"): "Закупки",
    normalize_role("Продуктовый аналитик"): "Продакт-аналитик",
    normalize_role("Продакт-аналитик"): "Продакт-аналитик",
    normalize_role("Разработчик Python"): "Python dev",
    normalize_role("Разработчик Java"): "Java dev",
    normalize_role("Frontend-разработчик"): "Frontend",
    normalize_role("QA инженер"): "QA",
    normalize_role("DevOps инженер"): "DevOps",
    normalize_role("Data Scientist"): "Data Sci",
    normalize_role("Project Manager"): "Project",
    normalize_role("Product Manager"): "Product",
    normalize_role("HR-менеджер"): "HR",
    normalize_role("Контент-менеджер"): "Контент",
    normalize_role("Таргетолог"): "Таргет",
    normalize_role("Продавец-консультант"): "Продавец",
    normalize_role("Маркетолог"): "Маркетинг",
    normalize_role("Дизайнер"): "Дизайн",
    normalize_role("Рекрутер"): "Рекрутер",
    normalize_role("Копирайтер"): "Копирайтер",
    normalize_role("Юрист"): "Юрист",
    normalize_role("Бухгалтер"): "Бухгалтер",
    normalize_role("Финансовый аналитик"): "Фин-аналитик",
    normalize_role("Специалист поддержки"): "Support",
    normalize_role("Customer Success"): "Customer",
}

_CATEGORY_ORDER: Sequence[str] = (
    "Продажи",
    "Маркетинг",
    "Дизайн",
    "Разработка",
    "Аналитика",
    "HR",
    "Офис",
    "Support",
    "Другое",
)

_ROLE_CATEGORY_OVERRIDES: Dict[str, str] = {
    normalize_role("Менеджер по продажам"): "Продажи",
    normalize_role("SMM-менеджер"): "Маркетинг",
    normalize_role("Продуктовый менеджер"): "Разработка",
    normalize_role("Бизнес-аналитик"): "Аналитика",
    normalize_role("Аналитик данных"): "Аналитика",
    normalize_role("Офис-менеджер"): "Офис",
    normalize_role("Специалист по закупкам"): "Офис",
    normalize_role("Специалист поддержки"): "Support",
    normalize_role("Customer Success"): "Support",
    normalize_role("Продуктовый аналитик"): "Аналитика",
    normalize_role("Продакт-аналитик"): "Аналитика",
    normalize_role("Разработчик Python"): "Разработка",
    normalize_role("Разработчик Java"): "Разработка",
    normalize_role("Frontend-разработчик"): "Разработка",
    normalize_role("QA инженер"): "Разработка",
    normalize_role("DevOps инженер"): "Разработка",
    normalize_role("Data Scientist"): "Аналитика",
    normalize_role("Project Manager"): "Разработка",
    normalize_role("Product Manager"): "Разработка",
    normalize_role("HR-менеджер"): "HR",
    normalize_role("Рекрутер"): "HR",
    normalize_role("Маркетолог"): "Маркетинг",
    normalize_role("Контент-менеджер"): "Маркетинг",
    normalize_role("Таргетолог"): "Маркетинг",
    normalize_role("Копирайтер"): "Маркетинг",
    normalize_role("Финансовый аналитик"): "Аналитика",
    normalize_role("Юрист"): "Офис",
    normalize_role("Бухгалтер"): "Офис",
    normalize_role("Продавец-консультант"): "Продажи",
    normalize_role("Дизайнер"): "Дизайн",
}

_CATEGORY_KEYWORDS: Dict[str, Sequence[str]] = {
    "Продажи": ("продаж", "sales", "account", "customer success"),
    "Маркетинг": ("маркет", "smm", "seo", "контент", "таргет", "performance", "pr"),
    "Дизайн": ("дизайн", "designer", "ui", "ux", "product design"),
    "Разработка": ("разработ", "developer", "инжен", "программист", "devops", "qa", "frontend", "backend", "тестиров"),
    "Аналитика": ("аналит", "data", "bi", "ml", "ds"),
    "HR": ("hr", "рекрут", "подбор", "кадр"),
    "Офис": ("офис", "администратор", "секретар", "делопроизвод", "закуп"),
    "Support": ("support", "поддерж", "helpdesk", "саппорт"),
}

_DEFAULT_CATEGORY = "Другое"

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


def _shorten_label(value: str, max_len: int = 16) -> str:
    if len(value) <= max_len:
        return value
    words = value.split()
    result = ""
    for word in words:
        candidate = f"{result} {word}".strip()
        if not candidate:
            continue
        if len(candidate) > max_len:
            break
        result = candidate
    if not result:
        result = value[:max_len].rstrip()
    result = result.rstrip("- ")
    return f"{result}…"


def _role_label(value: str) -> str:
    alias = _ROLE_ALIAS_MAP.get(value)
    if alias:
        return alias
    return _shorten_label(value, 16)


def _detect_category(value: str) -> str:
    override = _ROLE_CATEGORY_OVERRIDES.get(value)
    if override:
        return override
    lowered = value.casefold()
    for category in _CATEGORY_ORDER:
        keywords = _CATEGORY_KEYWORDS.get(category, ())
        for kw in keywords:
            if kw in lowered:
                return category
    return _DEFAULT_CATEGORY


def _ensure_page_key(session: ChipSession) -> Optional[str]:
    if session.kind == "role" and session.view == "items" and session.current_category:
        return session.current_category
    if session.kind == "role" and session.view == "categories":
        return None
    if session.kind == "role" and session.current_category:
        return session.current_category
    return None


def _update_page_limit(session: ChipSession, labels: Sequence[str]) -> None:
    if not labels:
        session.page_limit = max(1, min(session.page_limit, _PAGE_SIZE))
        return
    max_len = max(len(label) for label in labels)
    if max_len <= 10:
        per_row = 4
        rows = 3
    elif max_len <= 16:
        per_row = 3
        rows = 3
    else:
        per_row = 2
        rows = 4
    session.page_limit = min(_PAGE_SIZE, per_row * rows)


def _restore_page_from_cache(session: ChipSession) -> None:
    key = _ensure_page_key(session)
    if key not in session.page_cache:
        session.page_cache[key] = 0
    session.page = session.page_cache.get(key, 0)


def _store_page_in_cache(session: ChipSession) -> None:
    key = _ensure_page_key(session)
    session.page_cache[key] = session.page


def _labels_for_items(session: ChipSession, items: Sequence[int | str]) -> List[str]:
    labels: List[str] = []
    if session.kind == "role" and session.view == "categories" and session.current_category is None:
        labels.extend(str(item) for item in items)
        return labels

    for item in items:
        try:
            index = int(item)
        except (ValueError, TypeError):
            continue
        if 0 <= index < len(session.labels):
            labels.append(session.labels[index])
        elif 0 <= index < len(session.candidates):
            labels.append(session.candidates[index].value)
    return labels


def _build_keyboard(session: ChipSession) -> InlineKeyboardMarkup:
    _restore_page_from_cache(session)

    items = list(session.current_items())
    if not items:
        session.page = 0
        session.page_limit = _PAGE_SIZE
        return InlineKeyboardMarkup(row_width=1)

    all_labels = _labels_for_items(session, items)
    _update_page_limit(session, all_labels)

    total_pages = session.total_pages
    if session.page >= total_pages:
        session.page = max(0, total_pages - 1)

    start = session.page * session.page_limit
    end = start + session.page_limit
    current_items = items[start:end]

    labels = _labels_for_items(session, current_items)

    buttons: List[InlineKeyboardButton] = []

    if session.kind == "role" and session.view == "categories" and session.current_category is None:
        for category in current_items:
            label = str(category)
            index = session.category_indices.get(label, 0)
            buttons.append(
                InlineKeyboardButton(
                    label,
                    callback_data=f"chip:{session.kind}:{session.token}:category:{index}",
                )
            )
    else:
        for idx in current_items:
            try:
                label_index = int(idx)
            except (ValueError, TypeError):
                continue
            if not (0 <= label_index < len(session.candidates)):
                continue
            label = session.labels[label_index] if label_index < len(session.labels) else session.candidates[label_index].value
            buttons.append(
                InlineKeyboardButton(
                    label,
                    callback_data=f"chip:{session.kind}:{session.token}:pick:{label_index}",
                )
            )

    row_width = 4
    if labels:
        max_len = max(len(label) for label in labels)
        if max_len <= 10:
            row_width = 4
        elif max_len <= 16:
            row_width = 3
        else:
            row_width = 2

    markup = InlineKeyboardMarkup(row_width=row_width)
    for chunk in _chunk(buttons, row_width):
        markup.row(*chunk)

    if session.kind == "role" and session.view == "items" and session.current_category:
        markup.row(
            InlineKeyboardButton(
                "⬅️ Назад к категориям",
                callback_data=f"chip:{session.kind}:{session.token}:back",
            )
        )

    if session.total_pages > 1:
        markup.row(
            InlineKeyboardButton(
                "◀️ Назад",
                callback_data=f"chip:{session.kind}:{session.token}:prev",
            ),
            InlineKeyboardButton(
                "Ещё ▶️",
                callback_data=f"chip:{session.kind}:{session.token}:more",
            ),
        )

    if session.kind == "role" and _BASE_ROLE_LIST:
        markup.row(
            InlineKeyboardButton(
                "Случайная роль",
                callback_data=f"chip:{session.kind}:{session.token}:random",
            )
        )

    _store_page_in_cache(session)

    return markup


def _log_render(session: ChipSession) -> None:
    start = session.page * session.page_limit
    end = start + session.page_limit
    current = session.candidates[start:end]
    payload = [
        {"value": candidate.value, "source": candidate.source}
        for candidate in current
    ]
    log_event(
        "chips_rendered",
        chips_type=session.kind,
        page=session.page,
        page_size=session.page_limit,
        total_candidates=len(session.candidates),
        visible=payload,
        view=session.view,
        category=session.current_category,
    )


def _register_session(user_id: int, kind: ChipKind, candidates: List[ChipCandidate]) -> ChipSession:
    token = uuid.uuid4().hex[:12]
    session = ChipSession(
        token=token,
        user_id=user_id,
        kind=kind,
        candidates=candidates,
        page_limit=_PAGE_SIZE,
    )
    _ensure_session_capacity()
    _SESSIONS[token] = session
    _ACTIVE[(user_id, kind)] = token
    session.labels = [
        _role_label(candidate.value) if kind == "role" else candidate.value
        for candidate in candidates
    ]
    session.page_cache = {None: 0}

    if kind == "role":
        max_length = max((len(label) for label in session.labels), default=0)
        use_categories = len(candidates) > session.page_limit or max_length > 16
        if use_categories:
            category_map: Dict[str, List[int]] = {}
            for idx, candidate in enumerate(candidates):
                category = _detect_category(candidate.value)
                category_map.setdefault(category, []).append(idx)
            # фильтруем пустые категории и сохраняем порядок
            ordered: List[str] = []
            for category in _CATEGORY_ORDER:
                if category == _DEFAULT_CATEGORY:
                    continue
                if category_map.get(category):
                    ordered.append(category)
            extras = [cat for cat in category_map.keys() if cat not in ordered]
            if extras:
                for cat in extras:
                    if cat == _DEFAULT_CATEGORY:
                        continue
                    ordered.append(cat)
                if category_map.get(_DEFAULT_CATEGORY):
                    ordered.append(_DEFAULT_CATEGORY)
            session.categories = ordered
            session.category_map = category_map
            session.category_indices = {name: idx for idx, name in enumerate(session.categories)}
            session.view = "categories"
            session.current_category = None
            session.page = 0
            session.page_cache = {None: 0}
        else:
            session.view = "items"
            session.current_category = None
            session.page = 0
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
        base_text = (message.text or "").strip()
        hint = "Популярные варианты\nМожно выбрать из подсказок или ввести вручную"
        if base_text and hint not in base_text:
            new_text = f"{base_text}\n\n{hint}"
        elif base_text:
            new_text = base_text
        else:
            new_text = hint
        await message.edit_text(new_text, reply_markup=markup)
    except (MessageCantBeEdited, MessageNotModified):
        try:
            await message.edit_reply_markup(markup)
        except (MessageCantBeEdited, MessageNotModified):
            await message.answer(
                "Популярные варианты\nМожно выбрать из подсказок или ввести вручную",
                reply_markup=markup,
            )
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


def change_page(session: ChipSession, direction: int) -> InlineKeyboardMarkup:
    total = session.total_pages
    if total <= 0:
        total = 1
    session.page = (session.page + direction) % total
    _store_page_in_cache(session)
    markup = _build_keyboard(session)
    _log_render(session)
    return markup


def show_category(session: ChipSession, index: int) -> InlineKeyboardMarkup:
    if session.kind != "role" or not session.categories:
        markup = _build_keyboard(session)
        _log_render(session)
        return markup
    if not (0 <= index < len(session.categories)):
        markup = _build_keyboard(session)
        _log_render(session)
        return markup
    # запоминаем страницу категорий, чтобы вернуться позже
    session.view = "categories"
    session.current_category = None
    _store_page_in_cache(session)
    session.view = "items"
    category = session.categories[index]
    session.current_category = category
    session.page = session.page_cache.get(category, 0)
    markup = _build_keyboard(session)
    _log_render(session)
    return markup


def back_to_categories(session: ChipSession) -> InlineKeyboardMarkup:
    if session.kind != "role" or session.view == "categories":
        markup = _build_keyboard(session)
        _log_render(session)
        return markup
    # сохраняем страницу текущей категории
    _store_page_in_cache(session)
    session.view = "categories"
    session.current_category = None
    session.page = session.page_cache.get(None, 0)
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
