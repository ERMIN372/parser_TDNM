from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from . import payments

REQUEST_TTL_SECONDS = int(os.getenv("PAYWALL_REQUEST_TTL", "900"))
PAYMENT_TTL_SECONDS = int(os.getenv("PAYWALL_PAYMENT_TTL", "900"))


def _now() -> datetime:
    return datetime.utcnow()


def _format_rub(amount_cop: int) -> str:
    rub = int(round(amount_cop / 100))
    return f"{rub:,}".replace(",", " ")


@dataclass
class SavedRequest:
    kind: str
    query: str
    city: str
    overrides: Dict[str, Any] = field(default_factory=dict)
    area_id: Optional[int] = None
    total: Optional[int] = None
    approx_total: Optional[int] = None
    created_at: datetime = field(default_factory=_now)

    def __post_init__(self) -> None:
        self.overrides = dict(self.overrides or {})

    def summary(self) -> str:
        title = (self.query or "").strip() or "—"
        city = (self.city or "").strip() or "—"
        return f"{title}; {city}"

    def to_log(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "kind": self.kind,
            "query": self.query,
            "city": self.city,
        }
        if self.total is not None:
            payload["total"] = self.total
        if self.area_id is not None:
            payload["area_id"] = self.area_id
        if self.overrides:
            payload["overrides"] = self.overrides
        return payload


@dataclass
class PendingPayment:
    payment_id: str
    pack: str
    url: str
    created_at: datetime = field(default_factory=_now)

    def is_expired(self) -> bool:
        return (_now() - self.created_at) > timedelta(seconds=PAYMENT_TTL_SECONDS)


_REQUEST_CACHE: Dict[int, tuple[datetime, SavedRequest]] = {}
_PENDING_PAYMENTS: Dict[int, PendingPayment] = {}


def save_request(user_id: int, request: SavedRequest) -> None:
    expires_at = _now() + timedelta(seconds=REQUEST_TTL_SECONDS)
    _REQUEST_CACHE[user_id] = (expires_at, request)


def get_request(user_id: int) -> Optional[SavedRequest]:
    entry = _REQUEST_CACHE.get(user_id)
    if not entry:
        return None
    expires_at, request = entry
    if _now() > expires_at:
        _REQUEST_CACHE.pop(user_id, None)
        return None
    return request


def consume_request(user_id: int) -> Optional[SavedRequest]:
    request = get_request(user_id)
    if request is None:
        return None
    _REQUEST_CACHE.pop(user_id, None)
    return request


def clear_request(user_id: int) -> None:
    _REQUEST_CACHE.pop(user_id, None)


def set_pending_payment(user_id: int, payment_id: str, pack: str, url: str) -> PendingPayment:
    pending = PendingPayment(payment_id=payment_id, pack=pack, url=url)
    _PENDING_PAYMENTS[user_id] = pending
    return pending


def get_pending_payment(user_id: int) -> Optional[PendingPayment]:
    pending = _PENDING_PAYMENTS.get(user_id)
    if not pending:
        return None
    if pending.is_expired():
        _PENDING_PAYMENTS.pop(user_id, None)
        return None
    return pending


def clear_pending_payment(user_id: int) -> None:
    _PENDING_PAYMENTS.pop(user_id, None)


def paywall_text() -> str:
    lines = [
        "Лимит исчерпан: бесплатные запросы на этот месяц закончились, платные кредиты отсутствуют.",
        "",
        "Можно оформить доступ:",
    ]
    for pack_id in payments.PACK_ORDER:
        price_cop = payments.PRICES.get(pack_id)
        if price_cop is None:
            continue
        title = payments.TITLES.get(pack_id, pack_id)
        lines.append(f"• {title} — ₽{_format_rub(price_cop)}")
    return "\n".join(lines)


def pack_price_text(pack_id: str) -> str:
    price_cop = payments.PRICES.get(pack_id)
    if price_cop is None:
        return ""
    return f"₽{_format_rub(price_cop)}"


def paywall_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("Купить 1", callback_data="buy:pack:1"),
        InlineKeyboardButton("Купить 3", callback_data="buy:pack:3"),
        InlineKeyboardButton("Купить 9", callback_data="buy:pack:9"),
        InlineKeyboardButton("Безлимит на 30 дней", callback_data="buy:unlim:30"),
    )
    kb.row(InlineKeyboardButton("Тарифы", callback_data="buy:info"))
    kb.row(InlineKeyboardButton("Назад в меню", callback_data="buy:back"))
    return kb


def resume_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Да, запусти", callback_data="resume:last"),
        InlineKeyboardButton("Нет, позже", callback_data="resume:skip"),
    )
    return kb
