from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from peewee import fn
from .db import db
from .models import User, Usage, Credit, Payment, SearchQuery


# ---------- утилиты ----------
def _month_key(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.utcnow()
    return dt.strftime("%Y-%m")


# ---------- базовые операции с пользователем/лимитами ----------
def ensure_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> User:
    """
    Создаёт пользователя при первом обращении и/или обновляет метаданные.
    """
    with db.atomic():
        user, created = User.get_or_create(
            user_id=user_id,
            defaults={"username": username, "full_name": full_name},
        )
        # обновим метаданные
        updates = {}
        if username and username != user.username:
            updates["username"] = username
        if full_name and full_name != user.full_name:
            updates["full_name"] = full_name
        updates["last_seen"] = datetime.utcnow()
        if updates:
            User.update(**updates).where(User.user_id == user_id).execute()
        return user


def get_user(user_id: int) -> Optional[User]:
    return User.get_or_none(User.user_id == user_id)


def is_unlimited_active(user_id: int) -> Tuple[bool, Optional[datetime]]:
    u = User.get_or_none(User.user_id == user_id)
    if not u or u.plan != "unlimited" or not u.plan_until:
        return False, None
    return (u.plan_until > datetime.utcnow(), u.plan_until)


def set_unlimited(user_id: int, days: int) -> datetime:
    """
    Выдать безлимит на N дней: plan='unlimited', plan_until=now+days
    """
    until = datetime.utcnow() + timedelta(days=days)
    with db.atomic():
        ensure_user(user_id, None, None)
        User.update(plan="unlimited", plan_until=until).where(User.user_id == user_id).execute()
    return until


def unset_unlimited(user_id: int) -> None:
    """
    Снять безлимит.
    """
    with db.atomic():
        User.update(plan=None, plan_until=None).where(User.user_id == user_id).execute()


def free_used_this_month(user_id: int) -> int:
    mk = _month_key()
    return (
        Usage.select(fn.COUNT(Usage.id))
        .where((Usage.user == user_id) & (Usage.month_key == mk) & (Usage.kind == "free"))
        .scalar()
        or 0
    )


def record_usage(user_id: int, kind: str) -> None:
    """
    kind: 'free' | 'paid' | 'unlimited'
    """
    with db.atomic():
        ensure_user(user_id, None, None)
        Usage.create(user=user_id, month_key=_month_key(), kind=kind)


# ---------- поисковые запросы / чипсы ----------
def record_successful_search(user_id: int, role: str, city: str) -> None:
    role = (role or "").strip()
    city = (city or "").strip()
    if not role or not city:
        return
    with db.atomic():
        ensure_user(user_id, None, None)
        SearchQuery.create(user=user_id, role=role, city=city)


def get_recent_searches(user_id: int, limit: int = 20) -> List[SearchQuery]:
    if limit <= 0:
        return []
    query = (
        SearchQuery.select()
        .where(SearchQuery.user == user_id)
        .order_by(SearchQuery.created_at.desc())
        .limit(limit)
    )
    return list(query)


def get_trending_roles(since: datetime, limit: int, min_count: int) -> List[Tuple[str, int]]:
    if limit <= 0:
        return []
    query = (
        SearchQuery.select(SearchQuery.role, fn.COUNT(SearchQuery.id).alias("cnt"))
        .where((SearchQuery.created_at >= since) & (SearchQuery.role != ""))
        .group_by(SearchQuery.role)
        .having(fn.COUNT(SearchQuery.id) >= min_count)
        .order_by(fn.COUNT(SearchQuery.id).desc(), SearchQuery.role.asc())
        .limit(limit)
    )
    return [(row.role, row.cnt) for row in query]


def get_trending_cities(since: datetime, limit: int, min_count: int) -> List[Tuple[str, int]]:
    if limit <= 0:
        return []
    query = (
        SearchQuery.select(SearchQuery.city, fn.COUNT(SearchQuery.id).alias("cnt"))
        .where((SearchQuery.created_at >= since) & (SearchQuery.city != ""))
        .group_by(SearchQuery.city)
        .having(fn.COUNT(SearchQuery.id) >= min_count)
        .order_by(fn.COUNT(SearchQuery.id).desc(), SearchQuery.city.asc())
        .limit(limit)
    )
    return [(row.city, row.cnt) for row in query]


# ---------- кредиты ----------
def get_credits(user_id: int) -> int:
    c = Credit.get_or_none(Credit.user == user_id)
    return c.balance if c else 0


def add_credits(user_id: int, delta: int) -> int:
    with db.atomic():
        ensure_user(user_id, None, None)
        c, _ = Credit.get_or_create(user=user_id, defaults={"balance": 0})
        new_balance = max(0, c.balance + delta)
        Credit.update(balance=new_balance).where(Credit.id == c.id).execute()
        return new_balance


def consume_credit(user_id: int) -> bool:
    with db.atomic():
        c = Credit.get_or_none(Credit.user == user_id)
        if not c or c.balance <= 0:
            return False
        Credit.update(balance=c.balance - 1).where(Credit.id == c.id).execute()
        return True


# ---------- платежи ----------
def create_payment(
    user_id: int,
    pack: str,
    amount: int,
    currency: str = "RUB",
    payload: str = "",
) -> Payment:
    with db.atomic():
        ensure_user(user_id, None, None)
        return Payment.create(
            user=user_id,
            pack=pack,
            amount=amount,
            currency=currency,
            provider_payload=payload,
        )


def mark_payment_paid(payment_id: int) -> None:
    with db.atomic():
        Payment.update(status="paid", paid_at=datetime.utcnow()).where(Payment.id == payment_id).execute()


# ---------- admin-помощники ----------
def count_users(query: Optional[str] = None) -> int:
    """
    Сколько пользователей всего (или по поиску).
    Поиск по username / full_name / user_id.
    """
    q = User.select()
    if query:
        like = f"%{query}%"
        q = q.where(
            (User.username.contains(query)) |
            (User.full_name.contains(query)) |
            (User.user_id.cast("TEXT").contains(query))
        )
    return q.count()


def list_users(offset: int = 0, limit: int = 10, query: Optional[str] = None) -> List[User]:
    """
    Список пользователей для пагинации админки.
    """
    q = User.select()
    if query:
        q = q.where(
            (User.username.contains(query)) |
            (User.full_name.contains(query)) |
            (User.user_id.cast("TEXT").contains(query))
        )
    # сортировка: сначала последние по id (простая эвристика активности)
    q = q.order_by(User.user_id.desc()).offset(offset).limit(limit)
    return list(q)


def get_all_user_ids() -> List[int]:
    """
    Все telegram-id пользователей — для рассылки.
    """
    return [u.user_id for u in User.select(User.user_id)]
