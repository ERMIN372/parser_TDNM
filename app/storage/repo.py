from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, Tuple
from peewee import fn
from .db import db
from .models import User, Usage, Credit, Payment

def _month_key(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.utcnow()
    return dt.strftime("%Y-%m")

def ensure_user(user_id: int, username: Optional[str], full_name: Optional[str]) -> User:
    with db.atomic():
        user, created = User.get_or_create(
            user_id=user_id,
            defaults={
                "username": username,
                "full_name": full_name,
            },
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

def is_unlimited_active(user_id: int) -> Tuple[bool, Optional[datetime]]:
    u = User.get_or_none(User.user_id == user_id)
    if not u or u.plan != "unlimited" or not u.plan_until:
        return False, None
    return (u.plan_until > datetime.utcnow(), u.plan_until)

def set_unlimited(user_id: int, days: int) -> datetime:
    until = datetime.utcnow() + timedelta(days=days)
    with db.atomic():
        ensure_user(user_id, None, None)
        User.update(plan="unlimited", plan_until=until).where(User.user_id == user_id).execute()
    return until

def free_used_this_month(user_id: int) -> int:
    mk = _month_key()
    return (
        Usage.select(fn.COUNT(Usage.id))
        .where((Usage.user == user_id) & (Usage.month_key == mk) & (Usage.kind == "free"))
        .scalar()
        or 0
    )

def record_usage(user_id: int, kind: str) -> None:
    with db.atomic():
        ensure_user(user_id, None, None)
        Usage.create(user=user_id, month_key=_month_key(), kind=kind)

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

def create_payment(user_id: int, pack: str, amount: int, currency: str = "RUB", payload: str = "") -> Payment:
    with db.atomic():
        ensure_user(user_id, None, None)
        return Payment.create(
            user=user_id, pack=pack, amount=amount, currency=currency, provider_payload=payload
        )

def mark_payment_paid(payment_id: int) -> None:
    with db.atomic():
        Payment.update(status="paid", paid_at=datetime.utcnow()).where(Payment.id == payment_id).execute()
