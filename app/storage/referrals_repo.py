from __future__ import annotations

from datetime import datetime
import secrets
import string
from typing import Iterable, Optional

from peewee import fn

from .db import db
from .models import (
    Credit,
    Ledger,
    PromoCode,
    Referral,
    ReferralBan,
    ReferralStats,
    User,
)

_TOKEN_ALPHABET = string.ascii_letters + string.digits


def _generate_token(length: int = 10) -> str:
    while True:
        token = "".join(secrets.choice(_TOKEN_ALPHABET) for _ in range(length))
        exists = ReferralStats.select().where(ReferralStats.token == token).exists()
        if not exists:
            return token


def ensure_stats(user_id: int) -> ReferralStats:
    from . import repo  # локальный импорт, чтобы избежать циклов

    with db.atomic():
        repo.ensure_user(user_id, None, None)
        stats, created = ReferralStats.get_or_create(
            user=user_id,
            defaults={"token": _generate_token()},
        )
        if created:
            return stats
        if not stats.token:
            token = _generate_token()
            ReferralStats.update(token=token).where(ReferralStats.id == stats.id).execute()
            stats.token = token
        return stats


def get_stats(user_id: int) -> Optional[ReferralStats]:
    return ReferralStats.get_or_none(ReferralStats.user == user_id)


def get_token(user_id: int) -> str:
    stats = ensure_stats(user_id)
    return stats.token


def get_referral_by_invitee(invitee_id: int) -> Optional[Referral]:
    return Referral.get_or_none(Referral.invitee == invitee_id)


def find_referral_by_token(token: str) -> Optional[ReferralStats]:
    return ReferralStats.get_or_none(ReferralStats.token == token)


def create_referral(
    inviter_id: int,
    invitee_id: int,
    *,
    token: Optional[str],
    source: str,
    expires_at: Optional[datetime],
) -> Referral:
    from . import repo

    now = datetime.utcnow()
    with db.atomic():
        repo.ensure_user(inviter_id, None, None)
        repo.ensure_user(invitee_id, None, None)
        ensure_stats(inviter_id)
        referral = Referral.create(
            inviter=inviter_id,
            invitee=invitee_id,
            token=token,
            source=source,
            expires_at=expires_at,
            created_at=now,
        )
        ReferralStats.update(
            invited_count=ReferralStats.invited_count + 1,
            last_invited_at=now,
        ).where(ReferralStats.user == inviter_id).execute()
        return referral


def mark_referral_rejected(referral: Referral, reason: str) -> None:
    Referral.update(
        status="rejected",
        rejection_reason=reason,
    ).where(Referral.id == referral.id).execute()


def mark_referral_activated(referral: Referral) -> None:
    now = datetime.utcnow()
    Referral.update(
        status="activated",
        activated_at=now,
    ).where(Referral.id == referral.id).execute()
    ReferralStats.update(
        activated_count=ReferralStats.activated_count + 1,
    ).where(ReferralStats.user == referral.inviter_id).execute()


def count_bonuses_since(user_id: int, since: datetime) -> int:
    return (
        Ledger.select(fn.COUNT(Ledger.id))
        .where((Ledger.user == user_id) & (Ledger.ts >= since) & (Ledger.reason == "referral_inviter"))
        .scalar()
        or 0
    )


def count_total_bonuses(user_id: int) -> int:
    return (
        Ledger.select(fn.COUNT(Ledger.id))
        .where((Ledger.user == user_id) & (Ledger.reason == "referral_inviter"))
        .scalar()
        or 0
    )


def increment_bonuses(inviter_id: int, delta: int) -> None:
    now = datetime.utcnow()
    ReferralStats.update(
        bonuses_earned=ReferralStats.bonuses_earned + delta,
        last_bonus_at=now,
    ).where(ReferralStats.user == inviter_id).execute()


def grant_credit(user_id: int, delta: int, reason: str, referral: Optional[Referral] = None) -> int:
    from . import repo

    with db.atomic():
        repo.ensure_user(user_id, None, None)
        credit, _ = Credit.get_or_create(user=user_id, defaults={"balance": 0})
        new_balance = max(0, credit.balance + delta)
        Credit.update(balance=new_balance).where(Credit.id == credit.id).execute()
        Ledger.create(
            user=user_id,
            kind="credit",
            delta=delta,
            reason=reason,
            related_referral=referral,
            balance_after=new_balance,
        )
        return new_balance


def get_recent_rewards(user_id: int, limit: int = 10) -> Iterable[Ledger]:
    return (
        Ledger.select()
        .where(
            (Ledger.user == user_id)
            & (Ledger.reason.in_(["referral_inviter", "referral_invitee", "manual"]))
        )
        .order_by(Ledger.ts.desc())
        .limit(limit)
    )


def ensure_promocode(code: str) -> Optional[PromoCode]:
    return PromoCode.get_or_none(PromoCode.code == code)


def increment_promocode_usage(promo: PromoCode) -> None:
    PromoCode.update(uses=PromoCode.uses + 1).where(PromoCode.id == promo.id).execute()


def is_banned(user_id: int) -> bool:
    return ReferralBan.select().where(ReferralBan.user == user_id).exists()


def referral_summary() -> dict[str, int]:
    total_invited = Referral.select(fn.COUNT(Referral.id)).scalar() or 0
    activated = Referral.select(fn.COUNT(Referral.id)).where(Referral.status == "activated").scalar() or 0
    rejected = Referral.select(fn.COUNT(Referral.id)).where(Referral.status == "rejected").scalar() or 0
    bonuses = (
        Ledger.select(fn.SUM(Ledger.delta))
        .where(Ledger.reason == "referral_inviter")
        .scalar()
        or 0
    )
    return {
        "invited": total_invited,
        "activated": activated,
        "rejected": rejected,
        "bonuses": bonuses,
    }


def referral_top(limit: int = 10) -> Iterable[ReferralStats]:
    return (
        ReferralStats.select()
        .order_by(ReferralStats.activated_count.desc(), ReferralStats.invited_count.desc())
        .limit(limit)
    )


def get_referral(referral_id: int) -> Optional[Referral]:
    return Referral.get_or_none(Referral.id == referral_id)


def list_recent_referrals(limit: int = 20) -> Iterable[Referral]:
    return Referral.select().order_by(Referral.created_at.desc()).limit(limit)


def list_pending_referrals(limit: int = 20) -> Iterable[Referral]:
    return (
        Referral.select()
        .where(Referral.status == "pending")
        .order_by(Referral.created_at.asc())
        .limit(limit)
    )
