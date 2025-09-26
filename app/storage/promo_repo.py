from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, Optional, Sequence

from peewee import fn

from .db import db
from .models import PromoCreditCode, PromoRedemption, User


def get_by_normalized_code(code: str) -> Optional[PromoCreditCode]:
    return PromoCreditCode.get_or_none(PromoCreditCode.normalized_code == code)


def get_by_code(code: str) -> Optional[PromoCreditCode]:
    return PromoCreditCode.get_or_none(PromoCreditCode.code == code)


def create_code(
    *,
    code: str,
    normalized_code: str,
    bonus_credits: int,
    title: str | None,
    starts_at: datetime | None,
    expires_at: datetime | None,
    max_redemptions: int | None,
    created_by: int,
    meta: str | None = None,
) -> PromoCreditCode:
    with db.atomic():
        return PromoCreditCode.create(
            code=code,
            normalized_code=normalized_code,
            title=title,
            bonus_credits=bonus_credits,
            is_active=True,
            starts_at=starts_at,
            expires_at=expires_at,
            max_redemptions=max_redemptions,
            created_by=created_by,
            meta=meta,
        )


def list_codes(*, offset: int = 0, limit: int = 20, search: str | None = None) -> Sequence[PromoCreditCode]:
    query = PromoCreditCode.select().order_by(PromoCreditCode.created_at.desc())
    if search:
        needle = search.strip().upper()
        query = query.where(
            (PromoCreditCode.code.contains(needle))
            | (PromoCreditCode.normalized_code.contains(needle))
        )
        query = query.order_by(PromoCreditCode.code.asc())
    return list(query.offset(max(0, offset)).limit(max(0, limit)))


def count_codes(search: str | None = None) -> int:
    query = PromoCreditCode.select()
    if search:
        needle = search.strip().upper()
        query = query.where(
            (PromoCreditCode.code.contains(needle))
            | (PromoCreditCode.normalized_code.contains(needle))
        )
    return query.count()


def set_active(code: str, is_active: bool) -> bool:
    return bool(
        PromoCreditCode.update(is_active=is_active)
        .where(PromoCreditCode.code == code)
        .execute()
    )


def delete_code(code: str) -> bool:
    with db.atomic():
        promo = PromoCreditCode.get_or_none(PromoCreditCode.code == code)
        if not promo:
            return False
        if PromoRedemption.select().where(PromoRedemption.promo_code == code).exists():
            return False
        promo.delete_instance()
        return True


def user_has_redeemed(user_id: int, code: str) -> bool:
    return PromoRedemption.select().where(
        (PromoRedemption.user == user_id) & (PromoRedemption.promo_code == code)
    ).exists()


def reserve_redemption_slot(promo: PromoCreditCode) -> bool:
    update = (
        PromoCreditCode.update(redemptions_count=PromoCreditCode.redemptions_count + 1)
        .where(
            (PromoCreditCode.code == promo.code)
            & (
                (PromoCreditCode.max_redemptions.is_null())
                | (PromoCreditCode.redemptions_count < PromoCreditCode.max_redemptions)
            )
        )
        .execute()
    )
    if update:
        promo.redemptions_count += 1
    return bool(update)


def create_redemption(user_id: int, promo: PromoCreditCode, operation_id: str) -> PromoRedemption:
    User.get_or_create(user_id=user_id)
    return PromoRedemption.create(user=user_id, promo_code=promo.code, operation_id=operation_id)


def redemption_counts(promo: PromoCreditCode) -> dict[str, int]:
    now = datetime.utcnow()
    total = (
        PromoRedemption.select(fn.COUNT(PromoRedemption.id))
        .where(PromoRedemption.promo_code == promo.code)
        .scalar()
        or 0
    )
    last7 = (
        PromoRedemption.select(fn.COUNT(PromoRedemption.id))
        .where(
            (PromoRedemption.promo_code == promo.code)
            & (PromoRedemption.created_at >= now - timedelta(days=7))
        )
        .scalar()
        or 0
    )
    last30 = (
        PromoRedemption.select(fn.COUNT(PromoRedemption.id))
        .where(
            (PromoRedemption.promo_code == promo.code)
            & (PromoRedemption.created_at >= now - timedelta(days=30))
        )
        .scalar()
        or 0
    )
    return {"total": total, "last7": last7, "last30": last30}


def paginated_stats(*, limit: int = 10, offset: int = 0) -> Iterable[PromoCreditCode]:
    return (
        PromoCreditCode.select()
        .order_by(PromoCreditCode.created_at.desc())
        .offset(max(0, offset))
        .limit(max(0, limit))
    )


def search_codes(query: str, *, limit: int = 20) -> Sequence[PromoCreditCode]:
    if not query:
        return []
    token = query.strip().upper()
    return list(
        PromoCreditCode.select()
        .where(
            (PromoCreditCode.code.contains(token))
            | (PromoCreditCode.normalized_code.contains(token))
        )
        .order_by(PromoCreditCode.code.asc())
        .limit(max(1, limit))
    )
