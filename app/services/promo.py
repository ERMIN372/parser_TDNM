from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from peewee import IntegrityError

from app.storage import promo_repo, referrals_repo, repo
from app.storage.db import db
from app.storage.models import PromoCreditCode
from app.utils.logging import log_event, update_context

_CODE_RE = re.compile(r"^[A-Z0-9-]{1,64}$")


@dataclass
class PromoRedeemResult:
    ok: bool
    message: str
    new_balance: Optional[int]
    code: Optional[str] = None
    bonus: Optional[int] = None
    reason: Optional[str] = None


def normalize_code(raw: str) -> str:
    value = (raw or "").strip()
    if not value or any(ch.isspace() for ch in value):
        raise ValueError("empty")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("non_ascii") from exc
    value = value.upper()
    if not _CODE_RE.fullmatch(value):
        raise ValueError("bad_format")
    return value


def _promo_invalid(message: str, *, reason: str, code: str | None, raw: str) -> PromoRedeemResult:
    log_event(
        "promo_redeem_failed",
        message=message,
        reason=reason,
        code=code,
        raw_code=raw,
    )
    update_context(err=reason)
    return PromoRedeemResult(False, message, None, code=code, reason=reason)


def redeem_promo(user_id: int, raw_code: str) -> PromoRedeemResult:
    try:
        normalized = normalize_code(raw_code)
    except ValueError:
        return _promo_invalid("⚠️ Промокод указан неверно.", reason="invalid_format", code=None, raw=raw_code)

    promo: PromoCreditCode | None = promo_repo.get_by_normalized_code(normalized)
    now = datetime.utcnow()
    if not promo or not promo.is_active:
        return _promo_invalid(
            "⚠️ Промокод недействителен или срок действия истёк.",
            reason="not_found",
            code=normalized,
            raw=raw_code,
        )
    if promo.starts_at and now < promo.starts_at:
        return _promo_invalid(
            "⚠️ Промокод недействителен или срок действия истёк.",
            reason="not_started",
            code=promo.code,
            raw=raw_code,
        )
    if promo.expires_at and now > promo.expires_at:
        return _promo_invalid(
            "⚠️ Промокод недействителен или срок действия истёк.",
            reason="expired",
            code=promo.code,
            raw=raw_code,
        )

    if promo_repo.user_has_redeemed(user_id, promo.code):
        log_event(
            "promo_redeem_duplicate",
            message="Promo already redeemed",
            code=promo.code,
            user_id=user_id,
        )
        return PromoRedeemResult(False, "ℹ️ Этот промокод уже был активирован на ваш аккаунт.", None, code=promo.code, reason="already_redeemed")

    if promo.max_redemptions is not None and promo.redemptions_count >= promo.max_redemptions:
        return _promo_invalid(
            "⚠️ Лимит активаций по этому промокоду исчерпан.",
            reason="limit_reached",
            code=promo.code,
            raw=raw_code,
        )

    operation_id = f"promo:{promo.code}:{user_id}"

    try:
        with db.atomic():
            if promo_repo.user_has_redeemed(user_id, promo.code):
                log_event(
                    "promo_redeem_duplicate",
                    message="Promo already redeemed (tx)",
                    code=promo.code,
                    user_id=user_id,
                )
                return PromoRedeemResult(False, "ℹ️ Этот промокод уже был активирован на ваш аккаунт.", None, code=promo.code, reason="already_redeemed")
            if not promo_repo.reserve_redemption_slot(promo):
                return _promo_invalid(
                    "⚠️ Лимит активаций по этому промокоду исчерпан.",
                    reason="limit_reached",
                    code=promo.code,
                    raw=raw_code,
                )
            promo_repo.create_redemption(user_id, promo, operation_id)
            update_context(args={"promo_code": promo.code})
            repo.ensure_user(user_id, None, None)
            new_balance = referrals_repo.grant_credit(
                user_id,
                promo.bonus_credits,
                "promo_code",
                operation_id=operation_id,
            )
    except IntegrityError as exc:
        log_event(
            "promo_redeem_failed",
            level="ERROR",
            message="Integrity error during redemption",
            code=promo.code if promo else normalized,
            err=str(exc),
        )
        return PromoRedeemResult(False, "⚠️ Не удалось применить промокод. Попробуйте позже.", None, code=promo.code if promo else normalized, reason="db_error")
    except Exception as exc:  # pragma: no cover - defensive
        log_event(
            "promo_redeem_failed",
            level="ERROR",
            message="Unexpected error during redemption",
            code=promo.code if promo else normalized,
            err=str(exc),
        )
        return PromoRedeemResult(False, "⚠️ Не удалось применить промокод. Попробуйте позже.", None, code=promo.code if promo else normalized, reason="unexpected")

    update_context(credits_delta=promo.bonus_credits)
    log_event(
        "promo_redeem_success",
        message="Promo applied",
        code=promo.code,
        bonus=promo.bonus_credits,
        user_id=user_id,
        new_balance=new_balance,
    )
    text = (
        f"✅ Промокод {promo.code} активирован: +{promo.bonus_credits} кредитов. "
        f"Ваш баланс: {new_balance}."
    )
    return PromoRedeemResult(True, text, new_balance, code=promo.code, bonus=promo.bonus_credits)
