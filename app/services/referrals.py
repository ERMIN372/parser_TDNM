from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from app.config import settings
from app.storage import repo
from app.storage import referrals_repo
from app.storage.models import Referral
from app.utils.logging import log_event, update_context


@dataclass
class StartResult:
    inviter_id: Optional[int] = None
    inviter_username: Optional[str] = None
    status: str = "none"
    message: Optional[str] = None
    invitee_bonus: int = 0


@dataclass
class ActivationResult:
    inviter_id: Optional[int] = None
    inviter_username: Optional[str] = None
    granted: bool = False
    bonus: int = 0
    reason: Optional[str] = None


def _normalize_payload(payload: str) -> str:
    return (payload or "").strip()


def _build_referral_dict(referral: Referral | None, **extra) -> dict:
    data = dict(extra)
    if referral:
        data.update(
            {
                "referral_id": referral.id,
                "inviter_id": referral.inviter_id,
                "invitee_id": referral.invitee_id,
                "status": referral.status,
                "source": referral.source,
            }
        )
    return data


def handle_start(
    invitee_id: int,
    payload: str,
    *,
    is_new: bool,
    username: Optional[str],
    full_name: Optional[str],
) -> StartResult:
    payload = _normalize_payload(payload)
    if not settings.REF_ENABLED:
        return StartResult(status="disabled")

    if not payload.startswith("ref_"):
        return StartResult(status="none")

    token = payload.replace("ref_", "", 1)
    stats = referrals_repo.find_referral_by_token(token)
    if not stats:
        log_event(
            "referral_rejected",
            level="WARN",
            reason="invalid_token",
            inviter_token=token,
            invitee_id=invitee_id,
        )
        return StartResult(status="rejected", message="Реферальная ссылка недействительна.")
    log_event(
        "referral_link_opened",
        inviter_id=stats.user_id,
        invitee_id=invitee_id,
        token=token,
    )

    inviter_id = stats.user_id
    if inviter_id == invitee_id:
        log_event(
            "referral_rejected",
            level="WARN",
            reason="self_ref",
            inviter_id=inviter_id,
            invitee_id=invitee_id,
        )
        return StartResult(status="rejected", message="Нельзя пригласить самого себя 🙃")

    if referrals_repo.is_banned(inviter_id) or referrals_repo.is_banned(invitee_id):
        log_event(
            "referral_rejected",
            level="WARN",
            reason="banned",
            inviter_id=inviter_id,
            invitee_id=invitee_id,
        )
        return StartResult(status="rejected", message="Реферальная программа недоступна для этой пары пользователей.")

    existing = referrals_repo.get_referral_by_invitee(invitee_id)
    if existing:
        if existing.inviter_id == inviter_id:
            inviter_user = repo.get_user(inviter_id)
            return StartResult(
                inviter_id=inviter_id,
                inviter_username=_format_username(getattr(inviter_user, "username", None)),
                status=existing.status,
            )
        log_event(
            "referral_rejected",
            level="WARN",
            reason="duplicate",
            inviter_id=inviter_id,
            invitee_id=invitee_id,
            prev_inviter=existing.inviter_id,
        )
        return StartResult(status="rejected", message="Промокод уже был использован ранее.")

    if not is_new:
        log_event(
            "referral_rejected",
            level="WARN",
            reason="not_new",
            inviter_id=inviter_id,
            invitee_id=invitee_id,
        )
        return StartResult(status="rejected", message="Реферальная ссылка доступна только новым пользователям.")

    ttl_hours = max(1, settings.REF_ATTRIBUTION_TTL_HOURS)
    expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)

    referral = referrals_repo.create_referral(
        inviter_id,
        invitee_id,
        token=token,
        source="deep_link",
        expires_at=expires_at,
    )
    update_context(referral=_build_referral_dict(referral))
    log_event(
        "referral_attributed",
        inviter_id=inviter_id,
        invitee_id=invitee_id,
        expires_at=str(expires_at),
    )

    invitee_bonus = 0
    if settings.REF_BONUS_INVITEE > 0:
        invitee_bonus = settings.REF_BONUS_INVITEE
        balance = referrals_repo.grant_credit(invitee_id, invitee_bonus, "referral_invitee", referral)
        update_context(credits_delta=invitee_bonus)
        log_event(
            "bonus_granted",
            user_id=invitee_id,
            amount=invitee_bonus,
            reason="referral_invitee",
            balance=balance,
        )

    inviter = repo.get_user(inviter_id)
    username_formatted = _format_username(getattr(inviter, "username", None))
    return StartResult(
        inviter_id=inviter_id,
        inviter_username=username_formatted,
        status="pending",
        invitee_bonus=invitee_bonus,
    )


def _format_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    if username.startswith("@"):
        return username
    return f"@{username}"


def handle_activation_trigger(invitee_id: int, trigger: str) -> Optional[ActivationResult]:
    if not settings.REF_ENABLED:
        return None

    referral = referrals_repo.get_referral_by_invitee(invitee_id)
    if not referral or referral.status != "pending":
        return None

    if referral.expires_at and datetime.utcnow() > referral.expires_at:
        referrals_repo.mark_referral_rejected(referral, "expired")
        log_event(
            "referral_rejected",
            level="WARN",
            reason="expired",
            inviter_id=referral.inviter_id,
            invitee_id=invitee_id,
            trigger=trigger,
        )
        return ActivationResult(inviter_id=referral.inviter_id, reason="expired")

    referrals_repo.mark_referral_activated(referral)
    inviter = repo.get_user(referral.inviter_id)
    username = _format_username(getattr(inviter, "username", None))

    bonus = max(0, settings.REF_BONUS_INVITER)
    granted = False
    if bonus > 0 and not referrals_repo.is_banned(referral.inviter_id):
        now = datetime.utcnow()
        daily = referrals_repo.count_bonuses_since(referral.inviter_id, now - timedelta(days=1))
        total = referrals_repo.count_total_bonuses(referral.inviter_id)
        if daily >= settings.REF_MAX_BONUS_PER_DAY or total >= settings.REF_MAX_BONUS_TOTAL:
            log_event(
                "referral_rejected",
                level="WARN",
                reason="bonus_limit",
                inviter_id=referral.inviter_id,
                invitee_id=invitee_id,
                daily=daily,
                total=total,
            )
        else:
            balance = referrals_repo.grant_credit(referral.inviter_id, bonus, "referral_inviter", referral)
            referrals_repo.increment_bonuses(referral.inviter_id, bonus)
            granted = True
            update_context(credits_delta=bonus)
            log_event(
                "bonus_granted",
                user_id=referral.inviter_id,
                amount=bonus,
                reason="referral_inviter",
                balance=balance,
                trigger=trigger,
            )
    log_event(
        "referral_activated",
        inviter_id=referral.inviter_id,
        invitee_id=invitee_id,
        trigger=trigger,
        granted=granted,
    )
    return ActivationResult(
        inviter_id=referral.inviter_id,
        inviter_username=username,
        granted=granted,
        bonus=bonus if granted else 0,
    )


def build_referral_link(bot_username: str, user_id: int) -> str:
    token = referrals_repo.get_token(user_id)
    username = bot_username.lstrip("@")
    return f"https://t.me/{username}?start=ref_{token}"


def get_user_stats(user_id: int) -> dict:
    stats = referrals_repo.get_stats(user_id)
    if not stats:
        return {
            "invited": 0,
            "activated": 0,
            "bonuses": 0,
        }
    return {
        "invited": stats.invited_count,
        "activated": stats.activated_count,
        "bonuses": stats.bonuses_earned,
    }


def list_recent_rewards(user_id: int, limit: int = 10):
    return referrals_repo.get_recent_rewards(user_id, limit)


def apply_promocode(invitee_id: int, code: str, *, is_new: bool) -> tuple[bool, str]:
    if not settings.REF_ENABLED:
        return False, "Реферальная программа временно недоступна."

    promo_code = code.strip().upper()
    promo = referrals_repo.ensure_promocode(promo_code)
    if not promo or not promo.is_active:
        return False, "Промокод не найден или не активен."

    now = datetime.utcnow()
    if promo.expires_at and promo.expires_at < now:
        return False, "Срок действия промокода истёк."
    if promo.max_uses is not None and promo.uses >= promo.max_uses:
        return False, "Промокод уже исчерпан."

    if not is_new:
        return False, "Промокод доступен только новым пользователям."

    user = repo.get_user(invitee_id)
    if not user:
        return False, "Пользователь не найден."
    if (now - user.created_at).total_seconds() > settings.REF_PROMO_TTL_HOURS * 3600:
        return False, "Увы, промокод можно применить только в течение первых часов после старта."

    if not promo.inviter_id:
        return False, "Промокод не привязан к приглашающему."
    inviter_id = promo.inviter_id
    if inviter_id == invitee_id:
        return False, "Нельзя применить свой же промокод."

    existing = referrals_repo.get_referral_by_invitee(invitee_id)
    if existing:
        return False, "Промокод уже был применён ранее."

    expires_at = now + timedelta(hours=settings.REF_ATTRIBUTION_TTL_HOURS)
    referral = referrals_repo.create_referral(
        inviter_id,
        invitee_id,
        token=promo_code,
        source="promo_code",
        expires_at=expires_at,
    )
    referrals_repo.increment_promocode_usage(promo)
    update_context(referral=_build_referral_dict(referral))
    log_event(
        "referral_attributed",
        inviter_id=inviter_id,
        invitee_id=invitee_id,
        source="promo_code",
    )

    invitee_bonus = 0
    if settings.REF_BONUS_INVITEE > 0:
        invitee_bonus = settings.REF_BONUS_INVITEE
        balance = referrals_repo.grant_credit(invitee_id, invitee_bonus, "referral_invitee", referral)
        update_context(credits_delta=invitee_bonus)
        log_event(
            "bonus_granted",
            user_id=invitee_id,
            amount=invitee_bonus,
            reason="referral_invitee",
            balance=balance,
        )

    return True, "Промокод применён! Добро пожаловать в программу."


def render_rules_text() -> str:
    return (
        "Зови друзей и получай бонусы 🎁\n"
        "Дружбану — +1 кредит, тебе — +1 после первого отчёта/оплаты.\n"
        f"Лимиты: до {settings.REF_MAX_BONUS_PER_DAY} бонусов/день, до {settings.REF_MAX_BONUS_TOTAL} всего."
    )


def admin_summary() -> dict:
    data = referrals_repo.referral_summary()
    top = list(referrals_repo.referral_top())
    pending = list(referrals_repo.list_pending_referrals())
    recent = list(referrals_repo.list_recent_referrals())
    return {"summary": data, "top": top, "pending": pending, "recent": recent}


def admin_get_referral(referral_id: int) -> Optional[Referral]:
    return referrals_repo.get_referral(referral_id)


def admin_referral_details(referral_id: int) -> Optional[dict]:
    referral = referrals_repo.get_referral(referral_id)
    if not referral:
        return None
    inviter = repo.get_user(referral.inviter_id)
    invitee = repo.get_user(referral.invitee_id)
    return {
        "id": referral.id,
        "inviter": inviter,
        "invitee": invitee,
        "status": referral.status,
        "created_at": referral.created_at,
        "activated_at": referral.activated_at,
        "source": referral.source,
        "token": referral.token,
        "reason": referral.rejection_reason,
    }


def admin_activate_referral(referral_id: int, *, grant_bonus: bool = True) -> tuple[bool, str]:
    referral = referrals_repo.get_referral(referral_id)
    if not referral:
        return False, "Реферал не найден."
    if referral.status == "activated":
        return False, "Реферал уже активирован."
    referrals_repo.mark_referral_activated(referral)
    if grant_bonus and settings.REF_BONUS_INVITER > 0:
        referrals_repo.grant_credit(referral.inviter_id, settings.REF_BONUS_INVITER, "referral_inviter", referral)
        referrals_repo.increment_bonuses(referral.inviter_id, settings.REF_BONUS_INVITER)
    log_event("referral_activated", inviter_id=referral.inviter_id, invitee_id=referral.invitee_id, manual=True)
    return True, "Реферал активирован."


def admin_reject_referral(referral_id: int, reason: str) -> tuple[bool, str]:
    referral = referrals_repo.get_referral(referral_id)
    if not referral:
        return False, "Реферал не найден."
    if referral.status == "rejected":
        return False, "Реферал уже отклонён."
    referrals_repo.mark_referral_rejected(referral, reason)
    log_event(
        "referral_rejected",
        inviter_id=referral.inviter_id,
        invitee_id=referral.invitee_id,
        reason=reason,
        manual=True,
    )
    return True, "Реферал отклонён."
