from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Literal

from app.storage.repo import (
    consume_credit,
    ensure_user,
    free_used_this_month,
    get_credits,
    is_unlimited_active,
    record_usage,
)

FREE_PER_MONTH = int(os.getenv("FREE_PER_MONTH", "3"))

Mode = Literal["unlimited", "free", "paid", "none"]


@dataclass
class QuotaDecision:
    allowed: bool
    mode: Mode
    free_used: int
    free_left: int
    credits: int
    unlimited_until: Optional[datetime]


@dataclass
class QuotaUsageOutcome:
    mode: Mode
    free_used: int
    free_left: int
    credits: int
    unlimited_until: Optional[datetime]
    credits_delta: int = 0


def check_quota(user_id: int, username: Optional[str], full_name: Optional[str]) -> QuotaDecision:
    ensure_user(user_id, username, full_name)

    used = free_used_this_month(user_id)
    free_left = max(0, FREE_PER_MONTH - used)
    credits = get_credits(user_id)

    active, until = is_unlimited_active(user_id)
    if active:
        return QuotaDecision(True, "unlimited", free_used=used, free_left=free_left, credits=credits, unlimited_until=until)

    if credits > 0:
        return QuotaDecision(True, "paid", free_used=used, free_left=free_left, credits=credits, unlimited_until=None)

    if used < FREE_PER_MONTH:
        return QuotaDecision(
            True,
            "free",
            free_used=used,
            free_left=free_left,
            credits=credits,
            unlimited_until=None,
        )

    return QuotaDecision(False, "none", free_used=used, free_left=0, credits=credits, unlimited_until=None)


def commit_usage(user_id: int, decision: QuotaDecision) -> Optional[QuotaUsageOutcome]:
    if not decision.allowed:
        return None

    if decision.mode == "unlimited":
        credits = get_credits(user_id)
        used = free_used_this_month(user_id)
        free_left = max(0, FREE_PER_MONTH - used)
        active, until = is_unlimited_active(user_id)
        return QuotaUsageOutcome("unlimited", used, free_left, credits, until if active else None, credits_delta=0)

    if decision.mode == "free":
        record_usage(user_id, "free")
        used = free_used_this_month(user_id)
        free_left = max(0, FREE_PER_MONTH - used)
        credits = get_credits(user_id)
        return QuotaUsageOutcome("free", used, free_left, credits, None, credits_delta=0)

    if decision.mode == "paid":
        credits_delta = -1 if consume_credit(user_id) else 0
        if credits_delta:
            record_usage(user_id, "paid")
        used = free_used_this_month(user_id)
        free_left = max(0, FREE_PER_MONTH - used)
        credits = get_credits(user_id)
        return QuotaUsageOutcome("paid", used, free_left, credits, None, credits_delta=credits_delta)

    return None
