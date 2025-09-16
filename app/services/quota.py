from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional, Literal
from datetime import datetime
from app.storage.repo import (
    ensure_user, is_unlimited_active, free_used_this_month,
    record_usage, get_credits, consume_credit,
)

FREE_PER_MONTH = int(os.getenv("FREE_PER_MONTH", "3"))

Mode = Literal["unlimited", "free", "paid", "none"]

@dataclass
class QuotaDecision:
    allowed: bool
    mode: Mode
    free_left: int
    credits: int
    unlimited_until: Optional[datetime]
    message: str = ""

def check_and_consume(user_id: int, username: Optional[str], full_name: Optional[str]) -> QuotaDecision:
    # sync user meta + last_seen
    ensure_user(user_id, username, full_name)

    # 1) Unlimited plan?
    active, until = is_unlimited_active(user_id)
    if active:
        return QuotaDecision(True, "unlimited", free_left=0, credits=get_credits(user_id), unlimited_until=until)

    # 2) Free monthly
    used = free_used_this_month(user_id)
    if used < FREE_PER_MONTH:
        record_usage(user_id, "free")
        left = max(0, FREE_PER_MONTH - (used + 1))
        return QuotaDecision(True, "free", free_left=left, credits=get_credits(user_id), unlimited_until=None)

    # 3) Paid credits
    if consume_credit(user_id):
        return QuotaDecision(True, "paid", free_left=0, credits=get_credits(user_id), unlimited_until=None)

    # 4) No quota
    return QuotaDecision(
        False, "none",
        free_left=0,
        credits=get_credits(user_id),
        unlimited_until=None,
        message=(
            "Лимит исчерпан: 3 бесплатных запроса в месяц использованы и платных кредитов нет.\n"
            "Скоро добавим оплату — пока попробуй позже 🙏"
        ),
    )
