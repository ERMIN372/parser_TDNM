from __future__ import annotations

import os
import uuid
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Tuple

from yookassa import Configuration, Payment

from app.storage import repo
from app.utils.logging import log_event, update_context
from . import referrals


def _price_from_env(var: str, default_cop: int) -> int:
    raw = os.getenv(var)
    if not raw:
        return default_cop
    try:
        amount = Decimal(str(raw).replace(" ", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return default_cop
    if amount < 0:
        return default_cop
    value = (amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(value)


# Прайс-лист (копейки)
PRICES = {
    "p1": _price_from_env("PRICE_PACK_1", 49_00),        # 1 запрос
    "p3": _price_from_env("PRICE_PACK_3", 139_00),       # 3 запроса
    "p9": _price_from_env("PRICE_PACK_9", 399_00),       # 9 запросов
    "unlim30": _price_from_env("PRICE_PACK_UNLIM30", 1_299_00),
}

TITLES = {
    "p1": "1 запрос",
    "p3": "3 запроса",
    "p9": "9 запросов",
    "unlim30": "Безлимит 30 дней",
}

# порядок отображения пакетов
PACK_ORDER = ["p1", "p3", "p9", "unlim30"]


def _credits_delta(pack: str) -> int:
    return {"p1": 1, "p3": 3, "p9": 9}.get(pack, 0)


def _rub(amount_cop: int) -> str:
    return f"{amount_cop/100:.2f}"

def _apply_effect(user_id: int, pack: str) -> Tuple[str, Optional[referrals.ActivationResult]]:
    """Применить покупку к аккаунту."""
    if pack == "p1":
        bal = repo.add_credits(user_id, 1)
        return _finish_payment(user_id, "Зачислено 1 кредит. Баланс: {bal}")
    if pack == "p3":
        bal = repo.add_credits(user_id, 3)
        return _finish_payment(user_id, f"Зачислено 3 кредита. Баланс: {bal}")
    if pack == "p9":
        bal = repo.add_credits(user_id, 9)
        return _finish_payment(user_id, f"Зачислено 9 кредитов. Баланс: {bal}")
    if pack == "unlim30":
        until = repo.set_unlimited(user_id, 30)
        return _finish_payment(user_id, f"Включён безлимит до {until:%Y-%m-%d %H:%M} UTC")
    return _finish_payment(user_id, "Ок")


def _finish_payment(user_id: int, message: str) -> Tuple[str, Optional[referrals.ActivationResult]]:
    activation = referrals.handle_activation_trigger(user_id, "payment")
    return message, activation

_configured = False


def _cfg():
    global _configured
    if _configured:
        return

    shop_id = os.getenv("YOOKASSA_SHOP_ID")
    secret = os.getenv("YOOKASSA_SECRET_KEY")
    if not shop_id or not secret:
        raise RuntimeError("Платёжная система не настроена (YOOKASSA_SHOP_ID/YOOKASSA_SECRET_KEY).")

    Configuration.configure(account_id=shop_id, secret_key=secret)
    _configured = True

def create_payment(user_id: int, pack: str, bot_username: str | None = None) -> Tuple[str, str]:
    """
    Создаёт платёж в ЮKassa.
    Возвращает (payment_id, confirmation_url).
    """
    if pack not in PRICES:
        raise ValueError("Неизвестный пакет.")
    _cfg()

    amount_cop = PRICES[pack]
    description = f"HR-Assist — {TITLES[pack]} (uid {user_id})"
    idem = str(uuid.uuid4())

    # Формируем return_url (deeplink в бота)
    base = os.getenv("RETURN_URL_BASE")
    if not base:
        if not bot_username:
            raise RuntimeError("Не задан RETURN_URL_BASE и неизвестен username бота.")
        base = f"https://t.me/{bot_username}"
    return_url = f"{base}?start=paid_{idem}"

    # Создаём платёж
    body = {
        "amount": {"value": _rub(amount_cop), "currency": "RUB"},
        "capture": True,
        "description": description,
        "confirmation": {"type": "redirect", "return_url": return_url},
        "metadata": {
            "user_id": user_id,
            "pack": pack,
            "internal_payment_id": idem,
        },
        # Чек (фискализация) опускаем в MVP — зависит от настроек магазина
    }
    p = Payment.create(body, idempotency_key=idem)
    # Сохраним pending в нашей БД
    payment = repo.create_payment(user_id, pack, amount_cop, "RUB", payload=p.id)
    payment_ctx = {
        "id": p.id,
        "status": "pending",
        "pack": pack,
        "amount": _rub(amount_cop),
    }
    update_context(payment=payment_ctx)
    log_event("payment_created", message=f"payment {p.id} created", payment=payment_ctx)
    return p.id, p.confirmation.confirmation_url

def check_and_apply(user_id: int, payment_id: str) -> Tuple[str, Optional[referrals.ActivationResult], str]:
    """
    Проверяет статус платежа в ЮKassa.
    Если оплачен — отмечает как paid и применяет эффект на аккаунт.
    Возвращает человекочитаемый результат.
    """
    _cfg()
    p = Payment.find_one(payment_id)
    status = getattr(p, "status", "unknown")
    update_context(payment={"id": payment_id, "status": status})

    # найдём нашу запись
    from app.storage.models import Payment as DbPayment  # локальный импорт
    rec = DbPayment.get_or_none(DbPayment.provider_payload == payment_id)
    if not rec:
        return "Платёж не найден в системе.", None, status

    if status == "succeeded":
        if rec.status != "paid":
            repo.mark_payment_paid(rec.id)
            msg, activation = _apply_effect(user_id, rec.pack)
            delta = _credits_delta(rec.pack)
            if delta:
                update_context(credits_delta=delta)
            if rec.pack == "unlim30":
                update_context(quota={"unlimited": True})
            log_event(
                "payment_succeeded",
                message=f"payment {payment_id} succeeded",
                payment={"id": payment_id, "status": "succeeded", "pack": rec.pack},
            )
            return f"✅ Оплата прошла. {msg}", activation, "succeeded"
        else:
            log_event(
                "payment_succeeded",
                message=f"payment {payment_id} already applied",
                payment={"id": payment_id, "status": "succeeded", "pack": rec.pack},
            )
            return "✅ Этот платёж уже учтён.", None, "succeeded"
    elif status in {"canceled", "waiting_for_capture"}:
        log_event(
            "payment_failed",
            level="WARN",
            message=f"payment {payment_id} status={status}",
            payment={"id": payment_id, "status": status, "pack": rec.pack},
        )
        return f"Статус платежа: {status}. Если считаете, что это ошибка — напишите нам.", None, status
    else:
        log_event(
            "payment_failed",
            level="WARN",
            message=f"payment {payment_id} status={status}",
            payment={"id": payment_id, "status": status, "pack": rec.pack},
        )
        return f"Статус платежа: {status}. Ещё не оплачено.", None, status
