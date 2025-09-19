from __future__ import annotations

from aiogram import Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.services import payments
from app.services import referrals
from app.utils.logging import complete_operation, log_event, update_context


def _kb_packs() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("1 запрос — 49 ₽", callback_data="pay_create:p1"),
        InlineKeyboardButton("3 запроса — 139 ₽", callback_data="pay_create:p3"),
        InlineKeyboardButton("9 запросов — 399 ₽", callback_data="pay_create:p9"),
        InlineKeyboardButton("Безлимит 30 дней — 1299 ₽", callback_data="pay_create:unlim30"),
    )
    return kb


async def cmd_buy(message: types.Message):
    update_context(command="/buy")
    log_event("request_parsed", message="/buy", command="/buy")
    await message.reply("Выберите пакет:", reply_markup=_kb_packs())


async def cb_create(call: types.CallbackQuery):
    pack = call.data.split(":", 1)[1]
    update_context(command="pay_create", args={"pack": pack})
    log_event("request_parsed", message=f"pay_create {pack}", command="pay_create", args={"pack": pack})
    me = await call.bot.get_me()
    try:
        pid, url = payments.create_payment(call.from_user.id, pack, bot_username=me.username)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="create_payment failed")
        await call.answer("Ошибка создания платежа", show_alert=True)
        complete_operation(ok=False, err="payment_create_failed")
        return
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("💳 Оплатить", url=url),
        InlineKeyboardButton("✅ Проверить оплату", callback_data=f"pay_check:{pid}"),
    )
    await call.message.reply(
        f"Заказ оформлен: {payments.TITLES[pack]}.\nПосле оплаты жми «Проверить оплату».",
        reply_markup=kb,
    )
    await call.answer()


async def cb_check(call: types.CallbackQuery):
    pid = call.data.split(":", 1)[1]
    update_context(command="pay_check", args={"payment_id": pid})
    log_event("request_parsed", message=f"pay_check {pid}", command="pay_check", args={"payment_id": pid})
    try:
        msg, activation = payments.check_and_apply(call.from_user.id, pid)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="payment check failed")
        msg, activation = "Не удалось проверить оплату. Попробуйте позже.", None
        complete_operation(ok=False, err="payment_check_failed")
    await call.message.reply(msg)
    if activation:
        await _notify_referral_activation(call.bot, activation, call.from_user)
    await call.answer()


async def start_with_payload(message: types.Message):
    payload = (message.get_args() or "").strip()
    if not payload.startswith("paid_"):
        return
    pid = payload.replace("paid_", "", 1)
    update_context(command="start_payload", args={"payment_id": pid})
    log_event("request_parsed", message=f"/start payload {pid}", command="/start", args={"payment_id": pid})
    try:
        msg, activation = payments.check_and_apply(message.from_user.id, pid)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="payment check failed")
        msg, activation = "Не удалось проверить оплату. Попробуйте позже.", None
        complete_operation(ok=False, err="payment_check_failed")
    await message.reply(msg)
    if activation:
        await _notify_referral_activation(message.bot, activation, message.from_user)


async def _notify_referral_activation(bot, activation: referrals.ActivationResult, invitee: types.User | None) -> None:
    if not activation.inviter_id:
        return
    mention = _format_user_mention(invitee)
    if activation.granted and activation.bonus:
        text = f"🔥 Реферал {mention} активирован — +{activation.bonus} кредит начислен!"
    else:
        text = f"Реферал {mention} активировал триггер, но бонус не начислен (достигнут лимит)."

    try:
        await bot.send_message(activation.inviter_id, text)
    except Exception as exc:  # pragma: no cover
        log_event(
            "referral_notify_failed",
            level="WARN",
            inviter_id=activation.inviter_id,
            err=str(exc),
        )


def _format_user_mention(user: types.User | None) -> str:
    if not user:
        return "приглашённый"
    if user.username:
        return f"@{user.username}"
    if user.full_name:
        return user.full_name
    return str(getattr(user, "id", "приглашённый"))


def register(dp: Dispatcher):
    dp.register_message_handler(cmd_buy, commands=["buy"])
    dp.register_message_handler(cmd_buy, lambda m: m.text in {"💳 Купить", "Купить"}, state="*")
    dp.register_callback_query_handler(cb_create, lambda c: c.data and c.data.startswith("pay_create:"))
    dp.register_callback_query_handler(cb_check, lambda c: c.data and c.data.startswith("pay_check:"))
    dp.register_message_handler(start_with_payload, commands=["start"])
