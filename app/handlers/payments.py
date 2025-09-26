from __future__ import annotations

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app import keyboards
from app.handlers import parse
from app.services import paywall, payments, referrals, promo
from app.utils.callbacks import safe_answer
from app.utils.logging import complete_operation, log_event, update_context
from app.utils.admins import is_admin


class PromoForm(StatesGroup):
    waiting_code = State()


def _resolve_pack(data: str) -> str | None:
    parts = data.split(":")
    if len(parts) < 3:
        return None
    if parts[1] == "pack":
        return {"1": "p1", "3": "p3", "9": "p9"}.get(parts[2])
    if parts[1] == "unlim" and parts[2] == "30":
        return "unlim30"
    return None


async def cmd_buy(message: types.Message):
    update_context(command="/buy")
    log_event("request_parsed", message="/buy", command="/buy")
    await message.reply(paywall.paywall_text(), reply_markup=paywall.paywall_keyboard())


async def _start_payment_flow(call: types.CallbackQuery, pack: str) -> None:
    update_context(command="buy_pack", args={"pack": pack})
    log_event("buy_cta_clicked", message=f"buy_cta {pack}", args={"pack": pack})

    pending = paywall.get_pending_payment(call.from_user.id)
    if pending and pending.pack == pack:
        await safe_answer(call, "Оплата уже открыта — проверь ссылку выше.")
        return

    me = await call.bot.get_me()
    try:
        pid, url = payments.create_payment(call.from_user.id, pack, bot_username=me.username)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="create_payment failed")
        await safe_answer(call, "Ошибка создания платежа", show_alert=True)
        complete_operation(ok=False, err="payment_create_failed")
        return

    paywall.set_pending_payment(call.from_user.id, pid, pack, url)

    price_text = paywall.pack_price_text(pack)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💳 Оплатить", url=url))
    kb.add(InlineKeyboardButton("✅ Проверить оплату", callback_data=f"pay_check:{pid}"))

    title = payments.TITLES.get(pack, pack)
    price_part = f" — {price_text}" if price_text else ""
    await call.message.answer(
        f"Заказ оформлен: {title}{price_part}.\nПосле оплаты жми «Проверить оплату».",
        reply_markup=kb,
    )
    await call.message.answer("Открыл оплату. После успешного платежа доступ появится автоматически.")
    await safe_answer(call)


async def cb_create(call: types.CallbackQuery):
    pack = call.data.split(":", 1)[1]
    update_context(command="pay_create", args={"pack": pack})
    log_event("request_parsed", message=f"pay_create {pack}", command="pay_create", args={"pack": pack})
    await _start_payment_flow(call, pack)


async def cb_buy_pack(call: types.CallbackQuery):
    pack = _resolve_pack(call.data)
    if not pack:
        await safe_answer(call)
        return
    await _start_payment_flow(call, pack)


async def cb_buy_open(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    await call.message.answer(paywall.paywall_text(), reply_markup=paywall.paywall_keyboard())


async def cb_buy_info(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    await call.message.answer(paywall.paywall_text(), reply_markup=paywall.paywall_keyboard())


async def cb_buy_back(call: types.CallbackQuery):
    if not await safe_answer(call):
        return
    await call.message.answer(
        "Главное меню:", reply_markup=keyboards.main_kb(is_admin=is_admin(call.from_user.id))
    )


async def cb_buy_promo(call: types.CallbackQuery, state: FSMContext):
    update_context(command="buy_promo")
    log_event("promo_redeem_prompt", message="Promo prompt opened")
    if not await safe_answer(call):
        return
    await state.set_state(PromoForm.waiting_code.state)
    await call.message.answer("Введите промокод одним сообщением (без пробелов).")


async def cb_check(call: types.CallbackQuery):
    pid = call.data.split(":", 1)[1]
    update_context(command="pay_check", args={"payment_id": pid})
    log_event("request_parsed", message=f"pay_check {pid}", command="pay_check", args={"payment_id": pid})
    try:
        msg, activation, status = payments.check_and_apply(call.from_user.id, pid)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="payment check failed")
        msg, activation, status = "Не удалось проверить оплату. Попробуйте позже.", None, "error"
        complete_operation(ok=False, err="payment_check_failed")
    await call.message.reply(msg)
    if activation:
        await _notify_referral_activation(call.bot, activation, call.from_user)
    if status != "pending":
        paywall.clear_pending_payment(call.from_user.id)
    if status == "succeeded":
        await parse.prompt_resume(call.bot, call.from_user.id)
    await safe_answer(call)


async def promo_code_entered(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    if text.lower() in {"/cancel", "отмена"}:
        await state.finish()
        await message.reply(
            "Отменено.",
            reply_markup=keyboards.main_kb(is_admin=is_admin(message.from_user.id)),
        )
        return

    update_context(command="promo_redeem", args={"raw": text})
    log_event("promo_redeem_request", message="Promo redeem requested", raw=text)
    result = promo.redeem_promo(message.from_user.id, text)
    reply_kb = keyboards.promo_result_kb()
    await message.reply(result.message, reply_markup=reply_kb)
    await state.finish()


async def start_with_payload(message: types.Message):
    payload = (message.get_args() or "").strip()
    if not payload.startswith("paid_"):
        return
    pid = payload.replace("paid_", "", 1)
    update_context(command="start_payload", args={"payment_id": pid})
    log_event("request_parsed", message=f"/start payload {pid}", command="/start", args={"payment_id": pid})
    try:
        msg, activation, status = payments.check_and_apply(message.from_user.id, pid)
    except Exception as exc:
        log_event("payment_failed", level="ERROR", err=str(exc), message="payment check failed")
        msg, activation, status = "Не удалось проверить оплату. Попробуйте позже.", None, "error"
        complete_operation(ok=False, err="payment_check_failed")
    await message.reply(msg)
    if activation:
        await _notify_referral_activation(message.bot, activation, message.from_user)
    if status != "pending":
        paywall.clear_pending_payment(message.from_user.id)
    if status == "succeeded":
        await parse.prompt_resume(message.bot, message.from_user.id)


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
    dp.register_message_handler(
        cmd_buy,
        lambda m: (m.text or "").strip() in {"💳 Купить", "Купить", "Купить ещё кредиты"},
        state="*",
    )
    dp.register_callback_query_handler(cb_buy_open, lambda c: c.data == "buy:open", state="*")
    dp.register_callback_query_handler(cb_buy_info, lambda c: c.data == "buy:info", state="*")
    dp.register_callback_query_handler(cb_buy_back, lambda c: c.data == "buy:back", state="*")
    dp.register_callback_query_handler(
        cb_buy_pack,
        lambda c: c.data and (c.data.startswith("buy:pack:") or c.data.startswith("buy:unlim:")),
        state="*",
    )
    dp.register_callback_query_handler(cb_buy_promo, lambda c: c.data == "buy:promo", state="*")
    dp.register_callback_query_handler(cb_create, lambda c: c.data and c.data.startswith("pay_create:"))
    dp.register_callback_query_handler(cb_check, lambda c: c.data and c.data.startswith("pay_check:"))
    dp.register_message_handler(start_with_payload, commands=["start"])
    dp.register_message_handler(promo_code_entered, state=PromoForm.waiting_code, content_types=types.ContentTypes.TEXT)
