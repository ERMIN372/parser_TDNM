from __future__ import annotations
import logging
from aiogram import types, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.services import payments

log = logging.getLogger(__name__)

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
    await message.reply("Выберите пакет:", reply_markup=_kb_packs())

async def cb_create(call: types.CallbackQuery):
    pack = call.data.split(":", 1)[1]
    me = await call.bot.get_me()
    try:
        pid, url = payments.create_payment(call.from_user.id, pack, bot_username=me.username)
    except Exception as e:
        log.exception("create_payment failed")
        await call.answer("Ошибка создания платежа", show_alert=True)
        return
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("💳 Оплатить", url=url),
        InlineKeyboardButton("✅ Проверить оплату", callback_data=f"pay_check:{pid}"),
    )
    await call.message.reply(f"Заказ оформлен: {payments.TITLES[pack]}.\nПосле оплаты жми «Проверить оплату».", reply_markup=kb)
    await call.answer()

async def cb_check(call: types.CallbackQuery):
    pid = call.data.split(":", 1)[1]
    try:
        msg = payments.check_and_apply(call.from_user.id, pid)
    except Exception:
        msg = "Не удалось проверить оплату. Попробуйте позже."
    await call.message.reply(msg)
    await call.answer()

async def start_with_payload(message: types.Message):
    payload = (message.get_args() or "").strip()
    if not payload.startswith("paid_"):
        return
    pid = payload.replace("paid_", "", 1)
    try:
        msg = payments.check_and_apply(message.from_user.id, pid)
    except Exception:
        msg = "Не удалось проверить оплату. Попробуйте позже."
    await message.reply(msg)

def register(dp: Dispatcher):
    dp.register_message_handler(cmd_buy, commands=["buy"])
    # кнопка в главном меню
    dp.register_message_handler(cmd_buy, lambda m: m.text in {"💳 Купить", "Купить"}, state="*")
    dp.register_callback_query_handler(cb_create, lambda c: c.data and c.data.startswith("pay_create:"))
    dp.register_callback_query_handler(cb_check, lambda c: c.data and c.data.startswith("pay_check:"))
    dp.register_message_handler(start_with_payload, commands=["start"])
