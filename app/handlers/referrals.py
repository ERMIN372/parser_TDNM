from __future__ import annotations

from datetime import datetime
from urllib.parse import quote_plus

from aiogram import Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.services import referrals
from app.storage import repo
from app.utils.logging import log_event, update_context


def _kb_referrals(link: str) -> InlineKeyboardMarkup:
    share_text = quote_plus("HR-Assist — мой бот для вакансий. Держи ссылку: ")
    share_url = f"https://t.me/share/url?url={quote_plus(link)}&text={share_text}"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📋 Скопировать", callback_data="ref_copy"))
    kb.add(InlineKeyboardButton("📤 Поделиться", url=share_url))
    return kb


async def cmd_referrals(message: types.Message):
    update_context(command="/referrals")
    log_event("request_parsed", message="/referrals", command="/referrals")
    me = await message.bot.get_me()
    link = referrals.build_referral_link(me.username or "", message.from_user.id)
    stats = referrals.get_user_stats(message.from_user.id)
    rules = referrals.render_rules_text()
    text = (
        f"{rules}\n\n"
        f"Твоя ссылка: <code>{link}</code>\n"
        f"Приглашено: {stats['invited']}\n"
        f"Активировано: {stats['activated']}\n"
        f"Получено бонусов: {stats['bonuses']}"
    )
    update_context(referral={"link": link, "stats": stats})
    await message.reply(text, reply_markup=_kb_referrals(link))


async def cb_ref_copy(call: types.CallbackQuery):
    me = await call.bot.get_me()
    link = referrals.build_referral_link(me.username or "", call.from_user.id)
    await call.answer(link, show_alert=True)


async def cmd_promo(message: types.Message):
    update_context(command="/promo")
    log_event("request_parsed", message="/promo", command="/promo")
    code = (message.get_args() or "").strip()
    if not code:
        await message.reply("Введите промокод: /promo REF-XXXX")
        return

    user = repo.get_user(message.from_user.id)
    if not user:
        repo.ensure_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
        user = repo.get_user(message.from_user.id)
    is_new = False
    if user:
        age_hours = (datetime.utcnow() - user.created_at).total_seconds() / 3600
        is_new = age_hours <= settings.REF_PROMO_TTL_HOURS
    ok, text = referrals.apply_promocode(message.from_user.id, code, is_new=is_new)
    await message.reply(text)


async def cmd_rewards(message: types.Message):
    update_context(command="/rewards")
    log_event("request_parsed", message="/rewards", command="/rewards")
    entries = referrals.list_recent_rewards(message.from_user.id, limit=10)
    lines = ["Последние начисления:"]
    empty = True
    for entry in entries:
        empty = False
        sign = "+" if entry.delta >= 0 else ""
        ts = entry.ts.strftime("%Y-%m-%d %H:%M")
        lines.append(f"{ts} — {sign}{entry.delta} ({entry.reason})")
    if empty:
        lines.append("Пока начислений нет.")
    await message.reply("\n".join(lines))


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_referrals, commands=["referrals"])
    dp.register_message_handler(cmd_referrals, lambda m: (m.text or "").strip() in {"🎁 Рефералы", "Рефералы"}, state="*")
    dp.register_callback_query_handler(cb_ref_copy, lambda c: c.data == "ref_copy")
    dp.register_message_handler(cmd_promo, commands=["promo"])
    dp.register_message_handler(cmd_rewards, commands=["rewards"])
    dp.register_message_handler(cmd_rewards, lambda m: (m.text or "").strip() in {"🏆 Начисления", "Начисления"}, state="*")
