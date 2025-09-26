from __future__ import annotations
import asyncio
from datetime import datetime
from typing import List, Tuple

from aiogram import types, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, MessageNotModified
from pathlib import Path

from app.storage import repo
from app.storage.models import User
from app.services import referrals as referral_service
from app.utils.backup import make_sqlite_backup
from app.utils.admins import is_admin
from app.utils.callbacks import safe_answer

# --- доступ ---
def _guard(uid: int) -> bool: return is_admin(uid)

# --- пагинация ---
PAGE_SIZE = 10

def _kb_admin_home() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="admin_users:1"),
        InlineKeyboardButton("📣 Рассылка", callback_data="admin_cast"),
    )
    kb.add(InlineKeyboardButton("🎯 Рефералы", callback_data="admin_ref"))
    kb.add(InlineKeyboardButton("💾 Бэкап БД", callback_data="admin_backup"))
    return kb

async def _safe_edit_text(message: types.Message, text: str, **kwargs) -> None:
    try:
        await message.edit_text(text, **kwargs)
    except MessageNotModified:
        pass


# message_id -> целевой user_id для точечной рассылки
_CAST_TARGETS: dict[int, int] = {}


def _users_page(page: int, q: str | None = None) -> Tuple[str, InlineKeyboardMarkup]:
    total = repo.count_users(q)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, pages))
    users: List[User] = repo.list_users(offset=(page-1)*PAGE_SIZE, limit=PAGE_SIZE, query=q)

    lines = [f"👥 Пользователи — страница {page}/{pages} (всего: {total})"]
    kb = InlineKeyboardMarkup(row_width=1)
    for u in users:
        credits = repo.get_credits(u.user_id)
        active, until = repo.is_unlimited_active(u.user_id)
        tag = "♾" if active else f"{credits}💳"
        title = f"{u.user_id} • @{u.username or '-'} • {u.full_name or '-'} • {tag}"
        kb.add(InlineKeyboardButton(title[:64], callback_data=f"admin_user:{u.user_id}"))

    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_users:{page-1}"))
    nav.append(InlineKeyboardButton("🔄", callback_data=f"admin_users:{page}"))
    if page < pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_users:{page+1}"))
    if nav: kb.row(*nav)

    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_home"))
    return ("\n".join(lines), kb)

def _kb_user(u: User) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.row(
        InlineKeyboardButton("♾ 7д", callback_data=f"admin_unlim:{u.user_id}:7"),
        InlineKeyboardButton("♾ 30д", callback_data=f"admin_unlim:{u.user_id}:30"),
        InlineKeyboardButton("♾ 90д", callback_data=f"admin_unlim:{u.user_id}:90"),
    )
    kb.row(
        InlineKeyboardButton("+1💳", callback_data=f"admin_credit:{u.user_id}:1"),
        InlineKeyboardButton("+3💳", callback_data=f"admin_credit:{u.user_id}:3"),
        InlineKeyboardButton("+9💳", callback_data=f"admin_credit:{u.user_id}:9"),
    )
    kb.row(
        InlineKeyboardButton("🔔 Точечная рассылка", callback_data=f"admin_cast_user:{u.user_id}"),
    )
    kb.add(InlineKeyboardButton("⬅️ К списку", callback_data="admin_users:1"))
    return kb

def _user_card_text(u: User) -> str:
    credits = repo.get_credits(u.user_id)
    active, until = repo.is_unlimited_active(u.user_id)
    unlim = f"да, до {until:%Y-%m-%d %H:%M} UTC" if active and until else "нет"
    return (
        "👤 <b>Пользователь</b>\n"
        f"ID: <code>{u.user_id}</code>\n"
        f"Username: @{u.username or '-'}\n"
        f"Имя: {u.full_name or '-'}\n"
        f"Кредиты: {credits}\n"
        f"Безлимит: {unlim}\n"
    )

# -------- корневое меню / кнопка --------
async def admin_home(message: types.Message):
    if not _guard(message.from_user.id): return
    await message.reply("🛠 Админ-панель", reply_markup=_kb_admin_home())

async def cb_admin_home(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    await _safe_edit_text(call.message, "🛠 Админ-панель", reply_markup=_kb_admin_home())
    if not await safe_answer(call):
        return

# -------- список пользователей --------
async def cb_users(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, payload = call.data.split(":")
    page = int(payload)
    text, kb = _users_page(page)
    await _safe_edit_text(call.message, text, reply_markup=kb)
    if not await safe_answer(call):
        return

# -------- карточка пользователя --------
async def cb_user(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid = call.data.split(":")
    u = repo.get_user(int(uid))
    if not u:
        await safe_answer(call, "Пользователь не найден", show_alert=True)
        return
    await _safe_edit_text(call.message, _user_card_text(u), reply_markup=_kb_user(u), parse_mode="HTML")
    if not await safe_answer(call):
        return


def _render_referral_summary() -> tuple[str, InlineKeyboardMarkup]:
    data = referral_service.admin_summary()
    summary = data["summary"]
    lines = [
        "🎯 <b>Реферальная программа</b>",
        f"Приглашено: {summary['invited']}",
        f"Активировано: {summary['activated']}",
        f"Отклонено: {summary['rejected']}",
        f"Выдано бонусов: {summary['bonuses']}",
        "",
    ]
    top = data["top"]
    if top:
        lines.append("Топ-10 по активациям:")
        for stats in top:
            u = repo.get_user(stats.user_id)
            name = f"@{u.username}" if u and u.username else str(stats.user_id)
            lines.append(
                f"• {name}: приглашено {stats.invited_count}, активировано {stats.activated_count}, бонусы {stats.bonuses_earned}"
            )
        lines.append("")
    pending = data["pending"]
    kb = InlineKeyboardMarkup(row_width=1)
    if pending:
        lines.append("Ожидают активации:")
        for ref in pending[:10]:
            invitee = repo.get_user(ref.invitee_id)
            invitee_name = f"@{invitee.username}" if invitee and invitee.username else str(ref.invitee_id)
            lines.append(f"• #{ref.id} — {invitee_name} (от {ref.created_at:%Y-%m-%d %H:%M})")
            kb.add(InlineKeyboardButton(f"🔍 #{ref.id}", callback_data=f"admin_referral:{ref.id}"))
        lines.append("")
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="admin_ref"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_home"))
    return "\n".join(lines), kb


async def cb_ref_summary(call: types.CallbackQuery):
    if not _guard(call.from_user.id):
        return
    text, kb = _render_referral_summary()
    await _safe_edit_text(call.message, text, reply_markup=kb, parse_mode="HTML")
    if not await safe_answer(call):
        return


def _format_user(user: User | None) -> str:
    if not user:
        return "-"
    if user.username:
        return f"@{user.username} ({user.user_id})"
    return f"{user.full_name or '-'} ({user.user_id})"


def _kb_referral(referral_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Активировать", callback_data=f"admin_referral_activate:{referral_id}"),
        InlineKeyboardButton("🚫 Отклонить", callback_data=f"admin_referral_reject:{referral_id}"),
    )
    kb.add(InlineKeyboardButton("⬅️ К списку", callback_data="admin_ref"))
    return kb


async def cb_referral_card(call: types.CallbackQuery):
    if not _guard(call.from_user.id):
        return
    _, rid = call.data.split(":")
    rid_int = int(rid)
    details = referral_service.admin_referral_details(rid_int)
    if not details:
        await safe_answer(call, "Реферал не найден", show_alert=True)
        return
    inviter = _format_user(details["inviter"])
    invitee = _format_user(details["invitee"])
    text = (
        "🔍 <b>Реферал</b>\n"
        f"ID: <code>{details['id']}</code>\n"
        f"Пригласивший: {inviter}\n"
        f"Приглашённый: {invitee}\n"
        f"Статус: {details['status']}\n"
        f"Создан: {details['created_at']:%Y-%m-%d %H:%M} UTC\n"
    )
    if details.get("activated_at"):
        text += f"Активирован: {details['activated_at']:%Y-%m-%d %H:%M} UTC\n"
    if details.get("reason"):
        text += f"Причина: {details['reason']}\n"
    text += f"Источник: {details['source']}\n"
    await _safe_edit_text(call.message, text, reply_markup=_kb_referral(rid_int), parse_mode="HTML")
    if not await safe_answer(call):
        return


async def cb_referral_activate(call: types.CallbackQuery):
    if not _guard(call.from_user.id):
        return
    _, rid = call.data.split(":")
    ok, msg = referral_service.admin_activate_referral(int(rid))
    if not await safe_answer(call, msg, show_alert=not ok):
        return
    if ok:
        await cb_referral_card(call)


async def cb_referral_reject(call: types.CallbackQuery):
    if not _guard(call.from_user.id):
        return
    _, rid = call.data.split(":")
    ok, msg = referral_service.admin_reject_referral(int(rid), reason="manual_reject")
    if not await safe_answer(call, msg, show_alert=not ok):
        return
    if ok:
        await cb_referral_card(call)

# -------- действия: безлимит/кредиты --------
async def cb_unlim(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid, days = call.data.split(":")
    uid, days = int(uid), int(days)
    until = repo.set_unlimited(uid, days)
    if not await safe_answer(call, "Выдан безлимит", show_alert=False):
        return
    u = repo.get_user(uid)
    await _safe_edit_text(call.message, _user_card_text(u), reply_markup=_kb_user(u), parse_mode="HTML")

async def cb_credit(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid, n = call.data.split(":")
    uid, n = int(uid), int(n)
    bal = repo.add_credits(uid, n)
    if not await safe_answer(call, f"+{n} кредит(ов). Баланс: {bal}", show_alert=False):
        return
    u = repo.get_user(uid)
    await _safe_edit_text(call.message, _user_card_text(u), reply_markup=_kb_user(u), parse_mode="HTML")

# -------- рассылки --------
async def cb_cast_menu(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📣 Разослать всем (ответом на сообщение)", callback_data="admin_cast_all"),
        InlineKeyboardButton("🔔 Точечно (по ID)", callback_data="admin_cast_prompt"),
        InlineKeyboardButton("⬅️ Назад", callback_data="admin_home"),
    )
    await _safe_edit_text(
        call.message,
        "Рассылка:\n— отправь текст ответом на это сообщение\n— или используй точечную по ID",
        reply_markup=kb,
    )
    if not await safe_answer(call):
        return

# 0) точечная из карточки пользователя (КНОПКА, которая не работала)
async def cb_cast_user(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid = call.data.split(":")
    uid = int(uid)
    prompt = (
        f"Точечная рассылка пользователю <code>{uid}</code>.\n"
        "Ответь на это сообщение текстом — мы перешлём его пользователю.\n"
        "Отмена: /cancel"
    )
    await _safe_edit_text(call.message, prompt, parse_mode="HTML")
    _CAST_TARGETS[call.message.message_id] = uid
    if not await safe_answer(call):
        return

# ловим ответ на «точечная рассылка пользователю <uid>»
async def catch_reply_cast_user(message: types.Message):
    if not _guard(message.from_user.id): return
    if not message.reply_to_message: return
    uid = _CAST_TARGETS.get(message.reply_to_message.message_id)
    if uid is None:
        return
    text = message.html_text or message.text or ""
    if not text.strip():
        await message.reply("Пустое сообщение, нечего отправлять.")
        return
    try:
        await message.bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
        await message.reply(f"✅ Отправлено пользователю {uid}")
    except (BotBlocked, ChatNotFound):
        await message.reply("❌ Не удалось: бот заблокирован или чат не найден.")
    except RetryAfter as e:
        await asyncio.sleep(e.timeout + 0.5)
        try:
            await message.bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
            await message.reply(f"✅ Отправлено пользователю {uid} (после задержки)")
        except Exception:
            await message.reply("❌ Повторная попытка не удалась.")
    except Exception as e:
        await message.reply(f"❌ Ошибка отправки: {e}")
    finally:
        _CAST_TARGETS.pop(message.reply_to_message.message_id, None)

# 1) ответом на сообщение → всем
async def cb_cast_all(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    if not await safe_answer(call, "Пришли текст рассылки ответом на это сообщение."):
        return
    await _safe_edit_text(
        call.message,
        "Ответь на это сообщение текстом для рассылки всем пользователям.\nОтмена: /cancel",
    )

async def catch_reply_broadcast_all(message: types.Message):
    if not _guard(message.from_user.id): return
    if not message.reply_to_message: return
    src = (message.reply_to_message.text or "") + (message.reply_to_message.caption or "")
    if "текстом для рассылки всем пользователям" not in src:
        return
    text = message.html_text or message.text
    ids = repo.get_all_user_ids()
    sent = ok = fail = 0
    for uid in ids:
        try:
            await message.bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
            ok += 1
        except (BotBlocked, ChatNotFound):
            fail += 1
        except RetryAfter as e:
            await asyncio.sleep(e.timeout + 0.5)
            continue
        except Exception:
            fail += 1
        finally:
            sent += 1
            await asyncio.sleep(0.05)
    await message.reply(f"Готово. Отправлено: {sent}, успешно: {ok}, ошибок: {fail}")

# 2) точечно / по ID через команду
async def cb_cast_prompt(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    await _safe_edit_text(
        call.message,
        "Пришли в чат команду:\n<code>/cast &lt;id1,id2,...&gt; текст</code>\n"
        "Пример: <code>/cast 123,456 Обновили бота — теперь быстрее!</code>\n"
        "Отмена: /cancel",
        parse_mode="HTML",
    )
    if not await safe_answer(call):
        return

async def cast_cmd(message: types.Message):
    if not _guard(message.from_user.id): return
    if not message.text.startswith("/cast "):
        return
    try:
        _, rest = message.text.split(" ", 1)
        ids_str, text = rest.split(" ", 1)
        ids = [int(x) for x in ids_str.split(",") if x.strip().isdigit()]
    except Exception:
        await message.reply("Формат: /cast <id1,id2,...> <текст>")
        return

    ok = fail = 0
    for uid in ids:
        try:
            await message.bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await message.reply(f"Отправлено: {ok}, ошибок: {fail}")

# -------- бэкап БД --------
async def cb_backup(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = Path(f"backups/botdb_{ts}.zip")
    try:
        z = make_sqlite_backup(out)
        await call.message.reply_document(InputFile(z), caption=f"Бэкап {z.name}")
    except Exception as e:
        await call.message.reply(f"Не удалось сделать бэкап: {e}")
    await safe_answer(call, "Готово")

# -------- регистрация --------
def register(dp: Dispatcher):
    # вход в админку
    dp.register_message_handler(admin_home, commands=["admin"])
    dp.register_message_handler(admin_home, lambda m: m.text in {"🛠 Админ", "Админ"}, state="*")

    # список/карточки/правки
    dp.register_callback_query_handler(cb_admin_home, lambda c: c.data == "admin_home")
    dp.register_callback_query_handler(cb_users, lambda c: c.data and c.data.startswith("admin_users:"))
    dp.register_callback_query_handler(cb_user,  lambda c: c.data and c.data.startswith("admin_user:"))
    dp.register_callback_query_handler(cb_unlim, lambda c: c.data and c.data.startswith("admin_unlim:"))
    dp.register_callback_query_handler(cb_credit, lambda c: c.data and c.data.startswith("admin_credit:"))
    dp.register_callback_query_handler(cb_ref_summary, lambda c: c.data == "admin_ref")
    dp.register_callback_query_handler(cb_referral_card, lambda c: c.data and c.data.startswith("admin_referral:"))
    dp.register_callback_query_handler(cb_referral_activate, lambda c: c.data and c.data.startswith("admin_referral_activate:"))
    dp.register_callback_query_handler(cb_referral_reject, lambda c: c.data and c.data.startswith("admin_referral_reject:"))

    # рассылки
    dp.register_callback_query_handler(cb_cast_menu,  lambda c: c.data == "admin_cast")
    dp.register_callback_query_handler(cb_cast_all,   lambda c: c.data == "admin_cast_all")
    dp.register_callback_query_handler(cb_cast_prompt,lambda c: c.data == "admin_cast_prompt")
    dp.register_callback_query_handler(cb_cast_user,  lambda c: c.data and c.data.startswith("admin_cast_user:"))
    dp.register_message_handler(
        catch_reply_broadcast_all,
        lambda m: (
            m.reply_to_message
            and "текстом для рассылки всем пользователям"
            in (
                (m.reply_to_message.text or "")
                + (m.reply_to_message.caption or "")
            )
        ),
        content_types=types.ContentTypes.TEXT,
    )
    dp.register_message_handler(catch_reply_cast_user,     content_types=types.ContentTypes.TEXT)

    # команда точечной рассылки
    dp.register_message_handler(cast_cmd, commands=["cast"])

    # бэкап
    dp.register_callback_query_handler(cb_backup, lambda c: c.data == "admin_backup")
