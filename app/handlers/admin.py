from __future__ import annotations
import os
import asyncio
from datetime import datetime
from typing import List, Tuple

from aiogram import types, Dispatcher
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, MessageNotModified
from pathlib import Path

from app.storage import repo
from app.storage.models import User
from app.utils.backup import make_sqlite_backup

# --- доступ ---
ADMINS = {int(x) for x in os.getenv("ADMIN_USER_IDS", "").replace(" ", "").split(",") if x.isdigit()}
def _guard(uid: int) -> bool: return uid in ADMINS

# --- пагинация ---
PAGE_SIZE = 10

def _kb_admin_home() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👥 Пользователи", callback_data="admin_users:1"),
        InlineKeyboardButton("📣 Рассылка", callback_data="admin_cast"),
    )
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
    await call.answer()

# -------- список пользователей --------
async def cb_users(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, payload = call.data.split(":")
    page = int(payload)
    text, kb = _users_page(page)
    await _safe_edit_text(call.message, text, reply_markup=kb)
    await call.answer()

# -------- карточка пользователя --------
async def cb_user(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid = call.data.split(":")
    u = repo.get_user(int(uid))
    if not u:
        await call.answer("Пользователь не найден", show_alert=True); return
    await _safe_edit_text(call.message, _user_card_text(u), reply_markup=_kb_user(u), parse_mode="HTML")
    await call.answer()

# -------- действия: безлимит/кредиты --------
async def cb_unlim(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid, days = call.data.split(":")
    uid, days = int(uid), int(days)
    until = repo.set_unlimited(uid, days)
    await call.answer("Выдан безлимит", show_alert=False)
    u = repo.get_user(uid)
    await _safe_edit_text(call.message, _user_card_text(u), reply_markup=_kb_user(u), parse_mode="HTML")

async def cb_credit(call: types.CallbackQuery):
    if not _guard(call.from_user.id): return
    _, uid, n = call.data.split(":")
    uid, n = int(uid), int(n)
    bal = repo.add_credits(uid, n)
    await call.answer(f"+{n} кредит(ов). Баланс: {bal}", show_alert=False)
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
    await call.answer()

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
    await call.answer()

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
    await call.answer("Пришли текст рассылки ответом на это сообщение.")
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
    await call.answer()

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
    await call.answer("Готово")

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

    # рассылки
    dp.register_callback_query_handler(cb_cast_menu,  lambda c: c.data == "admin_cast")
    dp.register_callback_query_handler(cb_cast_all,   lambda c: c.data == "admin_cast_all")
    dp.register_callback_query_handler(cb_cast_prompt,lambda c: c.data == "admin_cast_prompt")
    dp.register_callback_query_handler(cb_cast_user,  lambda c: c.data and c.data.startswith("admin_cast_user:"))
    dp.register_message_handler(catch_reply_broadcast_all, content_types=types.ContentTypes.TEXT)
    dp.register_message_handler(catch_reply_cast_user,     content_types=types.ContentTypes.TEXT)

    # команда точечной рассылки
    dp.register_message_handler(cast_cmd, commands=["cast"])

    # бэкап
    dp.register_callback_query_handler(cb_backup, lambda c: c.data == "admin_backup")
