from __future__ import annotations
import os
from aiogram import types, Dispatcher
from aiogram.types import InputFile  # <- для баннера
from app import keyboards
from aiogram.dispatcher import FSMContext
from app.utils.admins import is_admin


# Админы и лимиты
FREE_PER_MONTH = int(os.getenv("FREE_PER_MONTH", "3"))

# Путь к баннеру (можно переопределить в .env: START_BANNER_PATH=assets/start_banner_1x1.png)
BANNER_PATH = os.getenv("START_BANNER_PATH", "assets/start_banner_1x1.png")


# ---------- /start ----------
async def cmd_start(message: types.Message, state: FSMContext):
    try:
        await state.finish()
    except Exception:
        # на случай, если состояния нет или не инициализировано
        pass

    kb = keyboards.main_kb(is_admin=is_admin(message.from_user.id))

    # 1) Пробуем отправить баннер (если файл есть) с коротким капшеном
    try:
        if os.path.exists(BANNER_PATH):
            caption = (
                "HR-Assist — собираю вакансии и присылаю Excel-отчёт.\n"
                f"Нажми «🔎 Поиск». Бесплатно — {FREE_PER_MONTH} запроса в месяц."
            )
            await message.answer_photo(InputFile(BANNER_PATH), caption=caption, reply_markup=kb)
            return
    except Exception:
        # не ломаемся, просто идём на текст
        pass

    # 2) Фолбэк-текст (если баннера нет/не отправился)
    text = (
        "Привет! Я <b>HR-Assist</b> — соберу вакансии по твоему запросу и пришлю файл Excel.\n\n"
        "Как пользоваться:\n"
        "1) Нажми «🔎 Поиск» — я спрошу должность и город.\n"
        "2) Или одной командой: <code>/parse бариста; Москва</code>\n"
        "3) Получишь .xlsx с вакансиями.\n\n"
        f"🎁 Бесплатно — {FREE_PER_MONTH} запроса в месяц.\n"
        "Нужно больше? Жми «💳 Купить».\n\n"
        "Подробная помощь — <code>/help</code> (очень коротко и по делу).\n"
        "Продвинутые настройки — <code>/advanced</code> (необязательно)."
    )
    await message.reply(text, reply_markup=kb, disable_web_page_preview=True)


# ---------- /help ----------
async def cmd_help(message: types.Message):
    text = (
        "<b>Памятка</b>\n\n"
        "<b>Как искать</b>\n"
        "• Самый простой способ — кнопка «🔎 Поиск» (спрошу должность и город).\n"
        "• Быстрая команда: <code>/parse кассир; Москва</code>\n\n"
        "<b>Что пришлёт бот</b>\n"
        "• Файл <code>.xlsx</code> с вакансиями: компания, зарплата, ссылка и т.п.\n\n"
        "<b>Если долго не приходит</b>\n"
        "• Попробуй ещё раз позже — сайты иногда тормозят.\n"
        "• Для больших запросов начни с простого (например, только один город).\n\n"
        "<b>Лимиты и оплата</b>\n"
        f"• Бесплатно: {FREE_PER_MONTH} запроса в месяц.\n"
        "• Нужно больше — нажми «💳 Купить» (пакеты 1/3/9 запросов или безлимит на 30 дней).\n\n"
        "<b>Команды</b>\n"
        "• <code>/parse</code> — поиск\n"
        "• <code>/status</code> — остаток бесплатных и баланс\n"
        "• <code>/buy</code> — покупка пакетов\n"
        "• <code>/help</code> — эта справка\n"
        "• <code>/advanced</code> — расширенные настройки (для редких случаев)"
    )
    await message.reply(text, disable_web_page_preview=True)


# ---------- /advanced (необязательно) ----------
async def cmd_advanced(message: types.Message):
    text = (
        "<b>Расширенные настройки</b> (нужны редко):\n\n"
        "Эти параметры можно дописать после города через точку с запятой.\n"
        "Пример: <code>/parse бариста; Москва; pages=1; site=hh</code>\n\n"
        "• <code>pages=1</code> — сколько страниц искать. Больше страниц — дольше ждать. По умолчанию 1.\n"
        "• <code>per_page=20</code> — сколько вакансий на странице. Обычно не трогаем.\n"
        "• <code>site=hh</code> | <code>gorodrabot</code> | <code>both</code> — откуда собирать.\n"
        "• <code>area=1</code> — код региона HH (если знаешь его). Обычно достаточно назвать город.\n"
        "• <code>pause=0.6</code> — пауза между запросами (оставь как есть).\n\n"
        "Если сомневаешься — лучше вообще не трогать эти настройки 🙂"
    )
    await message.reply(text, disable_web_page_preview=True)


# -------- показать меню по слову «Меню» --------
async def show_menu(message: types.Message):
    kb = keyboards.main_kb(is_admin=is_admin(message.from_user.id))
    await message.reply("Меню 👇", reply_markup=kb)

# опционально /cancel — сбросить текущий диалог
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.finish()
    kb = keyboards.main_kb(is_admin=is_admin(message.from_user.id))
    await message.reply("Окей, сбросил диалог. Нажми кнопки ниже или /start.", reply_markup=kb)

def register(dp: Dispatcher):
    # команды должны работать из любого состояния
    dp.register_message_handler(cmd_start,    commands=["start"],    state="*")
    dp.register_message_handler(cmd_help,     commands=["help"],     state="*")
    dp.register_message_handler(cmd_advanced, commands=["advanced"], state="*")
    dp.register_message_handler(cmd_cancel,   commands=["cancel"],   state="*")

    # показать меню по слову (тоже в любом состоянии)
    dp.register_message_handler(
        show_menu,
        lambda m: (m.text or "").lower() in {"меню", "menu", "🏠 меню"},
        state="*",
    )

    # реакции на кнопки меню (помощь) — из любого состояния
    dp.register_message_handler(
        cmd_help,
        lambda m: (m.text or "").strip() in {"ℹ️ Помощь", "Помощь"},
        state="*",
    )

