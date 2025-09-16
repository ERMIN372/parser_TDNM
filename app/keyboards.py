from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def main_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("🔎 Поиск"), KeyboardButton("🧭 Статус"))
    kb.row(KeyboardButton("💳 Купить"), KeyboardButton("ℹ️ Помощь"))
    if is_admin:
        kb.add(KeyboardButton("🛠 Админ"))
    return kb
