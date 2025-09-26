from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def main_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("🔎 Поиск"), KeyboardButton("🧭 Статус"))
    kb.row(KeyboardButton("💳 Купить"), KeyboardButton("ℹ️ Помощь"))
    kb.row(KeyboardButton("🎁 Рефералы"), KeyboardButton("🏆 Начисления"))
    if is_admin:
        kb.add(KeyboardButton("🛠 Админ"))
    return kb


def promo_result_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("🔎 Поиск"), KeyboardButton("Купить ещё кредиты"))
    return kb
