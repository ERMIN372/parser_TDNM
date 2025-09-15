from aiogram import types, Dispatcher
import aiogram
import aiohttp

from ..config import settings


async def cmd_status(message: types.Message):
    text = (
        f"MODE: {settings.MODE}\n"
        f"aiogram: {aiogram.__version__}\n"
        f"aiohttp: {aiohttp.__version__}\n"
        f"REPORT_DIR: {settings.REPORT_DIR.resolve()}"
    )
    await message.answer(text)


def register(dp: Dispatcher):
    dp.register_message_handler(cmd_status, commands=["status"])
