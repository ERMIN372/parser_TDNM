from __future__ import annotations

import asyncio
from typing import Sequence

from aiogram import Bot
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified


class ProgressMessage:
    """Utility to manage a single editable progress message with spinner."""

    _SPINNER: Sequence[str] = ("⏳", "⌛", "🔄")

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        message_id: int,
        template: str,
        *,
        interval: float = 3.0,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._message_id = message_id
        self._template = template
        self._interval = interval
        self._spinner_index = 0
        self._active = True
        self._lock = asyncio.Lock()
        self._spin_task: asyncio.Task | None = None

    @classmethod
    async def create(
        cls,
        bot: Bot,
        chat_id: int,
        template: str,
        *,
        interval: float = 3.0,
    ) -> "ProgressMessage":
        text = template.format(spinner=cls._SPINNER[0])
        message = await bot.send_message(chat_id, text)
        inst = cls(bot, chat_id, message.message_id, template, interval=interval)
        inst._spin_task = asyncio.create_task(inst._spin())
        return inst

    async def update_template(self, template: str) -> None:
        self._template = template
        self._spinner_index = 0
        await self._render_current()

    async def _render_current(self) -> None:
        if not self._active:
            return
        async with self._lock:
            text = self._template.format(spinner=self._SPINNER[self._spinner_index])
            try:
                await self._bot.edit_message_text(text, self._chat_id, self._message_id)
            except (MessageNotModified, MessageCantBeEdited):
                pass
            except Exception:  # pragma: no cover
                pass

    async def _spin(self) -> None:
        try:
            while self._active:
                await asyncio.sleep(self._interval)
                if not self._active:
                    break
                self._spinner_index = (self._spinner_index + 1) % len(self._SPINNER)
                await self._render_current()
        except asyncio.CancelledError:  # pragma: no cover
            raise

    async def finish(self, text: str, *, delete_after: float | None = None) -> None:
        await self._stop()
        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            pass
        except Exception:  # pragma: no cover
            pass
        if delete_after:
            asyncio.create_task(self._delete_later(delete_after))

    async def fail(self, text: str) -> None:
        await self._stop()
        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            pass
        except Exception:  # pragma: no cover
            pass

    async def _stop(self) -> None:
        if not self._active:
            return
        self._active = False
        if self._spin_task:
            self._spin_task.cancel()
            try:
                await self._spin_task
            except asyncio.CancelledError:
                pass

    async def _delete_later(self, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            await self._bot.delete_message(self._chat_id, self._message_id)
        except Exception:  # pragma: no cover
            pass
