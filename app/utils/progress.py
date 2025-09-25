from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

from aiogram import Bot
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified

from app.utils.logging import log_event


@dataclass(frozen=True)
class ProgressStep:
    name: str
    title: str


class Progress:
    """Editable progress message with throttled updates and structured steps."""

    MIN_INTERVAL = 1.0

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        message_id: int,
        steps: Sequence[ProgressStep],
        *,
        mode: str,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._message_id = message_id
        self._steps = list(steps)
        self._mode = mode
        self._status_text = "⏳ Готовлю данные…"
        self._extra_text: str | None = None
        self._retry_text: str | None = None
        self._last_step: str | None = None
        self._last_percent: int = 0
        self._last_update_ts: float = 0.0
        self._closed = False

    @property
    def last_step(self) -> str | None:
        return self._last_step

    @classmethod
    async def create(
        cls,
        bot: Bot,
        chat_id: int,
        steps: Iterable[tuple[str, str]] | Iterable[ProgressStep],
        *,
        mode: str,
        initial_step: str,
        initial_percent: int = 0,
    ) -> "Progress":
        normalized_steps = [
            step
            if isinstance(step, ProgressStep)
            else ProgressStep(name=step[0], title=step[1])
            for step in steps
        ]
        text = cls._compose_text(
            status="⏳ Готовлю данные…",
            percent=initial_percent,
            steps=normalized_steps,
            current_step=initial_step,
        )
        message = await bot.send_message(chat_id, text)
        inst = cls(
            bot,
            chat_id,
            message.message_id,
            normalized_steps,
            mode=mode,
        )
        inst._last_step = initial_step
        inst._last_percent = initial_percent
        inst._last_update_ts = time.monotonic()
        log_event(
            "progress.create",
            mode=mode,
            progress_step=initial_step,
            percent=initial_percent,
        )
        return inst

    async def set(
        self,
        step_name: str,
        percent: int,
        *,
        extra_text: str | None = None,
        status_text: str | None = None,
        force: bool = False,
    ) -> None:
        if self._closed:
            return
        if status_text:
            self._status_text = status_text
        self._extra_text = extra_text
        self._last_step = step_name
        self._last_percent = percent
        now = time.monotonic()
        if not force and now - self._last_update_ts < self.MIN_INTERVAL:
            return
        self._last_update_ts = now
        text = self._compose_current()
        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            return
        except Exception:  # pragma: no cover - best effort
            return
        log_event(
            "progress.update",
            mode=self._mode,
            progress_step=step_name,
            percent=percent,
            extra=extra_text,
        )

    async def show_retry(self, attempt: int, total: int) -> None:
        if self._closed:
            return
        self._retry_text = f"♻️ Перепробуем ({attempt}/{total})…"
        self._status_text = f"⏳ Повтор {attempt}/{total}…"
        await self._render(force=True)
        log_event(
            "progress.retry",
            mode=self._mode,
            attempt=attempt,
            total=total,
            progress_step=self._last_step,
        )

    async def clear_retry(self) -> None:
        if self._closed:
            return
        self._retry_text = None
        self._status_text = "⏳ Готовлю данные…"
        await self._render(force=True)

    async def close(self, *, ok: bool, text: str, delete_after: float | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        if ok:
            self._last_step = self._last_step or "complete"
            self._last_percent = 100
        final_text = text
        try:
            await self._bot.edit_message_text(final_text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            pass
        except Exception:  # pragma: no cover - best effort
            pass
        else:
            log_event(
                "progress.close",
                mode=self._mode,
                progress_step=self._last_step,
                percent=self._last_percent,
                ok=ok,
            )
        if ok and delete_after:
            asyncio.create_task(self._delete_later(delete_after))

    async def _render(self, *, force: bool = False) -> None:
        if self._closed:
            return
        now = time.monotonic()
        if not force and now - self._last_update_ts < self.MIN_INTERVAL:
            return
        self._last_update_ts = now
        text = self._compose_current()
        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            return
        except Exception:  # pragma: no cover - best effort
            return

    def _compose_current(self) -> str:
        return self._compose_text(
            status=self._status_text,
            percent=self._last_percent,
            steps=self._steps,
            current_step=self._last_step,
            extra=self._extra_text,
            retry_text=self._retry_text,
        )

    @staticmethod
    def _compose_text(
        *,
        status: str,
        percent: int,
        steps: Sequence[ProgressStep],
        current_step: str | None,
        extra: str | None = None,
        retry_text: str | None = None,
    ) -> str:
        lines = [f"{status} {max(0, min(100, percent))}%"]
        for step in steps:
            prefix = "→" if step.name == current_step else "—"
            lines.append(f"{prefix} {step.title}")
        if retry_text:
            lines.append(retry_text)
        if extra:
            lines.append(extra)
        return "\n".join(lines)

    async def _delete_later(self, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            await self._bot.delete_message(self._chat_id, self._message_id)
        except Exception:  # pragma: no cover - best effort
            pass
