from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Iterable, Sequence

from aiogram import Bot
from aiogram.utils.exceptions import (
    BadRequest,
    MessageCantBeEdited,
    MessageNotModified,
    RetryAfter,
)

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
        self._ui_strategy: str = "edit"

    @property
    def last_step(self) -> str | None:
        return self._last_step

    @property
    def ui_strategy(self) -> str:
        return self._ui_strategy

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
            "progress.start",
            mode=mode,
            progress_step=initial_step,
            percent=initial_percent,
            ui_action="send",
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
        await self._deliver(text, step_name, percent, extra_text=extra_text)

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
        action = await self._deliver(
            final_text,
            self._last_step or "complete",
            self._last_percent,
            allow_skip_logging=False,
        )
        log_event(
            "progress.close",
            mode=self._mode,
            progress_step=self._last_step,
            percent=self._last_percent,
            ok=ok,
            ui_action=action,
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
        await self._deliver(text, self._last_step or "unknown", self._last_percent)

    def _compose_current(self) -> str:
        return self._compose_text(
            status=self._status_text,
            percent=self._last_percent,
            steps=self._steps,
            current_step=self._last_step,
            extra=self._extra_text,
            retry_text=self._retry_text,
        )

    async def _deliver(
        self,
        text: str,
        step_name: str,
        percent: int,
        *,
        extra_text: str | None = None,
        allow_skip_logging: bool = True,
    ) -> str:
        """Try to update progress message, falling back to sending a new one."""

        payload = {
            "mode": self._mode,
            "progress_step": step_name,
            "percent": percent,
            "extra": extra_text,
        }

        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except MessageNotModified:
            if allow_skip_logging:
                log_event("progress.update", ui_action="skip", **payload)
            return "skip"
        except RetryAfter as exc:  # pragma: no cover - network timing dependant
            await asyncio.sleep(getattr(exc, "timeout", 1))
            return await self._deliver(
                text,
                step_name,
                percent,
                extra_text=extra_text,
                allow_skip_logging=allow_skip_logging,
            )
        except (MessageCantBeEdited, BadRequest) as exc:
            if isinstance(exc, BadRequest) and _is_message_not_modified_error(exc):
                if allow_skip_logging:
                    log_event("progress.update", ui_action="skip", **payload)
                return "skip"
            if isinstance(exc, BadRequest) and not _should_fallback_on_bad_request(exc):
                if allow_skip_logging:
                    log_event(
                        "progress.update",
                        ui_action="skip",
                        err=str(exc),
                        **payload,
                    )
                return "skip"
            message = await self._bot.send_message(self._chat_id, text)
            self._message_id = message.message_id
            self._ui_strategy = "send"
            log_event("progress.update", ui_action="send", **payload)
            return "send"
        except Exception as exc:  # pragma: no cover - best effort
            try:
                message = await self._bot.send_message(self._chat_id, text)
            except Exception:  # pragma: no cover - best effort
                if allow_skip_logging:
                    log_event(
                        "progress.update",
                        ui_action="skip",
                        err=str(exc),
                        **payload,
                    )
                return "skip"
            else:
                self._message_id = message.message_id
                self._ui_strategy = "send"
                log_event("progress.update", ui_action="send", **payload)
                return "send"
        else:
            log_event("progress.update", ui_action="edit", **payload)
            return "edit"


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


def _is_message_not_modified_error(exc: BadRequest) -> bool:
    message = str(exc).lower()
    return "message is not modified" in message


def _should_fallback_on_bad_request(exc: BadRequest) -> bool:
    message = str(exc).lower()
    return "message can't be edited" in message or "message to edit not found" in message
