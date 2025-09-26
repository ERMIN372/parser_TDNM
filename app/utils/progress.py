from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Sequence

from aiogram import Bot
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified

from app.utils.logging import get_operation_context, log_event


@dataclass(frozen=True)
class StageDefinition:
    key: str
    weight: float
    label: str
    fallback_active: int | None = None
    fallback_done: int | None = None


@dataclass
class _StageState:
    definition: StageDefinition
    progress: float = 0.0
    started_at: float | None = None
    completed_at: float | None = None
    done_reported: bool = False

    def mark_start(self) -> None:
        if self.started_at is None:
            self.started_at = time.perf_counter()

    def mark_done(self) -> None:
        if self.completed_at is None:
            self.completed_at = time.perf_counter()

    @property
    def is_done(self) -> bool:
        return self.progress >= 1.0


class ProgressMessage:
    """Wrapper around a single editable Telegram message used for progress."""

    def __init__(self, bot: Bot, chat_id: int, message_id: int) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._message_id = message_id

    @classmethod
    async def create(cls, bot: Bot, chat_id: int, text: str) -> "ProgressMessage":
        message = await bot.send_message(chat_id, text)
        return cls(bot, chat_id, message.message_id)

    async def edit(self, text: str) -> None:
        try:
            await self._bot.edit_message_text(text, self._chat_id, self._message_id)
        except (MessageNotModified, MessageCantBeEdited):
            pass
        except Exception:  # pragma: no cover - network errors are ignored
            pass

    async def delete_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            await self._bot.delete_message(self._chat_id, self._message_id)
        except Exception:  # pragma: no cover - best effort
            pass


REPORT_STAGES: Sequence[StageDefinition] = (
    StageDefinition("preflight", 5.0, "инициализация", fallback_done=10),
    StageDefinition(
        "fetch",
        60.0,
        "парсинг страниц",
        fallback_active=30,
        fallback_done=60,
    ),
    StageDefinition("normalize", 20.0, "нормализация", fallback_done=90),
    StageDefinition("write_xlsx", 15.0, "сбор XLSX", fallback_active=90, fallback_done=99),
)

PREVIEW_STAGES: Sequence[StageDefinition] = REPORT_STAGES


class ProgressReporter:
    """Aggregates stage-based progress and renders a single Telegram message."""

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        *,
        title: str,
        stages: Sequence[StageDefinition],
        update_interval: float = 2.0,
        min_delta: float = 1.0,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._title = title
        self._stage_states = [_StageState(stage) for stage in stages]
        self._stage_map: Dict[str, _StageState] = {state.definition.key: state for state in self._stage_states}
        self._update_interval = update_interval
        self._min_delta = min_delta
        self._message: ProgressMessage | None = None
        self._last_sent_percent: float = -1.0
        self._last_sent_ts: float = 0.0
        self._current_percent: float = 0.0
        self._pages_total: int | None = None
        self._pages_done: int | None = None
        self._pages_known = False
        self._site: str | None = None
        self._lock = asyncio.Lock()
        self._finalized = False
        self._started_at = time.perf_counter()
        self._correlation_id: str | None = None

    async def handle_event(self, kind: str, payload: dict) -> None:
        if kind == "start":
            await self._handle_start(payload)
        elif kind == "update":
            await self._handle_update(payload)
        elif kind == "close":
            await self.close(payload.get("ok", True), message=payload.get("message"), delete_after=payload.get("delete_after"))

    async def close(
        self,
        ok: bool,
        *,
        message: str | None = None,
        delete_after: float | None = None,
    ) -> None:
        async with self._lock:
            if self._finalized:
                return
            self._finalized = True
            duration_ms = int((time.perf_counter() - self._started_at) * 1000)
            if ok:
                self._current_percent = 100.0
            log_event(
                "progress_close",
                ok=ok,
                percent=int(round(self._current_percent)),
                duration_ms=duration_ms,
            )
            if self._message is None:
                return
            text = message or ("✅ Готово" if ok else "❌ Что-то пошло не так")
            await self._message.edit(text)
            if delete_after:
                asyncio.create_task(self._message.delete_after(delete_after))

    async def _handle_start(self, payload: dict) -> None:
        async with self._lock:
            if self._message is not None:
                return
            ctx = get_operation_context()
            self._correlation_id = payload.get("correlation_id") or (ctx.correlation_id if ctx else None)
            self._site = payload.get("site")
            self._started_at = time.perf_counter()
            pages_total = payload.get("pages_total")
            if isinstance(pages_total, int) and pages_total > 0:
                self._pages_total = pages_total
                self._pages_known = True
            fetch_state = self._stage_map.get("fetch")
            if fetch_state and self._site:
                fetch_state.definition = StageDefinition(
                    fetch_state.definition.key,
                    fetch_state.definition.weight,
                    f"{fetch_state.definition.label} {self._site}",
                    fallback_active=fetch_state.definition.fallback_active,
                    fallback_done=fetch_state.definition.fallback_done,
                )
            self._message = await ProgressMessage.create(self._bot, self._chat_id, self._render_text(0.0))
            self._last_sent_percent = 0.0
            self._last_sent_ts = time.perf_counter() - self._update_interval
            log_event(
                "progress_start",
                stage="preflight",
                percent=0,
                site=self._site,
                pages_total=self._pages_total,
            )

    async def _handle_update(self, payload: dict) -> None:
        stage = payload.get("stage")
        if not stage or stage not in self._stage_map:
            return
        state = self._stage_map[stage]
        subprogress = float(payload.get("subprogress", 0.0))
        subprogress = max(0.0, min(1.0, subprogress))

        pages_total = payload.get("pages_total")
        if isinstance(pages_total, int) and pages_total > 0:
            self._pages_total = pages_total
            self._pages_known = True
        pages_done = payload.get("pages_done")
        if isinstance(pages_done, int) and pages_done >= 0:
            self._pages_done = pages_done

        async with self._lock:
            if state.progress == 0.0 and subprogress > 0.0:
                state.mark_start()
                log_event("progress_stage_start", stage=stage)
            if subprogress >= state.progress:
                state.progress = subprogress
            if state.progress >= 1.0:
                if not state.is_done:
                    state.progress = 1.0
                state.mark_done()
            percent = self._compute_percent(stage)
            now = time.perf_counter()
            should_render = False
            if self._message is None:
                return
            if percent - self._last_sent_percent >= self._min_delta:
                if now - self._last_sent_ts >= self._update_interval:
                    should_render = True
            if should_render:
                self._current_percent = percent
                self._last_sent_percent = percent
                self._last_sent_ts = now
                await self._message.edit(self._render_text(percent))
            log_event(
                "progress_update",
                stage=stage,
                stage_progress=round(state.progress, 4),
                percent=int(round(percent)),
                pages_done=self._pages_done,
                pages_total=self._pages_total,
            )
            if state.completed_at and state.is_done and not state.done_reported:
                duration = None
                if state.started_at:
                    duration = int((state.completed_at - state.started_at) * 1000)
                log_event(
                    "progress_stage_done",
                    stage=stage,
                    duration_ms=duration,
                    pages_done=self._pages_done,
                    pages_total=self._pages_total,
                )
                state.done_reported = True

    def _compute_percent(self, stage: str) -> float:
        if not self._pages_known:
            return self._compute_fallback_percent(stage)
        percent = 0.0
        for state in self._stage_states:
            percent += state.definition.weight * max(0.0, min(1.0, state.progress))
        if not self._finalized:
            percent = min(percent, 99.0)
        return max(self._current_percent, round(percent, 2))

    def _compute_fallback_percent(self, stage: str) -> float:
        percent = self._current_percent
        state = self._stage_map[stage]
        fallback_active = state.definition.fallback_active
        fallback_done = state.definition.fallback_done
        if state.progress >= 1.0 and fallback_done is not None:
            percent = max(percent, float(fallback_done))
        elif state.progress > 0.0 and fallback_active is not None:
            percent = max(percent, float(fallback_active))
        if not self._finalized:
            percent = min(percent, 99.0)
        return percent

    def _render_text(self, percent: float) -> str:
        active_stage = self._resolve_active_stage()
        header = f"⏳ {self._title}… {int(round(percent))}%"
        lines = [header]
        for state in self._stage_states:
            prefix = "—"
            if state.is_done:
                prefix = "✓"
            elif state.definition.key == active_stage:
                prefix = "→"
            label = state.definition.label
            if state.definition.key == "fetch" and self._pages_total:
                done = self._pages_done or 0
                label = f"{label}… {done}/{self._pages_total}"
            else:
                label = f"{label}…"
            lines.append(f"{prefix} {label}")
        return "\n".join(lines)

    def _resolve_active_stage(self) -> str | None:
        for state in self._stage_states:
            if state.progress < 1.0:
                return state.definition.key
        return None

    def snapshot(self) -> dict:
        return {
            "percent": round(self._current_percent, 2),
            "site": self._site,
            "pages_done": self._pages_done,
            "pages_total": self._pages_total,
            "stages": {
                state.definition.key: {
                    "progress": round(state.progress, 4),
                    "started_at": state.started_at,
                    "completed_at": state.completed_at,
                }
                for state in self._stage_states
            },
        }


def create_report_progress(bot: Bot, chat_id: int) -> ProgressReporter:
    return ProgressReporter(bot, chat_id, title="Готовлю данные", stages=REPORT_STAGES)


def create_preview_progress(bot: Bot, chat_id: int) -> ProgressReporter:
    return ProgressReporter(bot, chat_id, title="Готовлю превью", stages=PREVIEW_STAGES)
