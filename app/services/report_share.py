from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from app.config import settings
from app.services import referrals


@dataclass
class ReportSnapshot:
    role: str
    city: str
    include: tuple[str, ...]
    exclude: tuple[str, ...]
    volume: int | None
    path: Path
    generated_at: datetime
    median: str | None
    low: str | None
    high: str | None
    top_companies: tuple[str, ...]


_LAST_REPORTS: dict[int, ReportSnapshot] = {}


def _normalize_list(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return tuple()
    cleaned = []
    for value in values:
        text = str(value).strip()
        if text:
            cleaned.append(text)
    return tuple(cleaned)


def save_last_report(
    user_id: int,
    *,
    role: str,
    city: str,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    volume: int | None,
    path: Path,
    median: str | None,
    low: str | None,
    high: str | None,
    top_companies: Sequence[str] | None,
) -> None:
    snapshot = ReportSnapshot(
        role=(role or "").strip(),
        city=(city or "").strip(),
        include=_normalize_list(include),
        exclude=_normalize_list(exclude),
        volume=volume,
        path=Path(path).resolve(),
        generated_at=datetime.utcnow(),
        median=(median or None),
        low=(low or None),
        high=(high or None),
        top_companies=_normalize_list(top_companies),
    )
    _LAST_REPORTS[user_id] = snapshot


def get_last_report(user_id: int) -> ReportSnapshot | None:
    return _LAST_REPORTS.get(user_id)


def build_share_link(bot_username: str, user_id: int) -> str:
    username = bot_username.lstrip("@")
    if settings.REF_ENABLED:
        base_link = referrals.build_referral_link(bot_username, user_id)
        parsed = urlsplit(base_link)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        token = None
        for name, value in query:
            if name == "start":
                token = value
                break
        if token is None:
            token = f"ref_{user_id}"
            query.append(("start", token))
        query.append(("utm_source", "share"))
        ref_code = token.replace("ref_", "", 1) if token else ""
        if ref_code:
            query.append(("ref_code", ref_code))
        new_query = urlencode(query, doseq=True)
        return urlunsplit(parsed._replace(query=new_query))
    return f"https://t.me/{username}?start=ref_{user_id}"


def _format_value(value: str | None) -> str:
    value = (value or "").strip()
    return value or "нет данных"


def build_share_text(report: ReportSnapshot, link: str) -> str:
    role = report.role or "—"
    city = report.city or "—"
    median = _format_value(report.median)
    low = _format_value(report.low)
    high = _format_value(report.high)
    top = ", ".join(report.top_companies[:3]) if report.top_companies else "—"
    if not top or top == "—":
        top = "нет данных"
    lines = [
        f"Отчёт по {role} • {city}",
        f"Медиана {median} (низ {low}, верх {high})",
        f"Топ работодателей: {top}. Скачать: {link}",
    ]
    return "\n".join(lines)
