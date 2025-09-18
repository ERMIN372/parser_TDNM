from __future__ import annotations

import html
import logging
import math
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

try:  # pragma: no cover - optional dependency handling
    import numpy as np
    import pandas as pd
except Exception:  # pragma: no cover - gracefully handle missing pandas
    np = None  # type: ignore
    pd = None  # type: ignore

log = logging.getLogger(__name__)

_CONTEXT: dict[str, tuple[str, str]] = {}

_THIN_NBSP = "\u202f"

__all__ = ["register_context", "render_mini_analytics"]

_SPARKLINE_LEVELS = "▁▂▃▄▅▆▇█"
_SCHEDULE_MAP = (
    ("5/2", "5/2"),
    ("смен", "смены"),
    ("вахт", "вахта"),
    ("удал", "удалёнка"),
    ("remote", "удалёнка"),
    ("гибк", "гибкий"),
)
_SOURCE_ALIASES = {
    "hh": "hh.ru",
    "headhunter": "hh.ru",
    "hh.ru": "hh.ru",
    "gorodrabot": "gorodrabot.ru",
    "gorodrabot.ru": "gorodrabot.ru",
}

_COLUMN_SYNONYMS: dict[str, tuple[str, ...]] = {
    "title": (
        "title",
        "name",
        "vacancy_title",
        "должность",
        "позиция",
        "vacancy",
    ),
    "company": (
        "company",
        "employer",
        "company_name",
        "работодатель",
        "компания",
    ),
    "link": (
        "link",
        "url",
        "vacancy_url",
        "alternate_url",
        "ссылка",
    ),
    "city": (
        "city",
        "area",
        "location",
        "город",
        "регион",
        "локация",
    ),
    "source": (
        "source",
        "site",
        "источник",
    ),
    "salary_from": (
        "salary_from",
        "salary_min",
        "зарплата от",
        "зп от",
        "зп от (т.р.)",
        "зарплата от (т.р.)",
    ),
    "salary_to": (
        "salary_to",
        "salary_max",
        "зарплата до",
        "зп до",
        "зп до (т.р.)",
        "зарплата до (т.р.)",
    ),
    "salary_currency": (
        "salary_currency",
        "currency",
        "валюта",
    ),
    "schedule": (
        "schedule",
        "format",
        "employment_type",
        "график",
        "формат",
    ),
    "experience": (
        "experience",
        "exp",
        "experience_level",
        "требуемый опыт",
        "опыт",
    ),
    "published_at": (
        "published_at",
        "date",
        "created_at",
        "дата",
        "дата публикации",
    ),
}

_COLUMN_SYNONYMS_NORMALIZED: dict[str, set[str]] = {
    key: {"".join(ch for ch in alias.lower() if ch.isalnum()) for alias in aliases}
    for key, aliases in _COLUMN_SYNONYMS.items()
}


def register_context(path: Path, *, title: str | None, city: str | None) -> None:
    """Register request context for later rendering."""

    key = str(Path(path).resolve())
    safe_title = (title or "").strip()
    safe_city = (city or "").strip()
    _CONTEXT[key] = (safe_title, safe_city)


def _format_int(value: int | float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "0"
    return f"{int(round(value)):,}".replace(",", " ")


def _format_money(value: float | int | None) -> str:
    if value is None:
        return "нет данных"

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "нет данных"

    if math.isnan(numeric):
        return "нет данных"

    absolute = abs(numeric)
    rounded = int(1000 * math.floor((absolute / 1000) + 0.5))
    if numeric < 0:
        rounded = -rounded
    return f"{rounded:,}".replace(",", _THIN_NBSP)


def _format_percent(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "0%"
    return f"{int(round(value))}%"


def _normalize_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _load_dataframe(path: Path):
    if pd is None:  # pragma: no cover - pandas missing
        log.warning("mini_analytics: pandas is not available")
        return None

    raw_csv = path.parent / "raw.csv"
    try:
        if raw_csv.exists():
            return pd.read_csv(raw_csv, encoding="utf-8-sig")
    except Exception as exc:  # pragma: no cover - log and fallback to xlsx
        log.warning("mini_analytics: failed to read raw.csv: %s", exc)

    try:
        try:
            return pd.read_excel(path, sheet_name="vacancies")
        except ValueError:
            return pd.read_excel(path)
    except Exception as exc:  # pragma: no cover
        log.warning("mini_analytics: failed to read %s: %s", path, exc)
        return None


def _resolve_columns(df: "pd.DataFrame") -> tuple[dict[str, str], dict[str, str]]:
    normalized = {_normalize_name(str(col)): str(col) for col in df.columns}
    rename: dict[str, str] = {}
    raw: dict[str, str] = {}
    for target, aliases in _COLUMN_SYNONYMS_NORMALIZED.items():
        for norm, original in normalized.items():
            if norm in aliases:
                rename[original] = target
                raw[target] = original
                break
    return rename, raw


def _clean_text(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    text = str(val).replace("\xa0", " ").strip()
    return text


_NUMBER_RE = re.compile(r"[^0-9,\.\-]")


def _to_number(val) -> float:
    if val is None:
        return float("nan")
    if isinstance(val, (int, float)):
        return float(val)
    text = str(val).strip()
    if not text:
        return float("nan")
    text = text.replace("\xa0", " ")
    text = text.replace(" ", "")
    text = text.replace(",", ".")
    text = _NUMBER_RE.sub("", text)
    if not text or text in {"-", ".", "-.", "--"}:
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def _normalize_currency(val: str | float | int | None) -> str:
    if val is None:
        return "RUB"
    if isinstance(val, (int, float)):
        return "RUB"
    text = str(val).strip()
    if not text:
        return "RUB"
    text = text.upper().replace("РУБ", "RUB")
    if text == "RUR":
        text = "RUB"
    return text


def _detect_scale(column_name: str | None) -> float:
    if not column_name:
        return 1.0
    name = column_name.lower()
    if "т.р" in name or "тыр" in name or "тыс" in name:
        return 1000.0
    return 1.0


def _schedule_label(value: str) -> str:
    text = value.lower()
    for needle, label in _SCHEDULE_MAP:
        if needle in text:
            return label
    return value


def _map_experience(value: str) -> str:
    text = value.lower()
    if not text:
        return ""
    if any(word in text for word in ("без опыта", "no experience", "не требуется", "без стажа", "noexp")):
        return "без опыта"
    if "до 1" in text or "0-1" in text or "0–1" in text or "до года" in text or "менее года" in text:
        return "без опыта"

    numbers = sorted({int(n) for n in re.findall(r"\d+", text)})
    if numbers:
        if numbers[-1] >= 6 or "6" in text and ("более" in text or "+" in text or ">" in text):
            return "6+"
        if numbers[-1] >= 6:
            return "6+"
        if numbers[-1] >= 3:
            return "3–6"
        return "1–3"

    if "middle" in text or "senior" in text and "junior" not in text:
        return "3–6"
    if "junior" in text or "intern" in text or "стаж" in text:
        return "без опыта"
    return "1–3"


def _sparkline(counts: Sequence[int]) -> str:
    if not counts:
        return ""
    max_count = max(counts)
    if max_count <= 0:
        return _SPARKLINE_LEVELS[0] * len(counts)
    levels = len(_SPARKLINE_LEVELS) - 1
    parts = []
    for count in counts:
        if count <= 0:
            idx = 0
        else:
            ratio = count / max_count
            idx = min(levels, max(0, int(round(ratio * levels))))
        parts.append(_SPARKLINE_LEVELS[idx])
    return "".join(parts)


def _filters_line(include: Iterable[str] | None, exclude: Iterable[str] | None) -> str:
    parts: list[str] = []
    inc_list = [p.strip() for p in (include or []) if str(p).strip()]
    exc_list = [p.strip() for p in (exclude or []) if str(p).strip()]
    if inc_list:
        parts.append(f"include — {', '.join(html.escape(p) for p in inc_list)}")
    if exc_list:
        parts.append(f"exclude — {', '.join(html.escape(p) for p in exc_list)}")
    if not parts:
        return ""
    return "Фильтр: " + "; ".join(parts)


def _format_share_line(counter: Counter[str], total: int, limit: int) -> str:
    if total <= 0:
        return ""
    items = counter.most_common(limit)
    parts = []
    for label, count in items:
        percent = (count / total) * 100 if total else 0
        parts.append(f"{html.escape(label)} — {_format_percent(percent)}")
    return " • ".join(parts)


def render_mini_analytics(
    path: Path,
    *,
    approx_total: int | float | str | None = None,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
) -> str | None:
    """Render compact analytics message for a generated report."""

    if pd is None or np is None:  # pragma: no cover - pandas missing
        return None

    try:
        df = _load_dataframe(path)
        if df is None or df.empty:
            return None

        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        rename_map, raw_map = _resolve_columns(df)
        df = df.rename(columns=rename_map)

        if "title" not in df.columns or "link" not in df.columns:
            return None

        df["title"] = df["title"].apply(_clean_text)
        df["link"] = df["link"].apply(_clean_text)
        if "company" in df.columns:
            df["company"] = df["company"].apply(_clean_text)
        else:
            df["company"] = ""

        valid_mask = (df["title"].str.len() > 0) & (df["link"].str.len() > 0)
        df = df.loc[valid_mask].copy()
        if df.empty:
            return None

        key_series = (
            df["title"].str.lower()
            + "||"
            + df["company"].str.lower()
            + "||"
            + df["link"].str.lower()
        )
        df = df.loc[~key_series.duplicated()].copy()
        if df.empty:
            return None

        processed = int(df.shape[0])

        approx_val: int | None = None
        if approx_total is not None:
            try:
                approx_val = int(float(str(approx_total).replace(" ", "").replace(",", ".")))
            except Exception:
                approx_val = None

        key = str(Path(path).resolve())
        context = _CONTEXT.pop(key, None)
        if context is None:
            title_text, city_text = "—", "—"
        else:
            raw_title, raw_city = context
            title_text = raw_title or "—"
            city_text = raw_city or "—"

        header_lines = [
            "<b>📊 HR-Assist — мини-аналитика</b>",
            f"Запрос: «{html.escape(title_text)}» • {html.escape(city_text)}",
        ]
        processed_line = f"Обработано: {_format_int(processed)}"
        if approx_val is not None:
            processed_line += f" из ~{_format_int(approx_val)}"
        header_lines.append(processed_line)

        sections: list[str] = ["\n".join(header_lines)]

        # Salary block
        salary_lines: list[str] = []
        if "salary_from" in df.columns or "salary_to" in df.columns:
            scale_from = _detect_scale(raw_map.get("salary_from"))
            scale_to = _detect_scale(raw_map.get("salary_to"))

            salary_from = df.get("salary_from")
            salary_to = df.get("salary_to")

            if salary_from is not None:
                min_series = salary_from.apply(_to_number) * scale_from
            else:
                min_series = pd.Series(np.nan, index=df.index)
            if salary_to is not None:
                max_series = salary_to.apply(_to_number) * scale_to
            else:
                max_series = pd.Series(np.nan, index=df.index)

            if "salary_currency" in df.columns:
                currencies = df["salary_currency"].apply(_normalize_currency)
            else:
                currencies = pd.Series(["RUB"] * len(df), index=df.index)
            currencies = currencies.replace({"RUR": "RUB"})
            currency_mask = currencies.eq("RUB")

            available_mask = currency_mask & (~min_series.isna() | ~max_series.isna())
            if available_mask.any():
                mid_values = []
                for a, b in zip(min_series, max_series):
                    if pd.isna(a) and pd.isna(b):
                        mid_values.append(np.nan)
                    elif pd.isna(a):
                        mid_values.append(b)
                    elif pd.isna(b):
                        mid_values.append(a)
                    else:
                        mid_values.append((a + b) / 2)
                mid = pd.Series(mid_values, index=df.index)
                mid = mid.where(available_mask, np.nan)
                mid_valid = mid.dropna()
                if not mid_valid.empty:
                    median_val = float(np.nanmedian(mid_valid.values))
                    p10_val = float(np.nanpercentile(mid_valid.values, 10))
                    p90_val = float(np.nanpercentile(mid_valid.values, 90))
                    share_val: float | None = None
                    if processed:
                        share_val = (available_mask.sum() / processed) * 100

                    salary_lines.extend(
                        [
                            "<b>💰 Вилки (₽/мес, midpoint)</b>",
                            f"• медиана: {_format_money(median_val)}",
                            f"• низ рынка: {_format_money(p10_val)}",
                            f"• верх рынка: {_format_money(p90_val)}",
                        ]
                    )
                    if share_val is not None:
                        salary_lines.append(f"• с вилкой: {_format_percent(share_val)}")
        if salary_lines:
            sections.append("\n".join(salary_lines))

        # Top companies
        companies = df["company"].apply(_clean_text)
        if companies.str.len().gt(0).any():
            normalized = companies.str.lower()
            combined = pd.DataFrame({"orig": companies, "norm": normalized})
            combined = combined[combined["norm"].str.len() > 0]
            if not combined.empty:
                counts = combined.groupby("norm").size().sort_values(ascending=False)
                top_rows = []
                for idx, (norm_name, count) in enumerate(counts.head(5).items(), start=1):
                    display_name = combined[combined["norm"] == norm_name]["orig"].iloc[0]
                    top_rows.append(f"{idx}) {html.escape(display_name)} — {_format_int(count)}")
                if top_rows:
                    sections.append("\n".join(["<b>🏢 Топ работодателей</b>"] + top_rows))

        # Schedule / format
        if "schedule" in df.columns:
            schedule_series = df["schedule"].apply(_clean_text)
            schedule_series = schedule_series[schedule_series.str.len() > 0]
            if not schedule_series.empty:
                normalized = schedule_series.apply(lambda x: _schedule_label(x) if x else "")
                normalized = normalized[normalized.str.len() > 0]
                if not normalized.empty:
                    counter = Counter(normalized)
                    line = _format_share_line(counter, sum(counter.values()), 4)
                    if line:
                        sections.append("\n".join(["<b>🗓 График/формат</b>", line]))

        # Experience
        if "experience" in df.columns:
            exp_series = df["experience"].apply(_clean_text)
            exp_series = exp_series[exp_series.str.len() > 0]
            if not exp_series.empty:
                mapped = exp_series.apply(_map_experience)
                mapped = mapped[mapped.str.len() > 0]
                if not mapped.empty:
                    counter = Counter(mapped)
                    total = sum(counter.values())
                    ordered_labels = ["без опыта", "1–3", "3–6", "6+"]
                    parts = []
                    for label in ordered_labels:
                        count = counter.get(label)
                        if count:
                            parts.append(f"{label} — {_format_percent((count / total) * 100)}")
                    if parts:
                        sections.append("\n".join(["<b>🧩 Опыт</b>", " • ".join(parts)]))

        # Published at / new in 7 days
        if "published_at" in df.columns:
            dates = pd.to_datetime(df["published_at"], errors="coerce", utc=True, dayfirst=True)
            dates = dates.dropna()
            if not dates.empty:
                now = datetime.now(timezone.utc)
                week_ago = now - timedelta(days=7)
                recent = dates[dates >= week_ago]
                last7 = int(recent.shape[0])

                day_counts: list[int] = []
                for offset in range(6, -1, -1):
                    day = (now - timedelta(days=offset)).date()
                    ts = pd.Timestamp(day, tz=timezone.utc)
                    day_counts.append(int(dates.dt.floor("D").eq(ts).sum()))

                sparkline = _sparkline(day_counts)
                sections.append(
                    "\n".join(
                        [
                            f"<b>🕒 Новые за 7 дней:</b> {_format_int(last7)}",
                            sparkline,
                        ]
                    )
                )

        # Source shares
        if "source" in df.columns:
            sources = df["source"].apply(_clean_text)
            sources = sources[sources.str.len() > 0]
            if not sources.empty:
                normalized = sources.str.lower().map(lambda x: _SOURCE_ALIASES.get(x, x))
                counter = Counter(normalized)
                total = sum(counter.values())
                line = _format_share_line(counter, total, 4)
                if line:
                    sections.append(f"<b>🔗 Источник</b> {line}")

        filters_line = _filters_line(include, exclude)
        if filters_line:
            sections.append(filters_line)

        return "\n\n".join(sections)
    except Exception as exc:  # pragma: no cover
        log.warning("mini_analytics: failed to render analytics for %s: %s", path, exc, exc_info=True)
        return None
