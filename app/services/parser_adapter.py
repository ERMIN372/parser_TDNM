from __future__ import annotations
import os
import sys
import asyncio
import subprocess
import logging
from pathlib import Path
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

log = logging.getLogger(__name__)

PYBIN = os.getenv("PYBIN", sys.executable or "python3")
PIPELINE = os.getenv("PARSER_PIPELINE", "vendor/parser_tdnm/run_pipeline.py")
REPORT_DIR = Path(os.getenv("REPORT_DIR", "reports"))

DEFAULT_TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "180"))
LARGE_TIMEOUT   = int(os.getenv("PARSER_TIMEOUT_LARGE", "600"))

PREVIEW_PER_PAGE = int(os.getenv("PREVIEW_PER_PAGE", "20"))
PREVIEW_MODE     = os.getenv("PREVIEW_MODE", "api_first").strip()  # api_first | pipeline_first | api_only | pipeline_only

# превью
PREVIEW_TIMEOUT = int(os.getenv("PREVIEW_TIMEOUT", "35"))
PREVIEW_RETRIES = int(os.getenv("PREVIEW_RETRIES", "2"))
PREVIEW_ROWS    = int(os.getenv("PREVIEW_ROWS", "5"))

REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _to_list(val: Optional[Iterable[str] | str]) -> List[str]:
    if not val:
        return []
    if isinstance(val, str):
        parts = [p.strip() for p in val.replace(";", ",").split(",")]
        return [p for p in parts if p]
    return [str(x).strip() for x in val if str(x).strip()]


def _load_table(path_csv: Path, path_xlsx: Optional[Path] = None):
    try:
        import pandas as pd
    except Exception as e:
        log.warning("pandas missing for table ops: %s", e)
        return None
    try:
        if path_csv and path_csv.exists():
            return pd.read_csv(path_csv)
        if path_xlsx and path_xlsx.exists():
            return pd.read_excel(path_xlsx)
    except Exception as e:
        log.warning("failed to load table: %s", e)
    return None


def _postfilter_any(xlsx_path: Path, include: List[str], exclude: List[str]) -> None:
    if not xlsx_path or not xlsx_path.exists():
        return
    df = _load_table(xlsx_path.parent / "raw.csv", xlsx_path)
    if df is None:
        return

    import pandas as pd  # safe, checked above
    text_cols = [c for c in df.columns if df[c].dtype == object]
    if not text_cols:
        return

    blob = (df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower())
    inc = [w.lower() for w in include]
    exc = [w.lower() for w in exclude]

    mask_inc = True
    if inc:
        mask_inc = False
        for w in inc:
            mask_inc = mask_inc | blob.str.contains(w, na=False)

    mask_exc = False
    for w in exc:
        mask_exc = mask_exc | blob.str.contains(w, na=False)

    filtered = df[mask_inc & (~mask_exc)].copy()
    try:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as wr:
            filtered.to_excel(wr, index=False, sheet_name="vacancies")
    except Exception as e:
        log.warning("postfilter: failed to write xlsx: %s", e)

def _hh_preview_rows(query: str, area: int | None, include, exclude, rows: int) -> Optional[list[tuple[str,str,str]]]:
    try:
        import requests
    except Exception as e:
        log.warning("hh api preview skipped (no requests): %s", e)
        return None

    params = {
        "text": query,
        "per_page": max(1, min(100, rows)),
        "page": 0,
        "search_field": "name",
    }
    if area:
        params["area"] = area

    try:
        r = requests.get(
            "https://api.hh.ru/vacancies",
            params=params,
            timeout=8,
            headers={"User-Agent": "hr-assist-bot/preview"},
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
    except Exception as e:
        log.warning("hh api preview error: %s", e)
        return None

    def _norm(s): return (s or "").strip()
    inc = [w.lower() for w in _to_list(include)]
    exc = [w.lower() for w in _to_list(exclude)]

    rows_out: list[tuple[str,str,str]] = []
    for it in items:
        title = _norm(it.get("name"))
        comp  = _norm((it.get("employer") or {}).get("name"))
        url   = _norm(it.get("alternate_url"))
        blob  = " ".join([
            title,
            comp,
            _norm(((it.get("snippet") or {}).get("requirement"))),
            _norm(((it.get("snippet") or {}).get("responsibility"))),
        ]).lower()

        ok_inc = True
        if inc:
            ok_inc = any(w in blob for w in inc)
        ok_exc = any(w in blob for w in exc)

        if ok_inc and not ok_exc:
            rows_out.append((title, comp, url))

    return rows_out[:rows]

async def preview_report(
    user_id: int,
    query: str,
    city: str,
    *,
    area: Optional[int] = None,
    include: Iterable[str] | str | None = None,
    exclude: Iterable[str] | str | None = None,
) -> Optional[List[Tuple[str, str, str]]]:
    """
    Быстрое превью первых PREVIEW_ROWS карточек:
    1) В зависимости от PREVIEW_MODE используем HH API или пайплайн.
    2) Если выбран режим *first* и он неудачный — пробуем второй вариант.
    Возвращает список [(title, company, url)] или None при полном фэйле/таймауте.
    """
    if not query or not city:
        return None

    # локальные шорткаты
    def _try_api():
        return _hh_preview_rows(query, area, include, exclude, PREVIEW_ROWS)

    async def _try_pipeline():
        uid_dir = REPORT_DIR / str(user_id)
        uid_dir.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, PREVIEW_RETRIES + 1):
            out = uid_dir / f"_preview_{attempt}.xlsx"
            cmd = [
                PYBIN, PIPELINE,
                "--query", query,
                "--city", city,
                "--pages", "1",
                "--per_page", str(PREVIEW_PER_PAGE),
                "--output", str(out),
                "--formats", "csv",     # только csv для скорости
                "--keep-csv",
                "--site", "hh",
            ]
            if area is not None:
                cmd += ["--area", str(area)]

            log.info("Preview attempt %d: %s", attempt, " ".join(map(str, cmd)))
            try:
                proc = await asyncio.to_thread(
                    subprocess.run, cmd, capture_output=True, text=True, timeout=PREVIEW_TIMEOUT
                )
            except subprocess.TimeoutExpired:
                log.warning("preview timeout (attempt %d)", attempt)
                continue

            if proc.returncode != 0:
                log.warning("preview failed rc=%s: %s", proc.returncode, proc.stderr)
                continue

            df = _load_table(out.parent / "raw.csv", None)
            if df is None or df.empty:
                return []

            # ранний include/exclude
            inc = [w.lower() for w in _to_list(include)]
            exc = [w.lower() for w in _to_list(exclude)]
            if inc or exc:
                text_cols = [c for c in df.columns if df[c].dtype == object]
                blob = (df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower())
                mask_inc = True
                if inc:
                    mask_inc = False
                    for w in inc:
                        mask_inc = mask_inc | blob.str.contains(w, na=False)
                mask_exc = False
                for w in exc:
                    mask_exc = mask_exc | blob.str.contains(w, na=False)
                df = df[mask_inc & (~mask_exc)]

            # собрать строки
            def _norm(s): return (s or "").strip()
            col_title   = next((c for c in df.columns if c.lower() in {"name","title","vacancy","position"}), df.columns[0])
            col_company = next((c for c in df.columns if "company" in c.lower()), df.columns[0])
            col_url     = next((c for c in df.columns if "url" in c.lower() or "link" in c.lower()), df.columns[0])

            rows: List[Tuple[str,str,str]] = []
            for _, r in df.head(PREVIEW_ROWS).iterrows():
                rows.append((_norm(str(r.get(col_title, ""))),
                             _norm(str(r.get(col_company, ""))),
                             _norm(str(r.get(col_url, "")))))
            return rows

        return None

    mode = PREVIEW_MODE.lower()
    if mode == "api_only":
        return _try_api()
    if mode == "pipeline_only":
        return await _try_pipeline()

    # смешанные режимы
    if mode == "api_first":
        rows = _try_api()
        if rows:
            return rows
        return await _try_pipeline()

    # pipeline_first (по умолчанию на случай опечатки)
    rows = await _try_pipeline()
    if rows:
        return rows
    return _try_api()


async def run_report(
    user_id: int,
    query: str,
    city: str,
    *,
    role: str | None = None,
    pages: int | None = None,
    per_page: int | None = None,
    pause: float | None = None,
    site: str | None = None,
    area: int | None = None,
    include: Iterable[str] | str | None = None,
    exclude: Iterable[str] | str | None = None,
    timeout: int | None = None,
) -> Path:
    if not query or not city:
        raise RuntimeError("Неверные параметры поиска (пустые город/должность).")

    inc = _to_list(include)
    exc = _to_list(exclude)

    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = user_dir / f"data_{ts}.xlsx"

    cmd = [
        PYBIN, PIPELINE,
        "--query", query,
        "--city", city,
        "--output", str(out_path),
        "--formats", "xlsx", "csv",   # отдельно значениями
        "--keep-csv",
    ]
    if role:
        cmd += ["--role", role]
    if pages is not None:
        cmd += ["--pages", str(pages)]
    if per_page is not None:
        cmd += ["--per_page", str(per_page)]
    if pause is not None:
        cmd += ["--pause", str(pause)]
    if site is not None:
        cmd += ["--site", site]
    if area is not None:
        cmd += ["--area", str(area)]

    eff_timeout = timeout or (LARGE_TIMEOUT if (pages or 0) > 2 or (per_page or 0) >= 100 else DEFAULT_TIMEOUT)
    log.info("Running parser: %s", " ".join(map(str, cmd)))

    try:
        proc = await asyncio.to_thread(
            subprocess.run, cmd, capture_output=True, text=True, timeout=eff_timeout
        )
    except subprocess.TimeoutExpired as e:
        log.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера") from e

    if proc.returncode != 0:
        log.error("Parser failed (rc=%s)\nstdout:\n%s\nstderr:\n%s", proc.returncode, proc.stdout, proc.stderr)
        raise RuntimeError(f"Не удалось получить отчёт: парсер завершился с ошибкой {proc.returncode}")

    if inc or exc:
        _postfilter_any(out_path, inc, exc)

    return out_path
