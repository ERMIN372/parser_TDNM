from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from app.utils.diagnostics import write_bundle
from app.utils.logging import get_operation_context, log_event

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


def _postfilter_any(
    xlsx_path: Path,
    include: List[str],
    exclude: List[str],
    *,
    csv_path: Path | None = None,
) -> None:
    if not xlsx_path or not xlsx_path.exists():
        return
    source_csv = csv_path if csv_path and csv_path.exists() else xlsx_path.parent / "raw.csv"
    df = _load_table(source_csv, xlsx_path)
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
    diagnostic: Dict[str, Any] | None = None,
) -> Optional[List[Tuple[str, str, str]]]:
    """
    Быстрое превью первых PREVIEW_ROWS карточек:
    1) В зависимости от PREVIEW_MODE используем HH API или пайплайн.
    2) Если выбран режим *first* и он неудачный — пробуем второй вариант.
    Возвращает список [(title, company, url)] или None при полном фэйле/таймауте.
    """
    if diagnostic is not None:
        diagnostic.clear()
        diagnostic.update(
            {
                "mode": None,
                "command": None,
                "stdout": [],
                "stderr": [],
                "attempt": None,
                "error": None,
                "returncode": None,
            }
        )

    if not query or not city:
        return None

    def _try_api() -> Optional[List[Tuple[str, str, str]]]:
        rows = _hh_preview_rows(query, area, include, exclude, PREVIEW_ROWS)
        if diagnostic is not None:
            diagnostic.update(
                {
                    "mode": "api",
                    "command": None,
                    "stdout": [],
                    "stderr": [],
                    "attempt": None,
                    "error": None,
                    "returncode": None,
                }
            )
        return rows

    async def _try_pipeline() -> Optional[List[Tuple[str, str, str]]]:
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
                "--formats", "csv",
                "--keep-csv",
                "--site", "hh",
            ]
            if area is not None:
                cmd += ["--area", str(area)]

            log.info("Preview attempt %d: %s", attempt, " ".join(map(str, cmd)))
            if diagnostic is not None:
                diagnostic.update(
                    {
                        "mode": "pipeline",
                        "command": [str(part) for part in cmd],
                        "attempt": attempt,
                        "error": None,
                        "stdout": [],
                        "stderr": [],
                        "returncode": None,
                    }
                )
            try:
                proc = await asyncio.to_thread(
                    subprocess.run, cmd, capture_output=True, text=True, timeout=PREVIEW_TIMEOUT
                )
            except subprocess.TimeoutExpired as exc:
                log.warning("preview timeout (attempt %d)", attempt)
                if diagnostic is not None:
                    diagnostic.update(
                        {
                            "error": "timeout",
                            "stdout": (exc.output or "").splitlines(),
                            "stderr": (exc.stderr or "").splitlines(),
                            "returncode": None,
                        }
                    )
                continue
            except Exception as exc:
                log.warning("preview failed to start pipeline: %s", exc)
                if diagnostic is not None:
                    diagnostic.update(
                        {
                            "error": str(exc),
                            "stdout": [],
                            "stderr": [],
                            "returncode": None,
                        }
                    )
                continue

            if diagnostic is not None:
                diagnostic.update(
                    {
                        "stdout": (proc.stdout or "").splitlines(),
                        "stderr": (proc.stderr or "").splitlines(),
                        "returncode": proc.returncode,
                    }
                )
            if proc.returncode != 0:
                log.warning("preview failed rc=%s: %s", proc.returncode, proc.stderr)
                if diagnostic is not None:
                    diagnostic["error"] = f"rc_{proc.returncode}"
                continue

            df = _load_table(out.parent / "raw.csv", None)
            if df is None or df.empty:
                if diagnostic is not None:
                    diagnostic["error"] = "empty"
                return []

            inc_words = [w.lower() for w in _to_list(include)]
            exc_words = [w.lower() for w in _to_list(exclude)]
            if inc_words or exc_words:
                text_cols = [c for c in df.columns if df[c].dtype == object]
                blob = (df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower())
                mask_inc = True
                if inc_words:
                    mask_inc = False
                    for w in inc_words:
                        mask_inc = mask_inc | blob.str.contains(w, na=False)
                mask_exc = False
                for w in exc_words:
                    mask_exc = mask_exc | blob.str.contains(w, na=False)
                df = df[mask_inc & (~mask_exc)]

            def _norm(value: object) -> str:
                if isinstance(value, str):
                    return value.strip()
                return str(value or "").strip()

            col_title = next((c for c in df.columns if c.lower() in {"name", "title", "vacancy", "position"}), df.columns[0])
            col_company = next((c for c in df.columns if "company" in c.lower()), df.columns[0])
            col_url = next((c for c in df.columns if "url" in c.lower() or "link" in c.lower()), df.columns[0])

            rows: List[Tuple[str, str, str]] = []
            for _, row in df.head(PREVIEW_ROWS).iterrows():
                rows.append(
                    (
                        _norm(row.get(col_title, "")),
                        _norm(row.get(col_company, "")),
                        _norm(row.get(col_url, "")),
                    )
                )
            if diagnostic is not None:
                diagnostic["error"] = None
            return rows

        return None

    mode = PREVIEW_MODE.lower()
    if mode == "api_only":
        return _try_api()
    if mode == "pipeline_only":
        return await _try_pipeline()

    if mode == "api_first":
        rows = _try_api()
        if rows:
            return rows
        return await _try_pipeline()

    rows = await _try_pipeline()
    if rows:
        return rows
    return _try_api()


async def preview_rows(
    user_id: int,
    query: str,
    city: str,
    *,
    area: Optional[int] = None,
    include: Iterable[str] | str | None = None,
    exclude: Iterable[str] | str | None = None,
    progress: ProgressCallback | None = None,
) -> PreviewResult:
    """Возвращает первые строки превью вместе с диагностикой."""

    def _norm(val) -> str:
        if val is None:
            return ""
        return str(val).strip()

    def _norm_salary(val) -> str:
        if isinstance(val, dict):
            text = val.get("text")
            if text:
                return _norm(text)
            fr = val.get("from")
            to = val.get("to")
            currency = val.get("currency")
            if fr and to:
                base = f"{fr}–{to}"
            elif fr:
                base = f"от {fr}"
            elif to:
                base = f"до {to}"
            else:
                base = ""
            if base and currency:
                return f"{base} {currency}".strip()
            return base
        return _norm(val)

    include_list = _to_list(include)
    exclude_list = _to_list(exclude)

    diagnostic: Dict[str, Any] = {}
    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    bundle_prefix = f"preview_{uuid.uuid4().hex[:6]}"
    base_meta: Dict[str, Any] = {
        "mode": "preview",
        "user_id": user_id,
        "query": query,
        "city": city,
        "area": area,
        "include": include_list,
        "exclude": exclude_list,
    }

    STAGE_KEYS = ("preflight", "fetch", "normalize", "write_xlsx")
    STAGE_WEIGHTS = {"preflight": 5.0, "fetch": 60.0, "normalize": 20.0, "write_xlsx": 15.0}
    stage_progress: Dict[str, float] = {key: 0.0 for key in STAGE_KEYS}
    progress_state: Dict[str, Any] = {
        "percent": 0.0,
        "stage": None,
        "stages": {key: 0.0 for key in STAGE_KEYS},
    }
    ctx = get_operation_context()
    correlation_id = ctx.correlation_id if ctx else str(uuid.uuid4())

    async def _emit(event: str, payload: dict) -> None:
        if not progress:
            return
        try:
            await progress(event, payload)
        except Exception:  # pragma: no cover
            log.warning("preview progress callback failed", exc_info=True)

    def _update(stage: str, value: float) -> None:
        if stage not in stage_progress:
            return
        val = max(0.0, min(1.0, value))
        if val >= stage_progress[stage]:
            stage_progress[stage] = val
        percent = 0.0
        for key, weight in STAGE_WEIGHTS.items():
            percent += weight * stage_progress[key]
        if stage_progress["write_xlsx"] < 1.0:
            percent = min(percent, 99.0)
        progress_state["percent"] = round(percent, 2)
        progress_state["stage"] = stage
        progress_state["stages"] = {key: round(val, 4) for key, val in stage_progress.items()}

    def _progress_snapshot() -> Dict[str, Any]:
        return {
            "percent": progress_state.get("percent"),
            "stage": progress_state.get("stage"),
            "stages": dict(progress_state.get("stages", {})),
        }

    _update("preflight", 1.0)
    if progress:
        await _emit(
            "start",
            {
                "correlation_id": correlation_id,
                "mode": "preview",
                "site": "hh",
            },
        )
        await _emit("update", {"stage": "preflight", "subprogress": 1.0})

    try:
        _update("fetch", 0.1)
        await _emit("update", {"stage": "fetch", "subprogress": 0.1})
        rows_raw = await preview_report(
            user_id,
            query,
            city,
            area=area,
            include=include,
            exclude=exclude,
            diagnostic=diagnostic,
        )
        _update("fetch", 1.0)
        await _emit("update", {"stage": "fetch", "subprogress": 1.0})
    except Exception as exc:
        stack_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        error_meta = dict(base_meta)
        error_meta.update(
            {
                "status": "error",
                "error": str(exc),
                "source_mode": diagnostic.get("mode"),
                "attempt": diagnostic.get("attempt"),
                "command": diagnostic.get("command"),
                "returncode": diagnostic.get("returncode"),
            }
        )
        error_meta["progress"] = _progress_snapshot()
        bundle = write_bundle(
            user_dir,
            bundle_prefix,
            error_meta,
            diagnostic.get("stdout") or [],
            diagnostic.get("stderr") or [],
            stack=stack_text,
        )
        log_event(
            "diagnostic_bundle_ready",
            level="ERROR",
            message="Diagnostic bundle prepared",
            kind="preview",
            ok=False,
            err=str(exc),
            path=str(bundle.path),
            bundle_id=bundle.bundle_id,
        )
        raise DiagnosticError(
            str(exc) or "preview_failed",
            bundle_path=bundle.path,
            bundle_id=bundle.bundle_id,
        ) from exc

    rows_out: List[dict[str, str]] = []
    if rows_raw:
        for item in rows_raw:
            if isinstance(item, dict):
                title = _norm(
                    item.get("title")
                    or item.get("name")
                    or item.get("vacancy")
                    or item.get("position")
                )
                employer = item.get("employer") or {}
                if not isinstance(employer, dict):
                    employer = {"name": employer}
                company = _norm(
                    item.get("company")
                    or item.get("employer_name")
                    or employer.get("name")
                )
                salary = _norm_salary(item.get("salary"))
                link = _norm(item.get("link") or item.get("url") or item.get("alternate_url"))
            elif isinstance(item, (list, tuple)):
                parts = list(item) + ["", "", ""]
                title = _norm(parts[0])
                company = _norm(parts[1])
                link = _norm(parts[2])
                salary = _norm(parts[3])
            else:
                title = _norm(item)
                company = ""
                link = ""
                salary = ""

            rows_out.append(
                {
                    "title": title,
                    "company": company,
                    "salary": salary,
                    "link": link,
                }
            )

    _update("normalize", 1.0)
    await _emit("update", {"stage": "normalize", "subprogress": 1.0})

    if stage_progress["write_xlsx"] < 0.2:
        _update("write_xlsx", 0.2)
        await _emit("update", {"stage": "write_xlsx", "subprogress": 0.2})

    success_meta = dict(base_meta)
    success_meta.update(
        {
            "status": "ok",
            "rows": len(rows_out),
            "source_mode": diagnostic.get("mode"),
            "attempt": diagnostic.get("attempt"),
            "command": diagnostic.get("command"),
            "returncode": diagnostic.get("returncode"),
            "error": diagnostic.get("error"),
        }
    )
    _update("write_xlsx", 1.0)
    await _emit("update", {"stage": "write_xlsx", "subprogress": 1.0})
    success_meta["progress"] = _progress_snapshot()

    bundle = write_bundle(
        user_dir,
        bundle_prefix,
        success_meta,
        diagnostic.get("stdout") or [],
        diagnostic.get("stderr") or [],
    )
    log_event(
        "diagnostic_bundle_ready",
        message="Diagnostic bundle prepared",
        kind="preview",
        ok=True,
        path=str(bundle.path),
        bundle_id=bundle.bundle_id,
        rows=len(rows_out),
    )

    return PreviewResult(rows=rows_out, bundle_path=bundle.path, bundle_id=bundle.bundle_id)

@dataclass
class ReportResult:
    xlsx_path: Path
    csv_path: Path | None = None
    bundle_path: Path | None = None
    bundle_id: str | None = None


@dataclass
class PreviewResult:
    rows: List[dict[str, str]]
    bundle_path: Path | None = None
    bundle_id: str | None = None


class DiagnosticError(RuntimeError):
    def __init__(self, message: str, *, bundle_path: Path, bundle_id: str):
        super().__init__(message)
        self.bundle_path = bundle_path
        self.bundle_id = bundle_id


ProgressCallback = Callable[[str, dict], Awaitable[None]]


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
    progress: ProgressCallback | None = None,
) -> ReportResult:
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
        "--formats", "xlsx", "csv",
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

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    csv_path: Path | None = None

    bundle_meta: Dict[str, object] = {
        "mode": "report",
        "user_id": user_id,
        "query": query,
        "city": city,
        "role": role,
        "pages": pages,
        "per_page": per_page,
        "pause": pause,
        "site": site,
        "area": area,
        "include": inc,
        "exclude": exc,
        "timeout": eff_timeout,
        "command": [str(part) for part in cmd],
        "report_path": str(out_path),
    }
    bundle_prefix = f"report_{uuid.uuid4().hex[:6]}"

    STAGE_KEYS = ("preflight", "fetch", "normalize", "write_xlsx")
    STAGE_WEIGHTS = {"preflight": 5.0, "fetch": 60.0, "normalize": 20.0, "write_xlsx": 15.0}
    stage_progress: Dict[str, float] = {key: 0.0 for key in STAGE_KEYS}
    progress_state: Dict[str, Any] = {
        "percent": 0.0,
        "pages_done": 0,
        "pages_total": pages or 0,
        "stage": None,
        "stages": {key: 0.0 for key in STAGE_KEYS},
    }
    ctx = get_operation_context()
    correlation_id = ctx.correlation_id if ctx else str(uuid.uuid4())

    async def _emit(event: str, payload: dict) -> None:
        if not progress:
            return
        try:
            await progress(event, payload)
        except Exception:  # pragma: no cover
            log.warning("progress callback failed", exc_info=True)

    def _update_snapshot(
        stage: str,
        subprogress: float,
        *,
        pages_done: int | None = None,
        pages_total: int | None = None,
    ) -> None:
        if stage not in stage_progress:
            return
        value = max(0.0, min(1.0, subprogress))
        if value >= stage_progress[stage]:
            stage_progress[stage] = value
        if pages_total is not None and pages_total > 0:
            progress_state["pages_total"] = pages_total
        if pages_done is not None and pages_done >= 0:
            progress_state["pages_done"] = pages_done
        percent = 0.0
        for key, weight in STAGE_WEIGHTS.items():
            percent += weight * min(1.0, stage_progress[key])
        if stage_progress["write_xlsx"] < 1.0:
            percent = min(percent, 99.0)
        progress_state["percent"] = round(percent, 2)
        progress_state["stage"] = stage
        progress_state["stages"] = {key: round(val, 4) for key, val in stage_progress.items()}

    def _progress_snapshot() -> Dict[str, Any]:
        return {
            "percent": progress_state.get("percent"),
            "stage": progress_state.get("stage"),
            "pages_done": progress_state.get("pages_done"),
            "pages_total": progress_state.get("pages_total"),
            "stages": dict(progress_state.get("stages", {})),
        }

    _update_snapshot("preflight", 1.0, pages_total=pages)
    if progress:
        await _emit(
            "start",
            {
                "correlation_id": correlation_id,
                "mode": "report",
                "pages_total": pages,
                "site": site,
            },
        )
        await _emit(
            "update",
            {
                "stage": "preflight",
                "subprogress": 1.0,
                "pages_total": pages,
            },
        )

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def _read_stdout() -> None:
            nonlocal csv_path
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text_line = line.decode(errors="ignore").rstrip("\r\n")
                stdout_lines.append(text_line)
                try:
                    payload = json.loads(text_line)
                except json.JSONDecodeError:
                    continue
                status = payload.get("status")
                if status == "fetch_progress":
                    pages_done = int(payload.get("pages_done") or 0)
                    pages_total = int(payload.get("pages_total") or 0) or progress_state.get("pages_total") or (pages or 0)
                    sub = float(pages_done / pages_total) if pages_total else 0.0
                    _update_snapshot("fetch", sub, pages_done=pages_done, pages_total=pages_total)
                    await _emit(
                        "update",
                        {
                            "stage": "fetch",
                            "subprogress": sub,
                            "pages_done": pages_done,
                            "pages_total": pages_total,
                        },
                    )
                    continue
                if status == "csv" and payload.get("path"):
                    try:
                        csv_path = Path(payload["path"])
                    except Exception:  # pragma: no cover
                        csv_path = None
                    total_pages = progress_state.get("pages_total") or (pages or 0)
                    done_pages = progress_state.get("pages_done") or total_pages
                    _update_snapshot("fetch", 1.0, pages_done=done_pages, pages_total=total_pages)
                    await _emit(
                        "update",
                        {
                            "stage": "fetch",
                            "subprogress": 1.0,
                            "pages_done": done_pages,
                            "pages_total": total_pages,
                        },
                    )
                    if stage_progress["normalize"] < 0.05:
                        _update_snapshot("normalize", 0.05)
                        await _emit("update", {"stage": "normalize", "subprogress": 0.05})
                    continue
                if status == "report" and payload.get("format") == "xlsx":
                    _update_snapshot("write_xlsx", 1.0)
                    await _emit("update", {"stage": "write_xlsx", "subprogress": 1.0})
                    continue

        async def _read_stderr() -> None:
            assert proc.stderr is not None
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                stderr_lines.append(line.decode(errors="ignore").rstrip("\r\n"))

        stdout_task = asyncio.create_task(_read_stdout())
        stderr_task = asyncio.create_task(_read_stderr())

        try:
            await asyncio.wait_for(proc.wait(), timeout=eff_timeout)
        except asyncio.TimeoutError as e:
            proc.kill()
            await proc.wait()
            log.error("Parser timeout")
            raise RuntimeError("Превышено время ожидания парсера") from e
        finally:
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

        if proc.returncode != 0:
            stdout_text = "\n".join(stdout_lines)
            stderr_text = "\n".join(stderr_lines)
            log.error(
                "Parser failed (rc=%s)\nstdout:\n%s\nstderr:\n%s",
                proc.returncode,
                stdout_text,
                stderr_text,
            )
            raise RuntimeError(
                f"Не удалось получить отчёт: парсер завершился с ошибкой {proc.returncode}"
            )

        if inc or exc:
            if stage_progress["normalize"] < 0.2:
                _update_snapshot("normalize", 0.2)
                await _emit("update", {"stage": "normalize", "subprogress": 0.2})
            await asyncio.to_thread(_postfilter_any, out_path, inc, exc, csv_path=csv_path)
            _update_snapshot("normalize", 1.0)
            await _emit(
                "update",
                {
                    "stage": "normalize",
                    "subprogress": 1.0,
                    "csv_path": str(csv_path) if csv_path else None,
                },
            )
        else:
            if stage_progress["normalize"] < 1.0:
                _update_snapshot("normalize", 1.0)
                await _emit("update", {"stage": "normalize", "subprogress": 1.0})

        if stage_progress["write_xlsx"] < 0.2:
            _update_snapshot("write_xlsx", 0.2)
            await _emit("update", {"stage": "write_xlsx", "subprogress": 0.2})

        bundle_meta["csv_path"] = str(csv_path) if csv_path else None
        bundle_meta["progress"] = _progress_snapshot()
    except Exception as exc:
        stack_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        error_meta = dict(bundle_meta)
        error_meta["status"] = "error"
        error_meta["error"] = str(exc)
        error_meta["progress"] = _progress_snapshot()
        bundle = write_bundle(
            user_dir,
            bundle_prefix,
            error_meta,
            stdout_lines,
            stderr_lines,
            stack=stack_text,
        )
        log_event(
            "diagnostic_bundle_ready",
            level="ERROR",
            message="Diagnostic bundle prepared",
            kind="report",
            ok=False,
            err=str(exc),
            path=str(bundle.path),
            bundle_id=bundle.bundle_id,
        )
        raise DiagnosticError(
            str(exc) or "report_failed",
            bundle_path=bundle.path,
            bundle_id=bundle.bundle_id,
        ) from exc

    success_meta = dict(bundle_meta)
    success_meta["status"] = "ok"
    success_meta["progress"] = _progress_snapshot()
    bundle = write_bundle(
        user_dir,
        bundle_prefix,
        success_meta,
        stdout_lines,
        stderr_lines,
    )
    log_event(
        "diagnostic_bundle_ready",
        message="Diagnostic bundle prepared",
        kind="report",
        ok=True,
        path=str(bundle.path),
        bundle_id=bundle.bundle_id,
    )
    return ReportResult(
        xlsx_path=out_path,
        csv_path=csv_path,
        bundle_path=bundle.path,
        bundle_id=bundle.bundle_id,
    )
