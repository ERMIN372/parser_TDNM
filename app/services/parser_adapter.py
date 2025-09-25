from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Iterable, List, Optional, Tuple

from app.services.diagnostics import remember_bundle
from app.utils.logging import current_correlation_id, log_event, update_context

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

DEBUG_ENABLED = os.getenv("DEBUG", "0").lower() in {"1", "true", "yes"}
SNIPPET_LINES = max(1, int(os.getenv("LOG_SNIPPET_LINES", "15")))
METRICS_FLUSH_EVERY = max(1, int(os.getenv("METRICS_FLUSH_INTERVAL", "10")))
ERROR_SNIPPET_LINES = 30

_parse_metrics = {"total": 0, "ok": 0, "failed": 0, "timeout": 0, "duration_sum": 0}
_preview_metrics = {"attempts": 0, "ok": 0, "timeout": 0}


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
) -> Optional[List[Tuple[str, str, str]]]:
    """
    Быстрое превью первых PREVIEW_ROWS карточек:
    1) В зависимости от PREVIEW_MODE используем HH API или пайплайн.
    2) Если выбран режим *first* и он неудачный — пробуем второй вариант.
    Возвращает список [(title, company, url)] или None при полном фэйле/таймауте.
    """
    if not query or not city:
        return None

    inc_list = _to_list(include)
    exc_list = _to_list(exclude)
    common_log_fields = {
        "query": query,
        "city": city,
        "include": inc_list,
        "exclude": exc_list,
    }

    attempt_index = 0
    failure_reasons: list[str] = []

    def _api_attempt() -> Optional[List[Tuple[str, str, str]]]:
        nonlocal attempt_index
        attempt_index += 1
        start = time.perf_counter()
        log_event("preview_attempt_start", source="api", attempt=attempt_index, **common_log_fields)
        rows: Optional[List[Tuple[str, str, str]]] = None
        try:
            rows = _hh_preview_rows(query, area, inc_list, exc_list, PREVIEW_ROWS)
        except Exception as exc:  # pragma: no cover - network errors best effort
            duration = int((time.perf_counter() - start) * 1000)
            log_event(
                "preview_attempt_failed",
                level="ERROR",
                source="api",
                attempt=attempt_index,
                duration_ms=duration,
                err=str(exc),
                **common_log_fields,
            )
            _register_preview_attempt(ok=False, timeout=False)
            failure_reasons.append(f"api:error:{exc}")
            return None

        duration = int((time.perf_counter() - start) * 1000)
        if rows:
            log_event(
                "preview_attempt_ok",
                source="api",
                attempt=attempt_index,
                duration_ms=duration,
                rows=len(rows),
                **common_log_fields,
            )
            _register_preview_attempt(ok=True, timeout=False)
            return rows

        log_event(
            "preview_attempt_failed",
            level="WARN",
            source="api",
            attempt=attempt_index,
            duration_ms=duration,
            err="no_rows",
            **common_log_fields,
        )
        _register_preview_attempt(ok=False, timeout=False)
        failure_reasons.append("api:no_rows")
        return None

    async def _pipeline_attempts() -> Optional[List[Tuple[str, str, str]]]:
        nonlocal attempt_index
        uid_dir = REPORT_DIR / str(user_id)
        uid_dir.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, PREVIEW_RETRIES + 1):
            attempt_index += 1
            out = uid_dir / f"_preview_{attempt}.xlsx"
            cmd = [
                PYBIN,
                PIPELINE,
                "--query",
                query,
                "--city",
                city,
                "--pages",
                "1",
                "--per_page",
                str(PREVIEW_PER_PAGE),
                "--output",
                str(out),
                "--formats",
                "csv",
                "--keep-csv",
                "--site",
                "hh",
            ]
            if area is not None:
                cmd += ["--area", str(area)]

            start = time.perf_counter()
            log_event(
                "preview_attempt_start",
                source="pipeline",
                attempt=attempt_index,
                internal_attempt=attempt,
                command_line=" ".join(map(str, cmd)),
                **common_log_fields,
            )
            try:
                proc = await asyncio.to_thread(
                    subprocess.run,
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=PREVIEW_TIMEOUT,
                )
            except subprocess.TimeoutExpired:
                duration = int((time.perf_counter() - start) * 1000)
                log_event(
                    "preview_attempt_timeout",
                    level="WARN",
                    source="pipeline",
                    attempt=attempt_index,
                    duration_ms=duration,
                    timeout=PREVIEW_TIMEOUT,
                    **common_log_fields,
                )
                _register_preview_attempt(ok=False, timeout=True)
                failure_reasons.append("pipeline:timeout")
                continue
            except Exception as exc:  # pragma: no cover - spawn errors are rare
                duration = int((time.perf_counter() - start) * 1000)
                log_event(
                    "preview_attempt_failed",
                    level="ERROR",
                    source="pipeline",
                    attempt=attempt_index,
                    duration_ms=duration,
                    err=str(exc),
                    **common_log_fields,
                )
                _register_preview_attempt(ok=False, timeout=False)
                failure_reasons.append(f"pipeline:error:{exc}")
                continue

            duration = int((time.perf_counter() - start) * 1000)
            if proc.returncode != 0:
                log_event(
                    "preview_attempt_failed",
                    level="WARN",
                    source="pipeline",
                    attempt=attempt_index,
                    duration_ms=duration,
                    err=f"rc={proc.returncode}",
                    stderr_snippet=proc.stderr.splitlines()[:5],
                    **common_log_fields,
                )
                _register_preview_attempt(ok=False, timeout=False)
                failure_reasons.append(f"pipeline:rc{proc.returncode}")
                continue

            df = _load_table(out.parent / "raw.csv", None)
            if df is None or df.empty:
                log_event(
                    "preview_attempt_ok",
                    source="pipeline",
                    attempt=attempt_index,
                    duration_ms=duration,
                    rows=0,
                    **common_log_fields,
                )
                _register_preview_attempt(ok=True, timeout=False)
                return []

            inc_lower = [w.lower() for w in inc_list]
            exc_lower = [w.lower() for w in exc_list]
            if inc_lower or exc_lower:
                text_cols = [c for c in df.columns if df[c].dtype == object]
                blob = (df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower())
                mask_inc = True
                if inc_lower:
                    mask_inc = False
                    for w in inc_lower:
                        mask_inc = mask_inc | blob.str.contains(w, na=False)
                mask_exc = False
                for w in exc_lower:
                    mask_exc = mask_exc | blob.str.contains(w, na=False)
                df = df[mask_inc & (~mask_exc)]

            def _norm_val(value: object) -> str:
                return (str(value) if value is not None else "").strip()

            col_title = next(
                (c for c in df.columns if c.lower() in {"name", "title", "vacancy", "position"}),
                df.columns[0],
            )
            col_company = next(
                (c for c in df.columns if "company" in c.lower()),
                df.columns[0],
            )
            col_url = next(
                (c for c in df.columns if "url" in c.lower() or "link" in c.lower()),
                df.columns[0],
            )

            rows: List[Tuple[str, str, str]] = []
            for _, record in df.head(PREVIEW_ROWS).iterrows():
                rows.append(
                    (
                        _norm_val(record.get(col_title, "")),
                        _norm_val(record.get(col_company, "")),
                        _norm_val(record.get(col_url, "")),
                    )
                )

            log_event(
                "preview_attempt_ok",
                source="pipeline",
                attempt=attempt_index,
                duration_ms=duration,
                rows=len(rows),
                **common_log_fields,
            )
            _register_preview_attempt(ok=True, timeout=False)
            return rows

        return None

    mode = PREVIEW_MODE.lower()
    result: Optional[List[Tuple[str, str, str]]] = None

    if mode == "api_only":
        result = _api_attempt()
    elif mode == "pipeline_only":
        result = await _pipeline_attempts()
    elif mode == "api_first":
        result = _api_attempt()
        if result is None:
            result = await _pipeline_attempts()
    else:  # pipeline_first and fallback
        result = await _pipeline_attempts()
        if result is None:
            result = _api_attempt()

    if result is None:
        log_event("preview_failed", level="WARN", reasons=failure_reasons, **common_log_fields)

    return result


async def preview_rows(
    user_id: int,
    query: str,
    city: str,
    *,
    area: Optional[int] = None,
    include: Iterable[str] | str | None = None,
    exclude: Iterable[str] | str | None = None,
) -> List[dict[str, str]]:
    """Возвращает первые строки превью в виде списка словарей."""

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

    rows_raw = await preview_report(
        user_id,
        query,
        city,
        area=area,
        include=include,
        exclude=exclude,
    )

    if not rows_raw:
        return []

    rows_out: List[dict[str, str]] = []
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

        rows_out.append({
            "title": title,
            "company": company,
            "salary": salary,
            "link": link,
        })

    return rows_out


@dataclass
class RunReportResult:
    ok: bool
    err_code: str | None = None
    err_message: str | None = None
    rc: int | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    xlsx_path: Path | None = None
    csv_path: Path | None = None
    bundle_path: Path | None = None
    duration_ms: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    user_message: str | None = None
    meta: dict[str, object] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.ok


ProgressCallback = Callable[[str, dict], Awaitable[None]]


def _register_parse_result(result: RunReportResult) -> None:
    _parse_metrics["total"] += 1
    _parse_metrics["duration_sum"] += int(result.duration_ms or 0)
    if result.ok:
        _parse_metrics["ok"] += 1
    elif result.err_code == "E_TIMEOUT":
        _parse_metrics["timeout"] += 1
    else:
        _parse_metrics["failed"] += 1
    _flush_metrics_if_needed()


def _register_preview_attempt(*, ok: bool, timeout: bool) -> None:
    _preview_metrics["attempts"] += 1
    if ok:
        _preview_metrics["ok"] += 1
    if timeout:
        _preview_metrics["timeout"] += 1
    _flush_metrics_if_needed()


def _flush_metrics_if_needed(force: bool = False) -> None:
    should_flush = force
    if _parse_metrics["total"] and _parse_metrics["total"] % METRICS_FLUSH_EVERY == 0:
        should_flush = True
    if _preview_metrics["attempts"] and _preview_metrics["attempts"] % METRICS_FLUSH_EVERY == 0:
        should_flush = True
    if not should_flush:
        return

    avg_duration = (
        _parse_metrics["duration_sum"] / _parse_metrics["total"]
        if _parse_metrics["total"]
        else 0
    )
    preview_success_rate = (
        _preview_metrics["ok"] / _preview_metrics["attempts"]
        if _preview_metrics["attempts"]
        else 0
    )
    preview_timeout_rate = (
        _preview_metrics["timeout"] / _preview_metrics["attempts"]
        if _preview_metrics["attempts"]
        else 0
    )

    log_event(
        "metrics_tick",
        parse_total=_parse_metrics["total"],
        parse_ok=_parse_metrics["ok"],
        parse_failed=_parse_metrics["failed"],
        parse_timeout=_parse_metrics["timeout"],
        parse_avg_duration_ms=int(avg_duration),
        preview_attempts=_preview_metrics["attempts"],
        preview_ok=_preview_metrics["ok"],
        preview_timeout=_preview_metrics["timeout"],
        preview_success_rate=round(preview_success_rate, 3),
        preview_timeout_rate=round(preview_timeout_rate, 3),
    )


def _collect_csv_head_tail(csv_path: Path) -> str:
    head: list[str] = []
    tail: list[str] = []
    try:
        with csv_path.open("r", encoding="utf-8", errors="ignore") as fh:
            for idx, line in enumerate(fh):
                clean = line.rstrip("\n")
                if idx < 30:
                    head.append(clean)
                tail.append(clean)
                if len(tail) > 30:
                    tail.pop(0)
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        return f"failed to read csv: {exc}"

    lines: list[str] = ["HEAD (30 lines max):"]
    lines.extend(head)
    lines.append("")
    lines.append("TAIL (30 lines max):")
    lines.extend(tail)
    return "\n".join(lines)


def _build_diagnostic_bundle(
    *,
    bundle_dir: Path,
    command: list[str],
    env_lines: list[str],
    stdout_lines: list[str],
    stderr_lines: list[str],
    meta: dict[str, object],
    csv_path: Path | None,
) -> Path | None:
    try:
        bundle_zip = bundle_dir.with_suffix(".zip")
        if bundle_zip.exists():
            bundle_zip.unlink()
        if bundle_dir.exists():
            shutil.rmtree(bundle_dir, ignore_errors=True)
        bundle_dir.mkdir(parents=True, exist_ok=True)

        command_text = " ".join(str(part) for part in command)
        (bundle_dir / "command.txt").write_text(command_text, encoding="utf-8")
        (bundle_dir / "env.txt").write_text("\n".join(env_lines), encoding="utf-8")
        (bundle_dir / "stdout.log").write_text("\n".join(stdout_lines), encoding="utf-8")
        (bundle_dir / "stderr.log").write_text("\n".join(stderr_lines), encoding="utf-8")

        meta_payload = json.loads(json.dumps(meta, default=str))
        (bundle_dir / "meta.json").write_text(
            json.dumps(meta_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if csv_path and csv_path.exists():
            head_tail = _collect_csv_head_tail(csv_path)
            (bundle_dir / "head_tail.txt").write_text(head_tail, encoding="utf-8")

        archive_path = shutil.make_archive(
            str(bundle_dir),
            "zip",
            root_dir=bundle_dir,
            base_dir=".",
        )
        return Path(archive_path)
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        log.warning("failed to build diagnostic bundle: %s", exc, exc_info=True)
        return None


BAD_ARGS_PATTERNS = (
    "unrecognized arguments",
    "invalid choice",
    "the following arguments are required",
    "expected one argument",
)


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
) -> RunReportResult:
    inc = _to_list(include)
    exc = _to_list(exclude)

    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow()
    out_path = user_dir / f"data_{ts.strftime('%Y%m%d_%H%M%S')}.xlsx"

    command: list[str] = [
        PYBIN,
        PIPELINE,
        "--query",
        query or "",
        "--city",
        city or "",
        "--output",
        str(out_path),
        "--formats",
        "xlsx",
        "csv",
        "--keep-csv",
    ]
    if role:
        command += ["--role", role]
    if pages is not None:
        command += ["--pages", str(pages)]
    if per_page is not None:
        command += ["--per_page", str(per_page)]
    if pause is not None:
        command += ["--pause", str(pause)]
    if site is not None:
        command += ["--site", site]
    if area is not None:
        command += ["--area", str(area)]

    eff_timeout = timeout or (
        LARGE_TIMEOUT if (pages or 0) > 2 or (per_page or 0) >= 100 else DEFAULT_TIMEOUT
    )

    correlation = current_correlation_id() or str(uuid.uuid4())
    bundle_dir = user_dir / f"bundle_{correlation}"

    env_lines = [
        f"python={platform.python_version()} ({sys.executable})",
        f"mode={os.getenv('MODE', '-')}",
        f"DEBUG={os.getenv('DEBUG', '0')}",
        f"PARSER_TIMEOUT={os.getenv('PARSER_TIMEOUT', DEFAULT_TIMEOUT)}",
        f"PARSER_TIMEOUT_LARGE={os.getenv('PARSER_TIMEOUT_LARGE', LARGE_TIMEOUT)}",
        f"PREVIEW_TIMEOUT={os.getenv('PREVIEW_TIMEOUT', PREVIEW_TIMEOUT)}",
        f"PREVIEW_RETRIES={os.getenv('PREVIEW_RETRIES', PREVIEW_RETRIES)}",
        f"PREVIEW_ROWS={os.getenv('PREVIEW_ROWS', PREVIEW_ROWS)}",
        f"PYBIN={PYBIN}",
        f"PIPELINE={PIPELINE} (exists={Path(PIPELINE).exists()})",
        f"REPORT_DIR={REPORT_DIR}",
        f"LOG_LEVEL={os.getenv('LOG_LEVEL', '-')}",
    ]

    common_log_fields: dict[str, object] = {
        "query": query,
        "city": city,
        "include": inc,
        "exclude": exc,
        "pages": pages,
        "per_page": per_page,
        "site": site,
        "area": area,
        "timeout": eff_timeout,
    }

    log_event(
        "parser_start",
        action="run_report",
        command_line=" ".join(str(part) for part in command),
        **common_log_fields,
    )

    started_at_dt = datetime.now(timezone.utc)
    started_perf = time.perf_counter()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    csv_path: Path | None = None
    proc: asyncio.subprocess.Process | None = None
    stdout_task: asyncio.Task | None = None
    stderr_task: asyncio.Task | None = None
    timeout_hit = False

    invalid_arguments = not query or not city
    spawn_error: Exception | None = None

    if not invalid_arguments:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def _read_stdout() -> None:
                nonlocal csv_path
                assert proc is not None and proc.stdout is not None
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    text_line = line.decode(errors="ignore").rstrip("\r\n")
                    stdout_lines.append(text_line)
                    if progress:
                        try:
                            payload = json.loads(text_line)
                        except json.JSONDecodeError:
                            continue
                        if payload.get("status") == "csv" and payload.get("path"):
                            try:
                                csv_path = Path(payload["path"])
                            except Exception:  # pragma: no cover - diagnostics best effort
                                csv_path = None
                        try:
                            await progress("status", payload)
                        except Exception:  # pragma: no cover - progress best effort
                            log.warning("progress callback failed", exc_info=True)

            async def _read_stderr() -> None:
                assert proc is not None and proc.stderr is not None
                while True:
                    line = await proc.stderr.readline()
                    if not line:
                        break
                    stderr_lines.append(line.decode(errors="ignore").rstrip("\r\n"))

            stdout_task = asyncio.create_task(_read_stdout())
            stderr_task = asyncio.create_task(_read_stderr())

            try:
                await asyncio.wait_for(proc.wait(), timeout=eff_timeout)
            except asyncio.TimeoutError:
                timeout_hit = True
                proc.kill()
                await proc.wait()
            finally:
                await asyncio.gather(
                    *(task for task in (stdout_task, stderr_task) if task),
                    return_exceptions=True,
                )
        except Exception as exc:  # pragma: no cover - spawn failure is rare
            spawn_error = exc
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            await asyncio.gather(
                *(task for task in (stdout_task, stderr_task) if task),
                return_exceptions=True,
            )
    else:
        spawn_error = ValueError("invalid search parameters")

    rc = proc.returncode if proc else None
    finished_at_dt = datetime.now(timezone.utc)
    duration_ms = int((time.perf_counter() - started_perf) * 1000)
    started_iso = started_at_dt.isoformat()
    finished_iso = finished_at_dt.isoformat()

    if (
        not timeout_hit
        and spawn_error is None
        and proc is not None
        and rc == 0
        and (inc or exc)
    ):
        if progress:
            try:
                await progress("filter_start", {"csv_path": str(csv_path) if csv_path else None})
            except Exception:  # pragma: no cover - diagnostics best effort
                log.warning("progress callback failed on filter_start", exc_info=True)
        await asyncio.to_thread(_postfilter_any, out_path, inc, exc, csv_path=csv_path)
        if progress:
            try:
                await progress("filter_done", {"csv_path": str(csv_path) if csv_path else None})
            except Exception:  # pragma: no cover - diagnostics best effort
                log.warning("progress callback failed on filter_done", exc_info=True)

    ok = False
    err_code: str | None = None
    err_message: str | None = None
    user_message: str | None = None

    if invalid_arguments:
        err_code = "E_BAD_ARGS"
        err_message = "Missing query or city"
        user_message = "Укажи должность и город для поиска."
    elif timeout_hit:
        err_code = "E_TIMEOUT"
        err_message = f"Parser timeout after {eff_timeout}s"
        user_message = "Сайт долго отвечает, попробуй ещё раз позже."
        log_event("parser_timeout", level="WARN", duration_ms=duration_ms, **common_log_fields)
    elif spawn_error is not None:
        err_code = "E_NONZERO_RC"
        err_message = str(spawn_error) or spawn_error.__class__.__name__
        user_message = "Парсер завершился с ошибкой."
    elif rc is None:
        err_code = "E_NONZERO_RC"
        err_message = "Parser process did not start"
        user_message = "Парсер завершился с ошибкой."
    elif rc != 0:
        err_code = "E_NONZERO_RC"
        err_message = f"Parser exited with code {rc}"
        user_message = "Парсер завершился с ошибкой."
        combined = "\n".join(stdout_lines + stderr_lines).lower()
        if any(pattern in combined for pattern in BAD_ARGS_PATTERNS):
            err_code = "E_BAD_ARGS"
            user_message = "Некорректные параметры запуска парсера."
    elif not out_path.exists():
        err_code = "E_NO_FILE"
        err_message = "Parser reported success but XLSX missing"
        user_message = "Файл отчёта не сформировался. Попробуй ещё раз."
    else:
        ok = True

    snippet_limit = ERROR_SNIPPET_LINES if not ok else SNIPPET_LINES
    if (DEBUG_ENABLED or not ok) and stdout_lines:
        log_event(
            "parser_stdout_snippet",
            snippet=stdout_lines[:snippet_limit],
            lines=len(stdout_lines),
            **common_log_fields,
        )
    if (DEBUG_ENABLED or not ok) and stderr_lines:
        log_event(
            "parser_stderr_snippet",
            level="WARN" if not ok else "INFO",
            snippet=stderr_lines[:snippet_limit],
            lines=len(stderr_lines),
            **common_log_fields,
        )

    meta: dict[str, object] = {
        "correlation_id": correlation,
        "started_at": started_iso,
        "finished_at": finished_iso,
        "duration_ms": duration_ms,
        "query": query,
        "city": city,
        "include": inc,
        "exclude": exc,
        "pages": pages,
        "per_page": per_page,
        "pause": pause,
        "site": site,
        "area": area,
        "role": role,
        "timeout": eff_timeout,
        "command": command,
        "stdout_lines": len(stdout_lines),
        "stderr_lines": len(stderr_lines),
        "result": {
            "ok": ok,
            "err_code": err_code,
            "err_message": err_message,
            "rc": rc,
        },
    }
    if csv_path:
        meta["csv_path"] = str(csv_path)
    if out_path.exists():
        meta["xlsx_path"] = str(out_path)

    bundle_path = _build_diagnostic_bundle(
        bundle_dir=bundle_dir,
        command=command,
        env_lines=env_lines,
        stdout_lines=stdout_lines,
        stderr_lines=stderr_lines,
        meta=meta,
        csv_path=csv_path,
    )

    if bundle_path:
        remember_bundle(chat_id=None, correlation_id=correlation, bundle_path=bundle_path)
        log_event("diagnostic_bundle_ready", bundle_path=str(bundle_path), **common_log_fields)

    result = RunReportResult(
        ok=ok,
        err_code=err_code,
        err_message=err_message,
        user_message=user_message,
        rc=rc,
        stdout_path=bundle_dir / "stdout.log" if bundle_path else None,
        stderr_path=bundle_dir / "stderr.log" if bundle_path else None,
        xlsx_path=out_path if out_path.exists() else None,
        csv_path=csv_path if csv_path and csv_path.exists() else None,
        bundle_path=bundle_path,
        duration_ms=duration_ms,
        started_at=started_iso,
        finished_at=finished_iso,
        meta=meta,
    )

    level = "INFO" if ok else ("WARN" if err_code == "E_TIMEOUT" else "ERROR")
    event_name = "parser_finished" if ok else "parser_failed"
    log_event(
        event_name,
        level=level,
        duration_ms=duration_ms,
        rc=rc,
        err_code=err_code,
        err_message=err_message,
        bundle_path=str(bundle_path) if bundle_path else None,
        **common_log_fields,
    )

    _register_parse_result(result)
    return result