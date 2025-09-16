import asyncio, os, shlex, subprocess, time, pathlib, logging, json
from datetime import datetime
from typing import Any, Dict

log = logging.getLogger(__name__)
REPORT_DIR = pathlib.Path(os.getenv("REPORT_DIR", "reports"))
TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "420"))
DEFAULT_PAGES = int(os.getenv("PARSER_PAGES", "1"))
PER_PAGE = int(os.getenv("PARSER_PER_PAGE", "20"))
DEFAULT_SITE = os.getenv("PARSER_SITE", "hh")
DEFAULT_PAUSE = float(os.getenv("PARSER_PAUSE", "0.6"))


async def run_report(
    user_id: int,
    query: str,
    city: str,
    role: str | None = None,
    pages: int | None = None,
    per_page: int | None = None,
    site: str | None = None,
    pause: float | None = None,
) -> pathlib.Path:
    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    target = user_dir / f"data_{ts}.xlsx"

    pages = DEFAULT_PAGES if pages is None else pages
    per_page = PER_PAGE if per_page is None else per_page
    site = (site or DEFAULT_SITE).lower()
    pause = pause if pause is not None else DEFAULT_PAUSE

    cmd = [
        os.environ.get("PYTHON_BIN", "python3"),
        str(pathlib.Path("vendor/parser_tdnm/run_pipeline.py").resolve()),
        "--query", query,
        "--city", city,
        "--pages", str(pages),
        "--per-page", str(per_page),
        "--pause", f"{pause}",
        "--site", site,
        "--formats", "xlsx",
        "--output", str(target),
        "--output-dir", str(user_dir),
    ]
    if role:
        cmd += ["--role", role]

    t0 = time.time()
    log.info("Running parser: %s", " ".join(shlex.quote(c) for c in cmd))

    try:
        def _run() -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=TIMEOUT,
                check=False,
            )

        proc = await asyncio.to_thread(_run)
    except subprocess.TimeoutExpired:
        log.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера")

    dt = time.time() - t0
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    if proc.returncode != 0:
        log.error(
            "Parser failed rc=%s\nSTDOUT:\n%s\nSTDERR:\n%s",
            proc.returncode,
            stdout,
            stderr,
        )
        raise RuntimeError("Парсер завершился с ошибкой")

    reported_paths: list[pathlib.Path] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload: Dict[str, Any] = json.loads(line)
        except json.JSONDecodeError:
            continue
        path_val = payload.get("path")
        if not path_val:
            continue
        path_obj = pathlib.Path(path_val)
        if not path_obj.is_absolute():
            path_obj = (user_dir / path_obj).resolve()
        reported_paths.append(path_obj)
        if payload.get("status") == "report" and path_obj.suffix.lower() == ".xlsx":
            target = path_obj

    if not target.exists():
        candidates = [p for p in reported_paths if p.suffix.lower() in {".xlsx", ".csv", ".docx"}]
        if not candidates:
            candidates = [
                p
                for p in user_dir.glob("**/*")
                if p.suffix.lower() in {".xlsx", ".csv", ".docx"}
            ]
        cand = max(
            candidates,
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
            default=None,
        )
        if not cand:
            raise RuntimeError("Не найден выходной файл парсера")
        target = cand

    log.info("Parser ok in %.1fs -> %s", dt, target)
    if stderr.strip():
        log.debug("Parser stderr: %s", stderr.strip())
    return target
