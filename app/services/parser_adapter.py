from __future__ import annotations
import asyncio, os, shlex, subprocess, time, logging
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

REPORT_DIR = Path(os.getenv("REPORT_DIR", "reports"))
TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "420"))
PAGES = int(os.getenv("PARSER_PAGES", "1"))
PER_PAGE = int(os.getenv("PARSER_PER_PAGE", "20"))
PAUSE = os.getenv("PARSER_PAUSE", None)
SITE = os.getenv("PARSER_SITE", "hh")
OUT_EXT = os.getenv("PARSER_OUTPUT_EXT", "xlsx")
PYBIN = os.getenv("PYTHON_BIN", "python3")
AREA_ENV = os.getenv("PARSER_AREA", "").strip()

CITY2AREA = {
    "москва": "1",
    "moscow": "1",
    "санкт-петербург": "2",
    "спб": "2",
    "saint petersburg": "2",
}

def _area_from(city: str) -> str | None:
    if AREA_ENV:
        return AREA_ENV
    return CITY2AREA.get(city.strip().lower())

async def run_report(user_id: int, query: str, city: str, role: str | None = None) -> Path:
    out_dir = (REPORT_DIR / str(user_id)).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = (out_dir / f"data_{ts}.{OUT_EXT}").resolve()

    # Запускаем из каталога парсера — чтобы его относительные пути не ломались
    cwd = (Path("vendor") / "parser_tdnm").resolve()

    cmd = [
        PYBIN, "run_pipeline.py",
        "--query", query,
        "--city", city,
        "--pages", str(PAGES),
        "--per-page", str(PER_PAGE),    # в новом CLI есть и --per-page, и alias --per_page
        "--site", SITE,                 # hh|gorodrabot|both
        "--formats", OUT_EXT,           # xlsx/csv/docx
        "--output", str(out_path),
        "--output-dir", str(out_dir),
        "--keep-csv",
    ]
    if PAUSE:
        cmd += ["--pause", str(PAUSE)]
    area = _area_from(city)
    if area:
        cmd += ["--area", area]
    if role:
        cmd += ["--role", role]

    log.info("Running parser (cwd=%s): %s", cwd, " ".join(shlex.quote(str(c)) for c in cmd))

    t0 = time.time()
    def _run():
        return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, timeout=TIMEOUT)

    try:
        proc = await asyncio.to_thread(_run)
    except subprocess.TimeoutExpired:
        log.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера")

    dt = time.time() - t0
    if proc.returncode != 0:
        stdout_tail = "\n".join(proc.stdout.splitlines()[-120:])
        stderr_tail = "\n".join(proc.stderr.splitlines()[-120:])
        log.error("Parser failed rc=%s\nSTDOUT tail:\n%s\nSTDERR tail:\n%s",
                  proc.returncode, stdout_tail, stderr_tail)
        raise RuntimeError("Парсер завершился с ошибкой 1")

    # Ищем артефакт ТОЛЬКО в каталоге пользователя
    def pick_output() -> Path | None:
        for p in [out_path] + sorted(out_dir.rglob("*"), key=lambda p: p.stat().st_mtime, reverse=True):
            if p.is_file() and p.suffix.lower() in (".xlsx", ".csv", ".docx"):
                return p
        return None

    target = pick_output()
    if not target:
        log.error("No output in %s", out_dir)
        raise RuntimeError("Отчёт не найден")

    log.info("Parser ok in %.1fs -> %s", dt, target)
    return target
