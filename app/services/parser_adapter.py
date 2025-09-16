import asyncio, os, shlex, subprocess, time, pathlib, logging
from datetime import datetime

log = logging.getLogger(__name__)
REPORT_DIR = pathlib.Path(os.getenv("REPORT_DIR", "reports"))
TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "420"))   # было 180
DEFAULT_PAGES = int(os.getenv("PARSER_PAGES", "1")) # урезаем до 1 страницы
PER_PAGE = int(os.getenv("PARSER_PER_PAGE", "20"))  # можно 20–50

async def run_report(user_id: int, query: str, city: str, role: str | None = None):
    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    # ожидаемый выход: позволяем и .docx и .xlsx
    target = user_dir / f"data_{ts}.xlsx"

    cmd = [
        os.environ.get("PYTHON_BIN", "python3"),
        str(pathlib.Path("vendor/parser_tdnm/run_pipeline.py").resolve()),
        "--query", query,
        "--city", city,
        "--pages", str(DEFAULT_PAGES),
        "--per_page", str(PER_PAGE),
        "--output", str(target),
    ]
    if role:
        cmd += ["--role", role]

    t0 = time.time()
    log.info("Running parser: %s", " ".join(shlex.quote(c) for c in cmd))
    try:
        # stdout/stderr в логи на случай фейла
        def _run():
            return subprocess.run(
                cmd, capture_output=True, text=True, timeout=TIMEOUT, check=False
            )
        proc = await asyncio.to_thread(_run)
    except subprocess.TimeoutExpired:
        log.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера")

    dt = time.time() - t0
    if proc.returncode != 0:
        log.error("Parser failed rc=%s\nSTDOUT:\n%s\nSTDERR:\n%s",
                  proc.returncode, proc.stdout, proc.stderr)
        raise RuntimeError("Парсер завершился с ошибкой")

    # Если файл не там/не тем расширением — ищем самый свежий артефакт в папке
    if not target.exists():
        cand = max(
            [p for p in user_dir.glob("**/*") if p.suffix.lower() in (".xlsx", ".csv", ".docx")],
            key=lambda p: p.stat().st_mtime, default=None
        )
        if not cand:
            raise RuntimeError("Не найден выходной файл парсера")
        target = cand

    log.info("Parser ok in %.1fs -> %s", dt, target)
    return target
