import asyncio
import logging
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from shutil import copy2

from ..config import settings

logger = logging.getLogger(__name__)
VENDOR_DIR = Path(__file__).resolve().parents[2] / "vendor" / "parser_tdnm"


async def run_report(user_id: int, query: str, city: str) -> Path:
    """Запуск внешнего парсера и возврат пути к DOCX отчёту."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    user_dir = settings.REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    out_xlsx = user_dir / f"data_{ts}.xlsx"
    dest_docx = user_dir / f"report_{ts}.docx"

    cmd = [
        sys.executable,
        str(VENDOR_DIR / "run_pipeline.py"),
        "--query", query,
        "--city", city,
        "--role", query,  # run_pipeline -> fetch_vacancies: --role
        "--output", str(out_xlsx),
    ]
    logger.info("Running parser: %s", " ".join(shlex.quote(c) for c in cmd))

    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            cmd,
            cwd=VENDOR_DIR,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except subprocess.TimeoutExpired as e:
        logger.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера") from e

    logger.info("Parser finished with code %s", proc.returncode)
    if proc.returncode != 0:
        logger.error("stderr: %s", proc.stderr)
        raise RuntimeError(f"Парсер завершился с ошибкой {proc.returncode}")

    report_src_dir = VENDOR_DIR / Path("C:/Users/Merkulov.I/Documents/Парсер вакансий/Reports")
    report_src = report_src_dir / f"Отчёт_{out_xlsx.stem}.docx"
    if not report_src.exists():
        logger.error("Report not found: %s", report_src)
        raise FileNotFoundError("Отчёт не найден")

    copy2(report_src, dest_docx)
    logger.info("Report ready: %s", dest_docx)
    return dest_docx
