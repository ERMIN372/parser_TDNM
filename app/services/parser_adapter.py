# app/services/parser_adapter.py
from __future__ import annotations
import os
import sys
import asyncio
import subprocess
import logging
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

# Конфиг
PYBIN = os.getenv("PYBIN", sys.executable or "python3")
PIPELINE = os.getenv("PARSER_PIPELINE", "vendor/parser_tdnm/run_pipeline.py")
REPORT_DIR = Path(os.getenv("REPORT_DIR", "reports"))
DEFAULT_TIMEOUT = int(os.getenv("PARSER_TIMEOUT", "180"))  # сек

REPORT_DIR.mkdir(parents=True, exist_ok=True)


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
    area: int | None = None,            # <-- НОВОЕ
    timeout: int = DEFAULT_TIMEOUT,
) -> Path:
    """
    Запускает vendor/parser_tdnm/run_pipeline.py и возвращает путь к XLSX.
    Все опции необязательны; передаются дальше в CLI только если заданы.
    """

    # имя файла
    user_dir = REPORT_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = user_dir / f"data_{ts}.xlsx"

    # собираем команду
    cmd = [
        PYBIN, PIPELINE,
        "--query", query,
        "--city", city,
        "--formats", "xlsx",
        "--output", str(out_path),
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
    if area is not None:                 # <-- НОВОЕ
        cmd += ["--area", str(area)]

    log.info("Running parser: %s", " ".join(map(str, cmd)))

    # запуск в отдельном потоке, чтобы не блокировать event loop
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        log.error("Parser timeout")
        raise RuntimeError("Превышено время ожидания парсера") from e

    if proc.returncode != 0:
        log.error("Parser failed (rc=%s)\nstdout:\n%s\nstderr:\n%s", proc.returncode, proc.stdout, proc.stderr)
        raise RuntimeError(f"Не удалось получить отчёт: парсер завершился с ошибкой {proc.returncode}")

    return out_path
