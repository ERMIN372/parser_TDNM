#!/usr/bin/env python3
"""Обновлённый конвейер парсера вакансий TDNM.

CLI стал богаче: поддерживает выбор площадок, форматов и автоматическое
формирование имени файла, если явный путь не задан.
"""
import argparse
import datetime as _dt
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

from .constants import DEFAULT_HH_SEARCH_FIELD

ROOT = Path(__file__).resolve().parent
PARSERS_DIR = ROOT / "parsers"
DEFAULT_OUTPUT_DIR = ROOT / "exports"

def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9\u0400-\u04FF]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "report"

def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TDNM vacancy parser pipeline")
    parser.add_argument("--query", required=True, help="Поисковая фраза")
    parser.add_argument("--area", type=int, default=1, help="Код региона HH")
    parser.add_argument("--city", default="Москва", help="Город для gorodrabot")
    parser.add_argument("--role", default=None, help="Фильтр из словаря FILTERS")
    parser.add_argument("--pages", type=int, default=1, help="Сколько страниц обходить")
    parser.add_argument("--per-page", dest="per_page", type=int, default=20,
                        help="Размер страницы для API HH")
    parser.add_argument("--per_page", dest="per_page", type=int, help="alias", metavar="N")
    parser.add_argument("--pause", type=float, default=0.6,
                        help="Пауза между запросами к HH")
    parser.add_argument("--site", choices=["hh", "gorodrabot", "both"], default="hh",
                        help="Ограничить источники вакансий")
    parser.add_argument("--search-in", dest="search_in", default=DEFAULT_HH_SEARCH_FIELD,
                        choices=["name", "description", "company_name", "everything"],
                        help="Поле поиска HH")
    parser.add_argument("--search_in", dest="search_in", help="alias")
    parser.add_argument("--output", type=Path, help="Путь к готовому XLSX")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help="Каталог для авто-генерации имени")
    parser.add_argument("--formats", nargs="+", choices=["csv", "xlsx", "docx"],
                        default=["xlsx"], help="Нужные форматы отчёта")
    parser.add_argument("--keep-csv", action="store_true",
                        help="Оставить промежуточный raw.csv в выходном каталоге")
    parser.add_argument("--name-suffix", default=None, help="Доп. постфикс к имени файла")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(list(argv))

def run_pipeline(args: argparse.Namespace) -> List[Path]:
    output_dir = Path(args.output_dir if args.output is None else args.output.parent)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_name = slugify(args.query)
    if args.name_suffix:
        base_name = f"{base_name}_{slugify(args.name_suffix)}"
    if args.output:
        base_name = Path(args.output).stem

    tmp_csv = PARSERS_DIR / "raw.csv"
    tmp_csv.parent.mkdir(parents=True, exist_ok=True)

    fetch_cmd = [
        sys.executable,
        str(PARSERS_DIR / "fetch_vacancies.py"),
        "--query", args.query,
        "--area", str(args.area),
        "--city", args.city,
        "--pages", str(args.pages),
        "--per_page", str(args.per_page),
        "--pause", str(args.pause),
        "--out_csv", str(tmp_csv),
        "--search_in", args.search_in,
        "--site", args.site,
    ]
    if args.role:
        fetch_cmd.extend(["--role", args.role])

    subprocess.run(fetch_cmd, check=True)
    print(json.dumps({"status": "csv", "path": str(tmp_csv)}))

    outputs: List[Path] = []
    if "csv" in args.formats or args.keep_csv:
        csv_name = f"{base_name}_{timestamp}.csv"
        csv_path = output_dir / csv_name
        shutil.copy2(tmp_csv, csv_path)
        outputs.append(csv_path)
        print(json.dumps({"status": "report", "format": "csv", "path": str(csv_path)}))

    if "xlsx" in args.formats:
        xlsx_path = Path(args.output) if args.output else output_dir / f"{base_name}_{timestamp}.xlsx"
        subprocess.run([
            sys.executable,
            str(ROOT / "build_job_analytics.py"),
            "--input", str(tmp_csv),
            "--output", str(xlsx_path),
        ], check=True)
        outputs.append(xlsx_path)
        print(json.dumps({"status": "report", "format": "xlsx", "path": str(xlsx_path)}))

    if "docx" in args.formats:
        docx_path = output_dir / f"{base_name}_{timestamp}.docx"
        subprocess.run([
            sys.executable,
            str(ROOT / "build_report_docx.py"),
            "--input_csv", str(tmp_csv),
            "--output_docx", str(docx_path),
            "--query", args.query,
            "--city", args.city,
        ], check=True)
        outputs.append(docx_path)
        print(json.dumps({"status": "report", "format": "docx", "path": str(docx_path)}))

    if not args.keep_csv and "csv" not in args.formats and tmp_csv.exists():
        try:
            tmp_csv.unlink()
        except OSError:
            pass

    return outputs

def main(argv: Iterable[str] | None = None) -> List[Path]:
    args = parse_args(argv or sys.argv[1:])
    outputs = run_pipeline(args)
    if args.verbose:
        print(json.dumps({"status": "done", "files": [str(p) for p in outputs]}))
    return outputs

if __name__ == "__main__":
    main()
