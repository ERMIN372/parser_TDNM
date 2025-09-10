#!/usr/bin/env python3
import sys, subprocess
from pathlib import Path

def parse_args(argv):
    """Вытащить --output и сохранить остальные флаги как есть."""
    if "--output" not in argv:
        print("Usage: run_pipeline.py --query ... --area ... [--city CITY] --pages ... --per_page ... --output OUT.xlsx")
        sys.exit(1)
    args = argv[:]  # копия
    out_idx = args.index("--output")
    out_val = args[out_idx + 1]
    # удаляем --output из списка для передачи в fetch_vacancies.py
    args_wo_output = args[:out_idx] + args[out_idx + 2:]
    # для отчёта: попробуем вытащить --query / --city, если есть
    def get_opt(name, default=""):
        if name in args_wo_output:
            i = args_wo_output.index(name)
            return args_wo_output[i + 1] if i + 1 < len(args_wo_output) else default
        return default
    q = get_opt("--query", "")
    city = get_opt("--city", "")
    return out_val, args_wo_output, q, city

def main():
    # argv: ['run_pipeline.py', ...]
    out, args_wo_output, query_text, city_text = parse_args(sys.argv[1:])

    tmp_csv = Path("parsers/raw.csv")
    tmp_csv.parent.mkdir(parents=True, exist_ok=True)

    # Шаг 1: сбор в CSV из всех источников
    subprocess.run(
        [sys.executable, "parsers/fetch_vacancies.py", "--out_csv", str(tmp_csv)] + args_wo_output,
        check=True
    )
    print(f"OK: {tmp_csv}")

    # Шаг 2: построить Excel по шаблону
    subprocess.run(
        [sys.executable, "build_job_analytics.py", "--input", str(tmp_csv), "--output", out],
        check=True
    )
    print(f"OK: {out}")

    # Шаг 3: DOCX-отчёт
    report_dir = Path(r"C:\Users\Merkulov.I\Documents\Парсер вакансий\Reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_docx = report_dir / ("Отчёт_" + Path(out).stem + ".docx")
    subprocess.run(
        [
            sys.executable, "build_report_docx.py",
            "--input_csv", str(tmp_csv),
            "--output_docx", report_docx,
            "--query", query_text,
            "--city", city_text,
        ],
        check=True
    )
    print(f"OK: {report_docx}")

if __name__ == "__main__":
    main()
