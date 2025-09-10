import subprocess, sys
from pathlib import Path

QUERY = "Бариста"
AREA = "1"           # 1=Москва
PAGES = "3"
PER_PAGE = "55"
CITY = "Москва"      # для gorodrabot.ru

OUTPUT = r"C:\Users\Merkulov.I\Documents\Парсер вакансий\Exports\Оператор Доставки.xlsx"
out_path = Path(OUTPUT)
out_path.parent.mkdir(parents=True, exist_ok=True)

subprocess.run([
    sys.executable, "parsers/run_pipeline.py",
    "--query", QUERY, "--area", AREA,
    "--pages", PAGES, "--per_page", PER_PAGE,
    "--city", CITY,                    # ← добавили
    "--output", str(out_path)
], check=True)

print("Готово:", out_path)
