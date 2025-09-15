import subprocess, sys
from pathlib import Path

QUERY = "Оператор доставки"
AREA = "1"           # 1=Москва
PAGES = "3"
PER_PAGE = "55"
CITY = "Москва"      # для gorodrabot.ru

OUTPUT = r"C:\Users\Merkulov.I\Documents\Парсер вакансий\Exports\Оператор доставки.xlsx"
out_path = Path(OUTPUT)
out_path.parent.mkdir(parents=True, exist_ok=True)

subprocess.run([
    sys.executable, "parsers/run_pipeline.py",
    "--query", "Оператор доставки",
    "--area", "1",
    "--city", "Москва",
    "--pages", "2",
    "--per_page", "100",
    "--role", "оператор_доставки",   # ← вот это
    "--output", str(out_path)
], check=True)


print("Готово:", out_path)
