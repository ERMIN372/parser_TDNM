import subprocess, sys
from pathlib import Path

QUERY = "Бариста"
AREA = "1"           # 1=Москва
PAGES = "2"
PER_PAGE = "50"
CITY = "Москва"      # для gorodrabot.ru

OUTPUT = r"C:\Users\Merkulov.I\Documents\Парсер вакансий\Exports\Бариста.xlsx"
out_path = Path(OUTPUT)
out_path.parent.mkdir(parents=True, exist_ok=True)

subprocess.run([
    sys.executable, "parsers/run_pipeline.py",
    "--query", "Бариста",
    "--area", "1",
    "--city", "Москва",
    "--pages", "2",
    "--per_page", "50",
    "--role", "бариста",   # ← вот это
    "--output", str(out_path)
], check=True)


print("Готово:", out_path)
