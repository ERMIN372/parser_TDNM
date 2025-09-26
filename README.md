# Job Parser Bot

## Setup
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# вставь токен
```

### Polling
```
python -m app.run
```

### Webhook
```
MODE=webhook python -m app.run
```

Отчёты сохраняются в каталоге `REPORT_DIR` (по умолчанию `./reports`).

Команда `/parse` принимает короткую форму `/parse кассир; Москва` или запускает диалог.

### Данные

SQLite-база бота по умолчанию хранится в каталоге `var/db/bot.db`, который не отслеживается Git.
Если раньше база лежала в `data/bot.db`, при запуске она будет автоматически
перемещена в новое место. При необходимости путь можно переопределить переменной
окружения `DB_PATH`.
