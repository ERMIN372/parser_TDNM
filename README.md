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

### Отладка и диагностика

Переменные окружения:

* `LOG_LEVEL` — уровень логирования (INFO по умолчанию).
* `MODE` — `polling` или `webhook`.
* `WEBHOOK_URL` — полный HTTPS-адрес вебхука, должен заканчиваться на `/webhook`.
* `PORT` — порт, который слушает FastAPI (обязателен на Replit).
* `ADMIN_USER_IDS` — список id администраторов через запятую.
* `GIT_REV` — ревизия сборки (по умолчанию `local`).

Как проверить готовность:

```bash
curl https://<app>.replit.app/health
curl https://<app>.replit.app/version
curl https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getWebhookInfo
```

Админ-команды в Telegram (`ADMIN_USER_IDS`): `/ping`, `/whoami`, `/rt`.

Пример логов при входящем сообщении:

```
2024-01-01 12:00:00,000 | INFO | updates | <= [1a2b3c4d] message from 123456789 @user: /start
2024-01-01 12:00:00,120 | INFO | updates | => [1a2b3c4d] handled in 120.5 ms
2024-01-01 12:00:00,121 | INFO | http | 10.0.0.1 "POST /webhook" 200 121.0ms
```
