import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    TELEGRAM_BOT_TOKEN: str
    MODE: str
    WEBAPP_HOST: str
    WEBAPP_PORT: int
    WEBAPP_PORT_SOURCE: str
    WEBHOOK_URL: str
    DB_PATH: Path
    REPORT_DIR: Path
    BUILD_VERSION: str
    PARSER_USER_AGENT: str | None = None
    PARSER_HH_BASE: str | None = None
    PARSER_GORODRABOT_BASE: str | None = None
    HTTP_PROXY: str | None = None
    REQUEST_TIMEOUT: int | None = None
    REF_ENABLED: bool = True
    REF_BONUS_INVITEE: int = 0
    REF_BONUS_INVITER: int = 0
    REF_ATTRIBUTION_TTL_HOURS: int = 48
    REF_MAX_BONUS_PER_DAY: int = 5
    REF_MAX_BONUS_TOTAL: int = 100
    REF_PROMO_TTL_HOURS: int = 48


def _load() -> Settings:
    def _bool(value: str | None, default: bool = False) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    port_source = "default"
    port_raw = os.getenv("WEBAPP_PORT")
    if port_raw:
        port_source = "env:WEBAPP_PORT"
    else:
        port_raw = os.getenv("PORT")
        if port_raw:
            port_source = "env:PORT"
        else:
            port_raw = "8090"

    try:
        port_value = int(str(port_raw).strip())
    except (TypeError, ValueError):
        port_source = f"{port_source}:invalid"
        port_value = 8090

    db_path = Path(os.getenv("DB_PATH", "var/db/bot.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = Settings(
        TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        MODE=os.getenv("MODE", "polling"),
        WEBAPP_HOST="0.0.0.0",
        WEBAPP_PORT=port_value,
        WEBAPP_PORT_SOURCE=port_source,
        WEBHOOK_URL=os.getenv("WEBHOOK_URL", ""),
        DB_PATH=db_path,
        REPORT_DIR=Path(os.getenv("REPORT_DIR", "./reports")),
        BUILD_VERSION=os.getenv("BUILD_VERSION")
        or os.getenv("REPLIT_RELEASE", "dev"),
        PARSER_USER_AGENT=os.getenv("PARSER_USER_AGENT"),
        PARSER_HH_BASE=os.getenv("PARSER_HH_BASE"),
        PARSER_GORODRABOT_BASE=os.getenv("PARSER_GORODRABOT_BASE"),
        HTTP_PROXY=os.getenv("HTTP_PROXY"),
        REQUEST_TIMEOUT=int(os.getenv("REQUEST_TIMEOUT", "20")),
        REF_ENABLED=_bool(os.getenv("REF_ENABLED"), True),
        REF_BONUS_INVITEE=int(os.getenv("REF_BONUS_INVITEE", "1")),
        REF_BONUS_INVITER=int(os.getenv("REF_BONUS_INVITER", "1")),
        REF_ATTRIBUTION_TTL_HOURS=int(os.getenv("REF_ATTRIBUTION_TTL_HOURS", "48")),
        REF_MAX_BONUS_PER_DAY=int(os.getenv("REF_MAX_BONUS_PER_DAY", "5")),
        REF_MAX_BONUS_TOTAL=int(os.getenv("REF_MAX_BONUS_TOTAL", "100")),
        REF_PROMO_TTL_HOURS=int(os.getenv("REF_PROMO_TTL_HOURS", "48")),
    )
    if cfg.MODE not in {"polling", "webhook"}:
        raise ValueError("MODE must be 'polling' or 'webhook'")
    cfg.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return cfg


settings = _load()
