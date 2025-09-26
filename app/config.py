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
    WEBHOOK_URL: str
    REPORT_DIR: Path
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

    cfg = Settings(
        TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        MODE=os.getenv("MODE", "polling"),
        WEBAPP_HOST=os.getenv("WEBAPP_HOST", "0.0.0.0"),
        WEBAPP_PORT=int(os.getenv("WEBAPP_PORT", "8080")),
        WEBHOOK_URL=os.getenv("WEBHOOK_URL", ""),
        REPORT_DIR=Path(os.getenv("REPORT_DIR", "./reports")),
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
