from __future__ import annotations
import os
import asyncio

# Глобальный семафор для ограничения параллельных задач бота
BOT_CONCURRENCY = int(os.getenv("BOT_CONCURRENCY", "2"))
SEM = asyncio.Semaphore(BOT_CONCURRENCY)