import asyncio
from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services import parser_adapter


def test_preview_rows_converts_to_dict(monkeypatch):
    async def fake_preview_report(*args, diagnostic=None, **kwargs):
        if diagnostic is not None:
            diagnostic.update({"mode": "api", "stdout": [], "stderr": [], "returncode": 0, "attempt": None, "error": None})
        return [("Dev", "Acme", "https://hh.ru/vacancy/1")]

    monkeypatch.setattr(parser_adapter, "preview_report", fake_preview_report)

    result = asyncio.run(parser_adapter.preview_rows(123, "Dev", "Москва"))

    assert isinstance(result.rows, list)
    assert result.rows == [
        {
            "title": "Dev",
            "company": "Acme",
            "salary": "",
            "link": "https://hh.ru/vacancy/1",
        }
    ]
    assert result.bundle_path.is_dir()
    assert result.bundle_id


def test_preview_rows_handles_empty(monkeypatch):
    async def fake_preview_report(*args, diagnostic=None, **kwargs):
        if diagnostic is not None:
            diagnostic.update({"mode": "api", "stdout": [], "stderr": [], "returncode": 0, "attempt": None, "error": None})
        return None

    monkeypatch.setattr(parser_adapter, "preview_report", fake_preview_report)

    result = asyncio.run(parser_adapter.preview_rows(1, "", ""))

    assert result.rows == []
    assert result.bundle_path.is_dir()
