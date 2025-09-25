import asyncio
from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services import parser_adapter


def test_preview_rows_converts_to_dict(monkeypatch):
    async def fake_preview_report(*args, **kwargs):
        return [("Dev", "Acme", "https://hh.ru/vacancy/1")]

    monkeypatch.setattr(parser_adapter, "preview_report", fake_preview_report)

    rows = asyncio.run(parser_adapter.preview_rows(123, "Dev", "Москва"))

    assert isinstance(rows, list)
    assert rows == [
        {
            "title": "Dev",
            "company": "Acme",
            "salary": "",
            "link": "https://hh.ru/vacancy/1",
        }
    ]


def test_normalize_overrides_ok():
    ok, normalized, invalid, error = parser_adapter.normalize_and_validate_overrides(
        {"pages": "2", "per_page": 5, "include": ["python", ""], "pause": "0.5"}
    )

    assert ok is True
    assert invalid == []
    assert error is None
    assert normalized["pages"] == 2
    assert normalized["per_page"] == 5
    assert normalized["pause"] == 0.5
    assert normalized["include"] == ["python"]


def test_normalize_overrides_invalid_site():
    ok, normalized, invalid, error = parser_adapter.normalize_and_validate_overrides({"site": "xxx"})

    assert ok is False
    assert normalized["include"] == []
    assert invalid == ["site"]
    assert error and "site" in error


def test_preview_rows_handles_empty():
    with pytest.raises(parser_adapter.ValidationError):
        asyncio.run(parser_adapter.preview_rows(1, "", ""))


def test_preview_rows_invalid_overrides():
    with pytest.raises(parser_adapter.ValidationError):
        asyncio.run(
            parser_adapter.preview_rows(
                1,
                "Dev",
                "Москва",
                include=["python"],
                exclude=None,
                area="abc",  # type: ignore[arg-type]
            )
        )
