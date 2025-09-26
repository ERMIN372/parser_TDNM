from __future__ import annotations

import importlib
from datetime import datetime, timedelta

import pytest

pytest.importorskip("peewee")


@pytest.fixture()
def promo_env(monkeypatch, tmp_path):
    db_path = tmp_path / "promo.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    from app.storage import db as db_module

    importlib.reload(db_module)
    from app.storage import models
    importlib.reload(models)
    from app.storage import repo
    importlib.reload(repo)
    from app.storage import referrals_repo
    importlib.reload(referrals_repo)
    from app.storage import promo_repo
    importlib.reload(promo_repo)
    from app.services import promo as promo_service
    importlib.reload(promo_service)

    db_module.init_db()

    yield {
        "db": db_module,
        "models": models,
        "repo": repo,
        "referrals_repo": referrals_repo,
        "promo_repo": promo_repo,
        "promo_service": promo_service,
    }

    db_module.db.close()


def test_redeem_promo_success(promo_env):
    promo_repo = promo_env["promo_repo"]
    promo_service = promo_env["promo_service"]
    now = datetime.utcnow()
    promo_repo.create_code(
        code="TESTCODE",
        normalized_code="TESTCODE",
        bonus_credits=5,
        title=None,
        starts_at=now - timedelta(days=1),
        expires_at=now + timedelta(days=1),
        max_redemptions=None,
        created_by=123,
        meta=None,
    )

    result = promo_service.redeem_promo(1, "testcode")
    assert result.ok
    assert "✅ Промокод TESTCODE активирован" in result.message
    assert result.new_balance == 5

    repeat = promo_service.redeem_promo(1, "TESTCODE")
    assert not repeat.ok
    assert "уже был активирован" in repeat.message


def test_redeem_promo_limit_and_inactive(promo_env):
    promo_repo = promo_env["promo_repo"]
    promo_service = promo_env["promo_service"]
    now = datetime.utcnow()
    promo_repo.create_code(
        code="LIMIT1",
        normalized_code="LIMIT1",
        bonus_credits=2,
        title=None,
        starts_at=now - timedelta(days=1),
        expires_at=now + timedelta(days=1),
        max_redemptions=1,
        created_by=123,
        meta=None,
    )

    assert promo_service.redeem_promo(10, "LIMIT1").ok
    limited = promo_service.redeem_promo(11, "LIMIT1")
    assert not limited.ok
    assert "Лимит" in limited.message

    promo_repo.set_active("LIMIT1", False)
    inactive = promo_service.redeem_promo(12, "LIMIT1")
    assert not inactive.ok
    assert "недействителен" in inactive.message


def test_redeem_promo_invalid_and_expired(promo_env):
    promo_repo = promo_env["promo_repo"]
    promo_service = promo_env["promo_service"]
    now = datetime.utcnow()
    promo_repo.create_code(
        code="EXPIRE",
        normalized_code="EXPIRE",
        bonus_credits=3,
        title=None,
        starts_at=now - timedelta(days=3),
        expires_at=now - timedelta(days=1),
        max_redemptions=None,
        created_by=321,
        meta=None,
    )

    invalid = promo_service.redeem_promo(50, "bad code")
    assert not invalid.ok
    assert "указан неверно" in invalid.message

    expired = promo_service.redeem_promo(51, "EXPIRE")
    assert not expired.ok
    assert "недействителен" in expired.message


def test_redeem_promo_preserves_existing_balance(promo_env):
    repo = promo_env["repo"]
    promo_repo = promo_env["promo_repo"]
    promo_service = promo_env["promo_service"]
    user_id = 123
    now = datetime.utcnow()
    promo_repo.create_code(
        code="STACKUP",
        normalized_code="STACKUP",
        bonus_credits=5,
        title=None,
        starts_at=now - timedelta(days=1),
        expires_at=now + timedelta(days=1),
        max_redemptions=None,
        created_by=999,
        meta=None,
    )

    repo.ensure_user(user_id, "tester", "Test User")
    repo.add_credits(user_id, 15)
    repo.set_unlimited(user_id, 30)

    result = promo_service.redeem_promo(user_id, "STACKUP")

    assert result.ok
    assert result.new_balance == 20
    assert repo.get_credits(user_id) == 20

    active, until = repo.is_unlimited_active(user_id)
    assert active
    assert until is not None
