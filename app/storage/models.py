from __future__ import annotations
from datetime import datetime
from peewee import (
    Model,
    AutoField,
    BigIntegerField,
    BooleanField,
    CharField,
    DateTimeField,
    ForeignKeyField,
    IntegerField,
    TextField,
)
from .db import db

class BaseModel(Model):
    class Meta:
        database = db

class User(BaseModel):
    user_id = BigIntegerField(primary_key=True)
    username = CharField(null=True)
    full_name = CharField(null=True)
    plan = CharField(default="free")           # free | unlimited
    plan_until = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.utcnow)
    last_seen = DateTimeField(default=datetime.utcnow)

class Usage(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="usages", on_delete="CASCADE")
    ts = DateTimeField(default=datetime.utcnow)
    month_key = CharField(index=True)          # YYYY-MM
    kind = CharField()                         # free | paid

class Credit(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="credit", on_delete="CASCADE", unique=True)
    balance = IntegerField(default=0)

class Payment(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="payments", on_delete="CASCADE")
    pack = CharField()                         # 1|3|9|unlimited
    amount = IntegerField()                    # копейки
    currency = CharField(default="RUB")
    status = CharField(default="pending")      # pending|paid|failed
    provider_payload = TextField(null=True)
    created_at = DateTimeField(default=datetime.utcnow)
    paid_at = DateTimeField(null=True)


class ReferralStats(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="referral_stats", unique=True, on_delete="CASCADE")
    token = CharField(unique=True)
    invited_count = IntegerField(default=0)
    activated_count = IntegerField(default=0)
    bonuses_earned = IntegerField(default=0)
    last_invited_at = DateTimeField(null=True)
    last_bonus_at = DateTimeField(null=True)


class Referral(BaseModel):
    id = AutoField()
    inviter = ForeignKeyField(User, backref="referrals_sent", on_delete="CASCADE")
    invitee = ForeignKeyField(User, backref="referral_source", unique=True, on_delete="CASCADE")
    created_at = DateTimeField(default=datetime.utcnow)
    expires_at = DateTimeField(null=True)
    activated_at = DateTimeField(null=True)
    status = CharField(default="pending")  # pending|activated|rejected
    source = CharField(default="deep_link")
    token = CharField(null=True)
    rejection_reason = CharField(null=True)


class PromoCode(BaseModel):
    id = AutoField()
    code = CharField(unique=True)
    inviter = ForeignKeyField(User, backref="promo_codes", null=True, on_delete="SET NULL")
    is_active = BooleanField(default=True)
    expires_at = DateTimeField(null=True)
    max_uses = IntegerField(null=True)
    uses = IntegerField(default=0)


class Ledger(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="ledger", on_delete="CASCADE")
    kind = CharField()  # credit|unlimited
    delta = IntegerField()
    reason = CharField()
    related_referral = ForeignKeyField("Referral", null=True, on_delete="SET NULL")
    ts = DateTimeField(default=datetime.utcnow)
    balance_after = IntegerField(null=True)


class ReferralBan(BaseModel):
    id = AutoField()
    user = ForeignKeyField(User, backref="referral_ban", unique=True, on_delete="CASCADE")
    reason = CharField(null=True)
    created_at = DateTimeField(default=datetime.utcnow)
