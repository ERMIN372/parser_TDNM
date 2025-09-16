from __future__ import annotations
from datetime import datetime
from peewee import (
    Model, AutoField, BigIntegerField, CharField, DateTimeField,
    ForeignKeyField, IntegerField, TextField
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
