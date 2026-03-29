"""
services.parser_cian.db.base - SQLAlchemy models and engine
"""

import json
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    Text,
    DateTime,
    ForeignKey,
    JSON,
    TIMESTAMP,
    func,
    text,
)
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class CianFilter(Base):
    __tablename__ = "cian_filters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    
    active_ads = relationship("CianActiveAd", back_populates="filter")

class CianActiveAd(Base):
    __tablename__ = "cian_active_ads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    filter_id = Column(Integer, ForeignKey("cian_filters.id"), nullable=True)
    source = Column(String, nullable=False, default="regular")
    parsed_data = Column(JSON, nullable=True)
    is_parsed = Column(Boolean, default=False)
    last_updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    added_at = Column(TIMESTAMP, server_default=func.current_timestamp())

    filter = relationship("CianFilter", back_populates="active_ads")

class CianSoldAd(Base):
    __tablename__ = "cian_sold_ads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    parsed_data = Column(JSON, nullable=True)
    publish_date = Column(String, nullable=True)
    sold_at = Column(TIMESTAMP, server_default=func.current_timestamp())

# Движок инициализируется динамически через init_engine()
engine = None
AsyncSessionLocal = None


def _migrate_cian_active_ads_source_column(sync_conn) -> None:
    """
    Старые БД созданы до поля source — create_all не добавляет колонки.
    Добавляем source TEXT NOT NULL DEFAULT 'regular' при отсутствии.
    """
    r = sync_conn.execute(text("PRAGMA table_info(cian_active_ads)"))
    names = {row[1] for row in r.fetchall()}
    if names and "source" not in names:
        sync_conn.execute(
            text(
                "ALTER TABLE cian_active_ads ADD COLUMN source TEXT NOT NULL DEFAULT 'regular'"
            )
        )


def init_engine(db_path: str = "data/parser_cian.db"):
    """Инициализирует SQLAlchemy engine с заданным путём к БД."""
    global engine, AsyncSessionLocal
    database_url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(
        database_url,
        echo=False,
        connect_args={"timeout": 30.0},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_connection, connection_record):
        cur = dbapi_connection.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=10000")
        cur.close()

    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(db_path: str = "data/parser_cian.db"):
    """Создаёт таблицы. Принимает путь к файлу БД."""
    init_engine(db_path)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_migrate_cian_active_ads_source_column)
