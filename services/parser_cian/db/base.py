"""
services.parser_cian.db.base - SQLAlchemy models and async engine (PostgreSQL)
"""

import logging

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    JSON,
    TIMESTAMP,
    ForeignKey,
    func,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship

logger = logging.getLogger(__name__)

Base = declarative_base()


class CianFilter(Base):
    __tablename__ = "cian_filters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    meta = Column(JSON, nullable=True)

    active_ads = relationship("CianActiveAd", back_populates="filter")


class CianActiveAd(Base):
    __tablename__ = "cian_active_ads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    filter_id = Column(Integer, ForeignKey("cian_filters.id"), nullable=True)
    source = Column(String, nullable=False, default="offers")
    parsed_data = Column(JSON, nullable=True)
    is_parsed = Column(Boolean, default=False)
    last_updated = Column(
        TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )
    added_at = Column(TIMESTAMP, server_default=func.current_timestamp())

    filter = relationship("CianFilter", back_populates="active_ads")


class CianSoldAd(Base):
    __tablename__ = "cian_sold_ads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, unique=True, nullable=False)
    parsed_data = Column(JSON, nullable=True)
    publish_date = Column(String, nullable=True)
    sold_at = Column(TIMESTAMP, server_default=func.current_timestamp())


engine = None
AsyncSessionLocal = None

DEFAULT_DATABASE_URL = (
    "postgresql+asyncpg://flipper:flipper_secret@app_postgres:5432/flipper"
)


def init_engine(database_url: str = DEFAULT_DATABASE_URL):
    global engine, AsyncSessionLocal

    engine = create_async_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )

    AsyncSessionLocal = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )


async def init_db(database_url: str = DEFAULT_DATABASE_URL):
    """Creates tables if they don't exist."""
    init_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured (PostgreSQL)")
