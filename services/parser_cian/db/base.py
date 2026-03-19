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
    func
)
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

# Настройка движка
DATABASE_URL = "sqlite+aiosqlite:///parser_cian.db"
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
