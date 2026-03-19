import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import select, delete, insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import selectinload

from services.parser_cian.db.base import (
    AsyncSessionLocal, 
    CianFilter, 
    CianActiveAd, 
    CianSoldAd,
    init_db as _init_db
)

logger = logging.getLogger(__name__)

class DatabaseRepository:
    def __init__(self, db_path: str = "parser_cian.db"):
        self.db_path = db_path

    async def init_db(self):
        """Создает таблицы через SQLAlchemy."""
        await _init_db()
        logger.info("Database initialized successfully with SQLAlchemy.")

    # --- Filters ---
    async def add_filters(self, urls: List[str]):
        """Добавляет новые ссылки-фильтры."""
        async with AsyncSessionLocal() as session:
            for url in urls:
                stmt = sqlite_insert(CianFilter).values(url=url).on_conflict_do_nothing()
                await session.execute(stmt)
            await session.commit()

    async def get_all_filters(self) -> List[Dict]:
        """Возвращает все фильтры."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(CianFilter))
            rows = result.scalars().all()
            return [{"id": r.id, "url": r.url} for r in rows]

    # --- Active Ads ---
    async def add_ad_urls(self, urls: List[str], filter_id: Optional[int] = None):
        """Добавляет объявления в очередь."""
        async with AsyncSessionLocal() as session:
            for url in urls:
                stmt = sqlite_insert(CianActiveAd).values(
                    url=url, 
                    filter_id=filter_id
                ).on_conflict_do_nothing()
                await session.execute(stmt)
            await session.commit()

    async def get_all_active_ads(self) -> List[str]:
        """Получает список всех URL активных объявлений."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(CianActiveAd.url))
            return result.scalars().all()

    async def update_active_ad(self, url: str, parsed_data: Dict[str, Any]):
        """Обновляет данные активного объявления."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(CianActiveAd).where(CianActiveAd.url == url)
            )
            ad = result.scalar_one_or_none()
            if ad:
                ad.parsed_data = parsed_data
                ad.is_parsed = True
                await session.commit()

    async def move_to_sold(self, url: str, parsed_data: Dict[str, Any], publish_date: str):
        """Перемещает объявление в проданные."""
        async with AsyncSessionLocal() as session:
            # Удаление из активных
            await session.execute(
                delete(CianActiveAd).where(CianActiveAd.url == url)
            )
            
            # Добавление в проданные
            stmt = sqlite_insert(CianSoldAd).values(
                url=url,
                parsed_data=parsed_data,
                publish_date=publish_date
            ).on_conflict_do_nothing()
            
            await session.execute(stmt)
            await session.commit()
