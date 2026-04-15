"""Quick stats from PostgreSQL database.

Usage (from host): docker compose exec app_postgres psql -U flipper -c "
  SELECT 'active_avans', COUNT(*) FROM cian_active_ads WHERE source='avans'
  UNION ALL SELECT 'active_offers', COUNT(*) FROM cian_active_ads WHERE source='offers'
  UNION ALL SELECT 'sold_total', COUNT(*) FROM cian_sold_ads;"

Or via Python:
  python scripts/db_check.py
"""

import asyncio
import sys
sys.path.insert(0, ".")

from services.parser_cian.config import settings
from services.parser_cian.db.base import init_engine, AsyncSessionLocal
from sqlalchemy import text


async def main():
    init_engine(settings.database_url)
    async with AsyncSessionLocal() as session:
        for label, query in [
            ("active_avans", "SELECT COUNT(*) FROM cian_active_ads WHERE source='avans'"),
            ("active_offers", "SELECT COUNT(*) FROM cian_active_ads WHERE source='offers'"),
            ("sold_total", "SELECT COUNT(*) FROM cian_sold_ads"),
        ]:
            result = await session.execute(text(query))
            print(f"{label}: {result.scalar()}")


if __name__ == "__main__":
    asyncio.run(main())
