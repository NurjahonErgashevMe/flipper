"""Clear cian_sold_ads table in PostgreSQL.

Usage: python scripts/reset_sold.py
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
        count = (await session.execute(text("SELECT COUNT(*) FROM cian_sold_ads"))).scalar()
        await session.execute(text("DELETE FROM cian_sold_ads"))
        await session.commit()
        remaining = (await session.execute(text("SELECT COUNT(*) FROM cian_sold_ads"))).scalar()
        print(f"Deleted {count} rows from cian_sold_ads. Remaining: {remaining}")


if __name__ == "__main__":
    asyncio.run(main())
