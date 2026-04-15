import sqlite3
c = sqlite3.connect("data/parser_cian.db")
count = c.execute("SELECT COUNT(*) FROM cian_sold_ads").fetchone()[0]
c.execute("DELETE FROM cian_sold_ads")
c.commit()
remaining = c.execute("SELECT COUNT(*) FROM cian_sold_ads").fetchone()[0]
print(f"Deleted {count} rows from cian_sold_ads. Remaining: {remaining}")
c.close()
