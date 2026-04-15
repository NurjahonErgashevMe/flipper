import sqlite3
c = sqlite3.connect("data/parser_cian.db")
print("active_avans:", c.execute("SELECT COUNT(*) FROM cian_active_ads WHERE source='avans'").fetchone()[0])
print("active_offers:", c.execute("SELECT COUNT(*) FROM cian_active_ads WHERE source='offers'").fetchone()[0])
print("sold_total:", c.execute("SELECT COUNT(*) FROM cian_sold_ads").fetchone()[0])
c.close()
