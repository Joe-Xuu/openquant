import psycopg2

c = psycopg2.connect(host='localhost', port=5432, user='openquant', password='openquant', dbname='openquant')
cur = c.cursor()

# Show rows 61, 105, 108, 110, 111, 114, 115, 116, 117 in detail
target_ids = [61, 105, 108, 110, 111, 114, 115, 116, 117]
for fid in target_ids:
    cur.execute("""
        SELECT fill_id, trade_id, symbol, buy_time, buy_price, buy_qty,
               sell_price, sell_placed, sell_filled, sell_order_id,
               sell_client_order_id, profit_pct, skip, updated_at
        FROM grid_fills WHERE fill_id = %s
    """, (fid,))
    r = cur.fetchone()
    if r:
        print(f"fill_id={r[0]} | trade_id={r[1]} | buy={r[5]} @ {r[4]}")
        print(f"  sell_placed={r[7]} | sell_filled={r[8]} | skip={r[12]}")
        print(f"  sell_order_id={r[9]} | sell_client_order_id={r[10]}")
        print(f"  updated_at={r[13]}")
        print()

c.close()
