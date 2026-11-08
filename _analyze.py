import psycopg2

c = psycopg2.connect(host='localhost', port=5432, user='openquant', password='openquant', dbname='openquant')
cur = c.cursor()

# 1. All rows where sell_filled=False (unsold positions)
print("=" * 80)
print("ALL UNSOLD GRID FILLS (sell_filled=False):")
print("=" * 80)
cur.execute("""
    SELECT fill_id, trade_id, symbol, buy_time, buy_price, buy_qty,
           sell_price, sell_placed, sell_filled, skip, sell_order_id,
           profit_pct
    FROM grid_fills WHERE sell_filled = FALSE AND skip = FALSE
    ORDER BY fill_id
""")
for r in cur.fetchall():
    fid, tid, sym, bt, bp, bq, sp, spl, sfl, sk, soid, pp = r
    bt_str = bt.strftime('%m-%d %H:%M') if bt else ''
    print(f"  fill_id={fid:<5} trade_id={tid:<15} {bt_str}  buy={float(bq):.0f}@{float(bp):.6f}  "
          f"sell_price={float(sp):.6f}  placed={spl}  skip={sk}")

# 2. Total unsold DOGE quantity
cur.execute("""
    SELECT SUM(buy_qty) FROM grid_fills WHERE sell_filled = FALSE AND skip = FALSE
""")
total_unsold = cur.fetchone()[0] or 0
print(f"\nTotal unsold DOGE quantity: {total_unsold}")

# 3. Check positions table
print("\n" + "=" * 80)
print("POSITIONS TABLE:")
print("=" * 80)
cur.execute("SELECT * FROM positions ORDER BY symbol")
for r in cur.fetchall():
    print(f"  {r}")

# 4. Check if there are any rows with skip=TRUE
print("\n" + "=" * 80)
print("ROWS WITH skip=TRUE:")
print("=" * 80)
cur.execute("SELECT fill_id, trade_id, buy_time, buy_qty, buy_price FROM grid_fills WHERE skip = TRUE ORDER BY fill_id")
skipped = cur.fetchall()
if skipped:
    for r in skipped:
        print(f"  fill_id={r[0]} trade_id={r[1]} buy_time={r[2]} qty={r[3]} price={r[4]}")
else:
    print("  (none)")

# 5. Gaps in dates — days with NO grid_fills at all since Jul 24
print("\n" + "=" * 80)
print("DAILY BUY COUNT SINCE JUL 24:")
print("=" * 80)
cur.execute("""
    SELECT DATE(buy_time), COUNT(*),
           SUM(CASE WHEN sell_filled THEN 1 ELSE 0 END) as completed,
           SUM(CASE WHEN NOT sell_filled THEN 1 ELSE 0 END) as pending
    FROM grid_fills WHERE buy_time >= '2026-07-24'
    GROUP BY DATE(buy_time) ORDER BY 1
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} buys, {r[2]} completed, {r[3]} pending")

c.close()
