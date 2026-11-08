import psycopg2

c = psycopg2.connect(host='localhost', port=5432, user='openquant', password='openquant', dbname='openquant')
cur = c.cursor()
cur.execute("""
    SELECT fill_id, trade_id, symbol, buy_time, buy_price, buy_qty,
           sell_price, sell_placed, sell_filled, sell_order_id, profit_pct, skip
    FROM grid_fills ORDER BY fill_id
""")
rows = cur.fetchall()
print(f"{'fill_id':<8} {'trade_id':<25} {'symbol':<8} {'buy_time':<22} {'buy_price':<12} {'buy_qty':<10} {'sell_price':<12} {'placed':<7} {'filled':<7} {'profit%':<8} {'skip':<6} {'sell_order_id'}")
print("-" * 170)
for r in rows:
    fid, tid, sym, bt, bp, bq, sp, spl, sfl, pp, sk, soid = r
    bt_str = bt.strftime('%Y-%m-%d %H:%M') if bt else ''
    bp_s = f"{float(bp):<12.6f}" if bp is not None else "N/A         "
    bq_s = f"{float(bq):<10.4f}" if bq is not None else "N/A       "
    sp_s = f"{float(sp):<12.6f}" if sp is not None else "N/A         "
    pp_s = f"{float(pp):<8.2f}" if pp is not None else "N/A     "
    print(f"{fid:<8} {tid:<25} {sym:<8} {bt_str:<22} {bp_s} {bq_s} {sp_s} {str(spl):<7} {str(sfl):<7} {pp_s} {str(sk):<6} {soid or ''}")

# Also check total count and date range
cur.execute("SELECT COUNT(*), MIN(buy_time), MAX(buy_time) FROM grid_fills")
cnt, dmin, dmax = cur.fetchone()
print(f"\nTotal rows: {cnt}, Date range: {dmin} to {dmax}")

# Check rows after July 24
cur.execute("SELECT COUNT(*) FROM grid_fills WHERE buy_time >= '2026-07-24'")
print(f"Rows since Jul 24: {cur.fetchone()[0]}")

c.close()
