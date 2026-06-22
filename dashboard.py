#!/usr/bin/env python3
"""
OpenQuant — 实时交易看板 / Live Trading Dashboard
启动后打开 http://localhost:8080 查看实时交易数据
"""

import json, os, sys, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.local_ledger import get_ledger

LEDGER_PATH = "data/trading_ledger.db"
REFRESH_SECONDS = 10

STYLE = """
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0d1117; color:#c9d1d9; font-family:-apple-system,BlinkMacSystemFont,sans-serif; padding:20px; }
h1 { color:#58a6ff; margin-bottom:20px; font-size:24px; }
h2 { color:#8b949e; margin:25px 0 10px; font-size:18px; border-bottom:1px solid #21262d; padding-bottom:8px; }
.card { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:16px; margin-bottom:16px; }
table { width:100%; border-collapse:collapse; font-size:14px; }
th { text-align:left; padding:8px 12px; border-bottom:2px solid #30363d; color:#8b949e; font-weight:600; }
td { padding:6px 12px; border-bottom:1px solid #21262d; }
.green { color:#3fb950; } .red { color:#f85149; } .yellow { color:#d2991d; }
.badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:12px; font-weight:600; }
.badge-buy { background:#1b3824; color:#3fb950; } .badge-sell { background:#3d1f1f; color:#f85149; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; }
.stat { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px; text-align:center; }
.stat-value { font-size:28px; font-weight:bold; } .stat-label { font-size:12px; color:#8b949e; margin-top:4px; }
.refresh { color:#8b949e; font-size:12px; text-align:center; margin-top:20px; }
"""

def build_page():
    ledger = get_ledger(LEDGER_PATH)
    conn = ledger._get_connection()

    equity = ledger.get_total_equity()
    balances = ledger.get_all_balances()
    stats = ledger.get_trade_statistics()
    positions = ledger.get_all_positions()

    cash = balances.get("CASH-USDT", 0)
    initial = balances.get("EQUITY-INITIAL", 10000)
    pnl = equity - initial
    pnl_color = "green" if pnl >= 0 else "red"
    total_trades = stats.get('total_trades', 0)
    win_rate = stats.get('win_rate', 0) * 100 if isinstance(stats.get('win_rate'), float) and stats.get('win_rate',0) <= 1 else stats.get('win_rate', 0)
    profit_factor = stats.get('profit_factor', 0) or 0
    max_dd = stats.get('max_drawdown_pct', 0)

    orders = conn.execute("""
        SELECT order_id, symbol, side, order_type, price, quantity, quantity_filled,
               status, exchange_order_id, created_at
        FROM orders WHERE status IN ('PENDING','OPEN','PARTIAL_FILL')
        ORDER BY created_at DESC LIMIT 50
    """).fetchall()

    trades = conn.execute("""
        SELECT trade_id, symbol, side, quantity, entry_price_avg, exit_price_avg,
               pnl_realized, status, entry_time, exit_time
        FROM trades WHERE pnl_realized IS NOT NULL OR status='CLOSED'
        ORDER BY COALESCE(exit_time, entry_time, created_at) DESC LIMIT 20
    """).fetchall()

    all_orders = conn.execute("""
        SELECT order_id, symbol, side, order_type, price, quantity, status, created_at
        FROM orders ORDER BY created_at DESC LIMIT 100
    """).fetchall()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>OpenQuant Dashboard</title>
<meta http-equiv="refresh" content="{REFRESH_SECONDS}">
<style>{STYLE}</style></head><body>
<h1>OpenQuant 交易看板</h1>

<div class="grid">
    <div class="stat"><div class="stat-value">${equity:,.2f}</div><div class="stat-label">总权益</div></div>
    <div class="stat"><div class="stat-value {pnl_color}">{pnl:+,.2f}</div><div class="stat-label">总盈亏</div></div>
    <div class="stat"><div class="stat-value">${cash:,.2f}</div><div class="stat-label">可用现金</div></div>
    <div class="stat"><div class="stat-value">{total_trades}</div><div class="stat-label">总交易</div></div>
    <div class="stat"><div class="stat-value">{win_rate:.0f}%</div><div class="stat-label">胜率</div></div>
    <div class="stat"><div class="stat-value">{profit_factor:.2f}</div><div class="stat-label">盈亏比</div></div>
    <div class="stat"><div class="stat-value">{max_dd:.1f}%</div><div class="stat-label">最大回撤</div></div>
    <div class="stat"><div class="stat-value">{len(positions)}</div><div class="stat-label">持仓数</div></div>
</div>

<h2>当前挂单 ({len(orders)} 笔)</h2>
<div class="card"><table>
<tr><th>交易对</th><th>方向</th><th>价格</th><th>数量</th><th>已成交</th><th>状态</th><th>交易所ID</th></tr>
"""
    for o in orders:
        side_cls = "badge-sell" if o['side'] == 'SELL' else "badge-buy"
        status_cls = "yellow" if o['status'] == 'PENDING' else "green"
        html += f"<tr><td>{o['symbol']}</td><td><span class='badge {side_cls}'>{o['side']}</span></td><td>${o['price']:,.2f}</td><td>{o['quantity']:.6f}</td><td>{o['quantity_filled']:.6f}</td><td class='{status_cls}'>{o['status']}</td><td>{o['exchange_order_id'] or '-'}</td></tr>"

    html += "</table></div>"

    html += f"<h2>最近成交 ({len(trades)} 笔)</h2><div class='card'><table>"
    html += "<tr><th>交易对</th><th>方向</th><th>数量</th><th>入场价</th><th>出场价</th><th>盈亏</th><th>时间</th></tr>"
    for t in trades:
        pnl_c = "green" if (t['pnl_realized'] or 0) >= 0 else "red"
        html += f"<tr><td>{t['symbol']}</td><td>{t['side']}</td><td>{t['quantity']:.4f}</td><td>${t['entry_price_avg'] or 0:,.2f}</td><td>${t['exit_price_avg'] or 0:,.2f}</td><td class='{pnl_c}'>${t['pnl_realized'] or 0:,.4f}</td><td>{(t['exit_time'] or t['entry_time'] or '')[:19]}</td></tr>"
    html += "</table></div>"

    html += f"<h2>订单历史 ({len(all_orders)} 条)</h2><div class='card'><table>"
    html += "<tr><th>订单ID</th><th>交易对</th><th>方向</th><th>类型</th><th>价格</th><th>数量</th><th>状态</th><th>时间</th></tr>"
    for o in all_orders:
        html += f"<tr><td style='font-size:11px'>{o['order_id'][:16]}...</td><td>{o['symbol']}</td><td>{o['side']}</td><td>{o['order_type']}</td><td>${o['price'] or 0:,.2f}</td><td>{o['quantity']:.6f}</td><td>{o['status']}</td><td>{o['created_at'][:19]}</td></tr>"
    html += "</table></div>"

    html += f"<h2>账户余额</h2><div class='card'><table>"
    html += "<tr><th>账户</th><th>余额 (USDT)</th></tr>"
    for code in sorted(balances.keys()):
        bal = balances[code]
        if abs(bal) > 0.001:
            html += f"<tr><td>{code}</td><td>${bal:,.2f}</td></tr>"
    html += "</table></div>"

    html += f"<div class='refresh'>每 {REFRESH_SECONDS} 秒自动刷新 | 最后更新: {now}</div>"
    html += "</body></html>"
    return html.encode('utf-8')


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            content = build_page()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"Error: {e}".encode())

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    port = 8080
    print(f"\n  OpenQuant Dashboard")
    print(f"  -> 打开浏览器访问: http://localhost:{port}")
    print(f"  -> 每 {REFRESH_SECONDS} 秒自动刷新")
    print(f"  -> 按 Ctrl+C 停止\n")
    server = HTTPServer(("0.0.0.0", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  已停止")
        server.shutdown()
