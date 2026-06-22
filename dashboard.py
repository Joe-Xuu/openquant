#!/usr/bin/env python3
"""
OpenQuant — 实时交易看板 (含K线图) / Live Trading Dashboard with Candlestick Chart
http://localhost:8080
"""

import json, os, sys, time, requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.local_ledger import get_ledger

LEDGER_PATH = "data/trading_ledger.db"
PRIMARY_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>OpenQuant Dashboard</title>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:16px}
h1{color:#58a6ff;font-size:20px;margin-bottom:4px}
.top{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px;margin-bottom:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;text-align:center}
.val{font-size:22px;font-weight:bold}.lbl{font-size:11px;color:#8b949e;margin-top:2px}
.g{color:#3fb950}.r{color:#f85149}
.row{display:flex;gap:12px;flex-wrap:wrap}
.chart-box{flex:2;min-width:500px;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px}
.panel{flex:1;min-width:300px}
.tbl{width:100%;border-collapse:collapse;font-size:12px}
.tbl th{text-align:left;padding:4px 8px;border-bottom:2px solid #30363d;color:#8b949e;font-weight:600}
.tbl td{padding:3px 8px;border-bottom:1px solid #21262d}
.buy{background:#1b3824;color:#3fb950;padding:1px 6px;border-radius:8px;font-size:11px}
.sell{background:#3d1f1f;color:#f85149;padding:1px 6px;border-radius:8px;font-size:11px}
.tabs{display:flex;gap:4px;margin-bottom:8px}
.tab{padding:6px 16px;background:#21262d;border:1px solid #30363d;border-radius:6px 6px 0 0;cursor:pointer;color:#8b949e;font-size:13px}
.tab.active{background:#161b22;color:#fff;border-bottom:1px solid #161b22}
.btn{background:#238636;color:#fff;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:12px}
.btn:hover{background:#2ea043}
#chart{width:100%;height:420px}
#chart2{width:100%;height:300px;margin-top:8px}
.refresh{color:#555;font-size:11px;text-align:center;margin-top:10px}
</style></head><body>
<h1>📊 OpenQuant <span style="font-size:14px;color:#8b949e" id="time"></span></h1>

<div class="top" id="stats"></div>

<div class="row">
  <div class="chart-box">
    <div class="tabs">
      <div class="tab active" onclick="switchSymbol('BTCUSDT',this)">BTC/USDT</div>
      <div class="tab" onclick="switchSymbol('ETHUSDT',this)">ETH/USDT</div>
    </div>
    <div id="chart"></div>
    <div id="chart2"></div>
  </div>
  <div class="panel">
    <h3 style="font-size:14px;color:#8b949e;margin-bottom:6px">📋 订单 / Orders</h3>
    <div id="orders"></div>
    <h3 style="font-size:14px;color:#8b949e;margin:12px 0 6px">✅ 成交 / Fills</h3>
    <div id="fills"></div>
  </div>
</div>
<div class="refresh" id="refresh"></div>

<script>
let currentSymbol='BTCUSDT';
let charts={}, series={}, gridLines=[], markers=[];

function makeChart(id,h){
  const c=LightweightCharts.createChart(document.getElementById(id),{
    layout:{background:{color:'#161b22'},textColor:'#8b949e'},
    grid:{vertLines:{color:'#21262d'},horzLines:{color:'#21262d'}},
    crosshair:{mode:0}, rightPriceScale:{borderColor:'#30363d'},
    timeScale:{borderColor:'#30363d',timeVisible:true,secondsVisible:false},
    height:h
  });
  c.addCandlestickSeries({upColor:'#3fb950',downColor:'#f85149',borderUpColor:'#3fb950',borderDownColor:'#f85149',wickUpColor:'#3fb950',wickDownColor:'#f85149'});
  c.addHistogramSeries({color:'#3fb95055',priceFormat:{type:'volume'},priceScaleId:''});
  return c;
}

function switchSymbol(sym,el){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  currentSymbol=sym;
  fetchData();
}

async function fetchData(){
  try{
    const r=await fetch('/api/data?symbol='+currentSymbol);
    const d=await r.json();
    renderStats(d);
    renderOrders(d);
    renderChart(d);
    document.getElementById('time').textContent=new Date().toLocaleTimeString();
    document.getElementById('refresh').textContent='⏱ 每5秒刷新 | '+d.price+' | '+d.candles.length+'根K线';
  }catch(e){console.error(e)}
}

function renderStats(d){
  document.getElementById('stats').innerHTML=
    '<div class=card><div class="val '+(d.pnl>=0?'g':'r')+'">$'+d.equity.toLocaleString()+'</div><div class=lbl>总权益</div></div>'+
    '<div class=card><div class="val '+(d.pnl>=0?'g':'r')+'">'+(d.pnl>=0?'+':'')+d.pnl.toFixed(2)+'</div><div class=lbl>盈亏</div></div>'+
    '<div class=card><div class=val>'+d.open_orders+'</div><div class=lbl>挂单</div></div>'+
    '<div class=card><div class=val>'+d.filled_count+'</div><div class=lbl>已成交</div></div>'+
    '<div class=card><div class=val>'+d.win_rate+'%</div><div class=lbl>胜率</div></div>';
}

function renderOrders(d){
  let h='<div class=card><table class=tbl><tr><th>方向</th><th>价格</th><th>数量</th><th>状态</th></tr>';
  d.orders.forEach(o=>{
    h+='<tr><td><span class="'+(o.side=='SELL'?'sell':'buy')+'">'+o.side+'</span></td><td>$'+o.price.toFixed(2)+'</td><td>'+o.qty.toFixed(5)+'</td><td style="color:'+(o.status=='OPEN'?'#3fb950':'#d2991d')+'">'+o.status+'</td></tr>';
  });
  document.getElementById('orders').innerHTML=h+'</table></div>';

  h='<div class=card><table class=tbl><tr><th>方向</th><th>价格</th><th>数量</th><th>盈亏</th></tr>';
  d.fills.forEach(f=>{
    h+='<tr><td>'+f.side+'</td><td>$'+f.price.toFixed(2)+'</td><td>'+f.qty.toFixed(5)+'</td><td class="'+(f.pnl>=0?'g':'r')+'">$'+f.pnl.toFixed(4)+'</td></tr>';
  });
  document.getElementById('fills').innerHTML=h+'</table></div>';
}

function renderChart(d){
  const cdata=d.candles.map(c=>({time:c.t/1000,open:c.o,high:c.h,low:c.l,close:c.c,volume:c.v}));
  const key=currentSymbol;
  if(!charts[key]){
    charts[key]={c1:makeChart('chart',420),c2:makeChart('chart2',300)};
    charts[key].c1.timeScale().fitContent();
    charts[key].c2.timeScale().fitContent();
  }
  const ch=charts[key];
  ch.c1.resize(document.getElementById('chart').clientWidth,420);
  ch.c2.resize(document.getElementById('chart2').clientWidth,300);

  // Candlestick
  const cs=ch.c1.serieses?.[0]||ch.c1.addCandlestickSeries({upColor:'#3fb950',downColor:'#f85149',borderUpColor:'#3fb950',borderDownColor:'#f85149',wickUpColor:'#3fb950',wickDownColor:'#f85149'});
  cs.setData(cdata);

  // Volume
  const vs=ch.c2.serieses?.[0]||ch.c2.addHistogramSeries({color:'#3fb95055',priceFormat:{type:'volume'},priceScaleId:''});
  vs.setData(cdata.map(c=>{let col=c.c>=c.o?'#3fb95055':'#f8514955';return{time:c.time,value:c.volume,color:col}}));

  // Grid levels
  gridLines.forEach(l=>ch.c1.removeSeries(l));
  gridLines=[];
  (d.grid_levels||[]).forEach(l=>{
    const ls=ch.c1.addLineSeries({color:l.side=='BUY'?'#00b4d8':'#ff6b00',lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false});
    const t0=cdata[0]?.time||0; const t1=cdata[cdata.length-1]?.time||0;
    ls.setData([{time:t0,value:l.price},{time:t1,value:l.price}]);
    gridLines.push(ls);
  });

  // Trade markers
  markers.forEach(m=>ch.c1.removeSeries(m));
  markers=[];
  const mks=ch.c1.addLineSeries({color:'#ffffff00',lineWidth:0,priceLineVisible:false,lastValueVisible:false});
  const mkdata=[];
  d.filled_orders.forEach(f=>{
    const t=f.timestamp?Math.floor(new Date(f.timestamp).getTime()/1000):cdata[Math.floor(cdata.length/2)]?.time;
    mkdata.push({time:t,value:f.price});
  });
  if(mkdata.length>0){
    mks.setMarkers(mkdata.map(m=>({time:m.time,position: m.value>d.price?'belowBar':'aboveBar',color:m.value>d.price?'#ff6b00':'#00b4d8',shape:m.value>d.price?'arrowDown':'arrowUp',text:'●',size:2})));
    markers.push(mks);
  }
}

fetchData();
setInterval(fetchData,5000);
</script></body></html>"""


def build_api(symbol):
    ledger = get_ledger(LEDGER_PATH)
    conn = ledger._get_connection()

    # Fetch candles from Binance
    try:
        r = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&limit=100", timeout=5)
        candles = []
        for c in r.json():
            candles.append({"t": c[0], "o": float(c[1]), "h": float(c[2]), "l": float(c[3]), "c": float(c[4]), "v": float(c[5])})
    except:
        candles = []

    # Ledger data
    equity = ledger.get_total_equity()
    balances = ledger.get_all_balances()
    initial = balances.get("EQUITY-INITIAL", 10000)
    pnl = equity - initial
    stats = ledger.get_trade_statistics()
    wr = stats.get('win_rate', 0)
    if isinstance(wr, float) and wr <= 1: wr *= 100

    orders = conn.execute(
        "SELECT order_id, symbol, side, price, quantity, quantity_filled, status FROM orders WHERE status IN ('OPEN','PENDING') AND symbol=? ORDER BY price DESC", (symbol,)
    ).fetchall()
    all_orders = conn.execute(
        "SELECT order_id, symbol, side, price, quantity, status, exchange_order_id, created_at FROM orders WHERE symbol=? ORDER BY created_at DESC LIMIT 50", (symbol,)
    ).fetchall()

    # Fills from exchange
    try:
        from dotenv import load_dotenv; load_dotenv()
        import hmac, hashlib
        key = os.getenv('BINANCE_TESTNET_API_KEY', '')
        secret = os.getenv('BINANCE_TESTNET_API_SECRET', '')
        fills = []
        if key and secret:
            p = {'symbol': symbol, 'timestamp': int(time.time() * 1000), 'recvWindow': 5000, 'limit': 20}
            q = '&'.join(f'{k}={v}' for k, v in sorted(p.items()))
            sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
            rr = requests.get(f"https://testnet.binance.vision/api/v3/myTrades?{q}&signature={sig}", headers={'X-MBX-APIKEY': key}, timeout=5)
            for t in rr.json():
                fills.append({"side": t.get("side", "?"), "price": float(t.get("price", 0)), "qty": float(t.get("qty", 0)),
                              "timestamp": datetime.fromtimestamp(t["time"] / 1000, tz=timezone.utc).isoformat()})
    except:
        fills = []

    # Current price
    price = candles[-1]["c"] if candles else 0

    # Grid levels from open orders
    grid_levels = []
    for o in orders:
        grid_levels.append({"side": o["side"], "price": o["price"]})

    # Build fill data with P&L (approximate: buy→sell pairs)
    fill_data = []
    buys = [f for f in fills if f["side"] == "BUY"]
    sells = [f for f in fills if f["side"] == "SELL"]
    for b, s in zip(buys, sells):
        pnl = (s["price"] - b["price"]) * b["qty"]
        fill_data.append({"side": "BUY→SELL", "price": b["price"], "qty": b["qty"], "pnl": pnl})

    return {
        "symbol": symbol,
        "price": price,
        "equity": round(equity, 2),
        "pnl": round(pnl, 2),
        "open_orders": len(orders),
        "filled_count": len(fills),
        "win_rate": round(wr, 0),
        "candles": candles,
        "orders": [{"side": o["side"], "price": o["price"], "qty": o["quantity"], "status": o["status"]} for o in orders],
        "fills": fill_data,
        "filled_orders": [{"price": f["price"], "timestamp": f.get("timestamp", "")} for f in fills],
        "grid_levels": grid_levels,
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/data":
                params = parse_qs(urlparse(self.path).query)
                symbol = params.get("symbol", ["BTCUSDT"])[0]
                data = build_api(symbol)
                self._json(data)
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(HTML.encode())
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def _json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, *a): pass


if __name__ == "__main__":
    port = 8080
    print(f"\n  📊 OpenQuant Dashboard")
    print(f"  → http://localhost:{port}")
    print(f"  → 实时K线图 + 订单 + 成交 + 网格线")
    print(f"  → 每5秒刷新，按 Ctrl+C 停止\n")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
