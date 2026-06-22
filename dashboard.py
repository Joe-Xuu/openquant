#!/usr/bin/env python3
"""
OpenQuant — 实时交易看板 (含K线图) / Live Dashboard with Candlestick Chart
http://localhost:8080
"""

import hashlib
import hmac
import json, os, sys, time, requests
from decimal import Decimal
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv; load_dotenv()
from core.local_ledger import get_ledger

# PostgreSQL connection from environment
LEDGER_DSN = None  # uses env vars PG_HOST/PG_USER/etc

# PostgreSQL returns NUMERIC as Decimal — convert for JSON
def _to_float(v):
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, dict): return {k: _to_float(vv) for k, vv in v.items()}
    if isinstance(v, list): return [_to_float(x) for x in v]
    return v

# Show fills from last 24 hours (not just current session)
FILL_LOOKBACK_MS = 24 * 3600 * 1000

HTML = r"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>OpenQuant Dashboard</title>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:16px}
h1{color:#58a6ff;font-size:20px;margin-bottom:4px}
.top{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-bottom:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;text-align:center}
.val{font-size:22px;font-weight:bold}.lbl{font-size:11px;color:#8b949e;margin-top:2px}
.g{color:#3fb950}.r{color:#f85149}.y{color:#d2991d}
.row{display:flex;gap:12px;flex-wrap:wrap}
.chart-box{flex:2;min-width:500px;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px}
.panel{flex:1;min-width:300px;max-height:750px;overflow-y:auto}
.tbl{width:100%;border-collapse:collapse;font-size:12px}
.tbl th{text-align:left;padding:4px 8px;border-bottom:2px solid #30363d;color:#8b949e;font-weight:600;font-size:11px}
.tbl td{padding:3px 8px;border-bottom:1px solid #21262d}
.buy{background:#1b3824;color:#3fb950;padding:1px 6px;border-radius:8px;font-size:11px}
.sell{background:#3d1f1f;color:#f85149;padding:1px 6px;border-radius:8px;font-size:11px}
.tabs{display:flex;gap:4px;margin-bottom:8px}
.tab{padding:6px 16px;background:#21262d;border:1px solid #30363d;border-radius:6px 6px 0 0;cursor:pointer;color:#8b949e;font-size:13px}
.tab.active{background:#161b22;color:#fff;border-bottom:1px solid #161b22}
#chart{width:100%;height:400px}
#chart2{width:100%;height:250px;margin-top:6px}
.refresh{color:#555;font-size:11px;text-align:center;margin-top:8px}
.section-title{font-size:13px;color:#8b949e;margin:10px 0 6px;border-bottom:1px solid #21262d;padding-bottom:4px}
.err{color:#f85149;font-size:11px;display:none}
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
    <div class="section-title">📋 订单 (Orders)</div>
    <div id="orders"></div>
    <div class="section-title">✅ 已成交 (Fills)</div>
    <div id="fills"></div>
    <div class="section-title">📜 成交历史</div>
    <div id="trades"></div>
    <div class="err" id="err"></div>
  </div>
</div>
<div class="refresh" id="refresh"></div>

<script>
var currentSymbol='BTCUSDT';
var mainChart=null, volChart=null, candleSeries=null, volSeries=null;
var gridLines=[], markerLines=[];
var failCount=0, chartInited=false;

function initCharts(){
  if(chartInited) return;
  var opts={layout:{background:{color:'#161b22'},textColor:'#8b949e'},grid:{vertLines:{color:'#21262d'},horzLines:{color:'#21262d'}},crosshair:{mode:0},rightPriceScale:{borderColor:'#30363d'},timeScale:{borderColor:'#30363d',timeVisible:true,secondsVisible:false}};
  mainChart=LightweightCharts.createChart(document.getElementById('chart'),Object.assign({},opts,{height:400}));
  volChart=LightweightCharts.createChart(document.getElementById('chart2'),Object.assign({},opts,{height:250}));
  chartInited=true;
}

function switchSymbol(sym,el){
  document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});
  el.classList.add('active');
  currentSymbol=sym;
  // Remove old series, create fresh for new symbol
  if(candleSeries){mainChart.removeSeries(candleSeries);candleSeries=null;}
  if(volSeries){volChart.removeSeries(volSeries);volSeries=null;}
  gridLines.forEach(function(s){try{mainChart.removeSeries(s)}catch(e){}});
  gridLines=[];
  markerLines.forEach(function(s){try{mainChart.removeSeries(s)}catch(e){}});
  markerLines=[];
  fetchData();
}

function fetchData(){
  var xhr=new XMLHttpRequest();
  xhr.open('GET','/api/data?symbol='+currentSymbol,true);
  xhr.onload=function(){
    if(xhr.status===200){
      try{
        var d=JSON.parse(xhr.responseText);
        document.getElementById('err').style.display='none';
        renderStats(d);
        renderOrders(d);
        renderFills(d);
        renderTrades(d);
        renderChart(d);
        document.getElementById('time').textContent=new Date().toLocaleTimeString();
        document.getElementById('refresh').textContent='⏱ 每5秒刷新 | $'+d.price.toFixed(2)+' | '+d.candles.length+'根K线 | '+d.open_orders+'挂单';
        failCount=0;
      }catch(e){
        failCount++;
        if(failCount>3) document.getElementById('err').style.display='block';
        document.getElementById('err').textContent='Error: '+e.message;
      }
    }
  };
  xhr.onerror=function(){failCount++;};
  xhr.send();
}

function renderStats(d){
  var eqClass=d.total_pnl>=0?'g':'r';
  document.getElementById('stats').innerHTML=
    '<div class=card><div class="val '+eqClass+'">$'+d.equity.toLocaleString()+'</div><div class=lbl>交易所总权益</div></div>'+
    '<div class=card><div class="val '+eqClass+'">'+(d.total_pnl>=0?'+':'')+d.total_pnl.toFixed(2)+'</div><div class=lbl>总盈亏</div></div>'+
    '<div class=card><div class=val>'+d.open_orders+' ORDER</div><div class=lbl>当前挂单</div></div>'+
    '<div class=card><div class=val>'+d.filled_today+' FILL</div><div class=lbl>今日成交</div></div>';
}

function renderOrders(d){
  var h='<div class=card><table class=tbl><tr><th>方向</th><th>价格</th><th>数量</th><th>状态</th></tr>';
  if(d.orders.length===0) h+='<tr><td colspan=4 style=color:#555>暂无挂单</td></tr>';
  d.orders.forEach(function(o){
    h+='<tr><td><span class="'+(o.side=='SELL'?'sell':'buy')+'">'+o.side+'</span></td><td>$'+o.price.toFixed(2)+'</td><td>'+o.qty.toFixed(5)+'</td><td style=color:#3fb950>'+o.status+'</td></tr>';
  });
  document.getElementById('orders').innerHTML=h+'</table></div>';
}

function renderFills(d){
  var h='<div class=card><table class=tbl><tr><th>方向</th><th>价格</th><th>数量</th><th>时间</th></tr>';
  if(d.fills.length===0) h+='<tr><td colspan=4 style=color:#555>暂无成交</td></tr>';
  d.fills.slice(-10).reverse().forEach(function(f){
    var ts=f.time?new Date(f.time).toLocaleString():'';
    h+='<tr><td><span class="'+(f.side=='SELL'?'sell':'buy')+'">'+f.side+'</span></td><td>$'+f.price.toFixed(2)+'</td><td>'+f.qty.toFixed(5)+'</td><td style=font-size:11px;color:#888>'+ts+'</td></tr>';
  });
  document.getElementById('fills').innerHTML=h+'</table></div>';
}

function renderTrades(d){
  var h='<div class=card><table class=tbl><tr><th>方向</th><th>入场</th><th>离场</th><th>数量</th><th>盈亏</th></tr>';
  if(d.completed_trades.length===0) h+='<tr><td colspan=5 style=color:#555>暂无完成交易</td></tr>';
  d.completed_trades.forEach(function(t){
    var cls=t.pnl>=0?'g':'r';
    h+='<tr><td>'+t.side+'</td><td>$'+t.entry.toFixed(2)+'</td><td>$'+t.exit.toFixed(2)+'</td><td>'+t.qty.toFixed(5)+'</td><td class='+cls+'>$'+t.pnl.toFixed(4)+'</td></tr>';
  });
  document.getElementById('trades').innerHTML=h+'</table></div>';
}

function renderChart(d){
  try{
    initCharts();
    var cdata=d.candles.map(function(c){return {time:c.t/1000,open:c.o,high:c.h,low:c.l,close:c.c,volume:c.v};});

    // Candlestick
    if(!candleSeries){
      candleSeries=mainChart.addCandlestickSeries({upColor:'#3fb950',downColor:'#f85149',borderUpColor:'#3fb950',borderDownColor:'#f85149',wickUpColor:'#3fb950',wickDownColor:'#f85149'});
    }
    candleSeries.setData(cdata);

    // Volume
    if(!volSeries){
      volSeries=volChart.addHistogramSeries({color:'#3fb95055',priceFormat:{type:'volume'},priceScaleId:''});
    }
    volSeries.setData(cdata.map(function(c){return {time:c.time,value:c.volume,color:c.c>=c.o?'#3fb95055':'#f8514955'};}));

    // Grid lines (rebuild)
    gridLines.forEach(function(s){try{mainChart.removeSeries(s)}catch(e){}});
    gridLines=[];
    var t0=cdata[0]?.time||0, t1=cdata[cdata.length-1]?.time||0;
    (d.grid_levels||[]).forEach(function(l){
      var ls=mainChart.addLineSeries({color:l.side=='BUY'?'#00b4d8':'#ff6b00',lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false});
      ls.setData([{time:t0,value:l.price},{time:t1,value:l.price}]);
      gridLines.push(ls);
    });

    // Trade markers: use a dedicated scatter-like series with bright colors
    markerLines.forEach(function(s){try{mainChart.removeSeries(s)}catch(e){}});
    markerLines=[];

    if(d.fill_markers.length>0){
      d.fill_markers.forEach(function(f){
        var ls=mainChart.addLineSeries({
          color:f.side=='BUY'?'#00ff88':'#ff4444',
          lineWidth:3, lineStyle:0, lineVisible:true,
          pointMarkersVisible:true,
          priceLineVisible:false, lastValueVisible:false,
        });
        // Single-point line = a dot on the chart
        ls.setData([{time:f.t,value:f.p}]);
        markerLines.push(ls);
      });
    }
  }catch(e){
    document.getElementById('err').textContent='Chart error: '+e.message;
    document.getElementById('err').style.display='block';
  }
}

fetchData();
setInterval(fetchData,5000);
</script></body></html>"""


def build_api(symbol):
    ledger = get_ledger(LEDGER_DSN)
    key = os.getenv('BINANCE_TESTNET_API_KEY', '')
    secret = os.getenv('BINANCE_TESTNET_API_SECRET', '')

    def _signed_get(endpoint, extra_params=None):
        p = {'timestamp': int(time.time() * 1000), 'recvWindow': 5000}
        if extra_params: p.update(extra_params)
        q = '&'.join(f'{k}={v}' for k, v in sorted(p.items()))
        sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
        return requests.get(f"https://testnet.binance.vision{endpoint}?{q}&signature={sig}", headers={'X-MBX-APIKEY': key}, timeout=5)

    # Fetch candles
    try:
        r = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&limit=300", timeout=5)
        candles = [{"t": c[0], "o": float(c[1]), "h": float(c[2]), "l": float(c[3]), "c": float(c[4]), "v": float(c[5])} for c in r.json()]
        price = candles[-1]["c"]
    except:
        candles = [{"t": int(time.time()*1000), "o": 0, "h": 0, "l": 0, "c": 0, "v": 0}]
        price = 0

    # ---- UNIFIED ACCOUNT EQUITY (from exchange, not ledger) ----
    try:
        rr = _signed_get('/api/v3/account')
        btc_qty = eth_qty = usdt_bal = 0.0
        for b in rr.json().get('balances', []):
            free = float(b['free']); locked = float(b['locked']); total = free + locked
            if b['asset'] == 'BTC': btc_qty = total
            elif b['asset'] == 'ETH': eth_qty = total
            elif b['asset'] == 'USDT': usdt_bal = total
        rp = requests.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT', timeout=5)
        btc_px = float(rp.json()['price'])
        rp = requests.get('https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT', timeout=5)
        eth_px = float(rp.json()['price'])
        total_equity = usdt_bal + btc_qty * btc_px + eth_qty * eth_px
        # Track initial equity for correct P&L
        initial_key = f"initial_equity_{os.uname().nodename}"
        initial = ledger.get_metadata(initial_key)
        if initial is None:
            ledger.set_metadata(initial_key, str(total_equity))
            initial = str(total_equity)
        unrealized_pnl_total = total_equity - float(initial)
    except:
        total_equity = 10000.0; unrealized_pnl_total = 0.0

    # ---- GRID LEVELS (computed from strategy, plus live exchange orders) ----
    grid_levels = []
    orders_list = []
    try:
        rr = _signed_get('/api/v3/openOrders', {'symbol': symbol})
        exchange_orders = rr.json() if isinstance(rr.json(), list) else []
        for o in exchange_orders:
            grid_levels.append({"side": o['side'], "price": float(o['price'])})
            orders_list.append({"side": o['side'], "price": float(o['price']),
                               "qty": float(o['origQty']), "status": o['status']})
    except:
        pass

    # Add INTENDED grid levels (both sides) from strategy config
    try:
        if price > 0:
            from strategy.grid_strategy import GridStrategy
            gs = GridStrategy(grid_type='geometric', upper_bound_pct=5.0, lower_bound_pct=5.0,
                              grid_levels=10, profit_per_grid_pct=0.5, total_capital=7000)
            full_grid = gs.compute_grid(price, symbol)
            # Add all levels not already present
            existing_prices = {g['price'] for g in grid_levels}
            for lvl in full_grid.levels:
                p = round(lvl.price, 2)
                if p not in existing_prices:
                    grid_levels.append({"side": lvl.side.value, "price": p})
    except Exception:
        pass

    # ---- FILLS (last 24h) and Position (FIFO) ----
    fills = []
    try:
        rr = _signed_get('/api/v3/myTrades', {'symbol': symbol, 'limit': 100})
        for t in rr.json():
            if t.get('time', 0) < int(time.time() * 1000) - FILL_LOOKBACK_MS: continue
            fills.append({
                "side": "BUY" if t.get('isBuyer') else "SELL",
                "price": float(t.get('price', 0)), "qty": float(t.get('qty', 0)),
                "time": t.get('time', 0),
            })
    except: pass

    # FIFO: completed trades + remaining position
    completed = []; realized_pnl = 0.0
    buy_q = [(f["qty"], f["price"]) for f in fills if f["side"] == "BUY"]
    sell_q = [(f["qty"], f["price"]) for f in fills if f["side"] == "SELL"]
    bi = si = 0
    while bi < len(buy_q) and si < len(sell_q):
        bq, bp = buy_q[bi]; sq, sp = sell_q[si]
        m = min(bq, sq)
        realized_pnl += (sp - bp) * m
        completed.append({"side": "BUY→SELL", "entry": bp, "exit": sp, "qty": m, "pnl": (sp-bp)*m})
        buy_q[bi] = (bq - m, bp); sell_q[si] = (sq - m, sp)
        if buy_q[bi][0] < 0.000001: bi += 1
        if sell_q[si][0] < 0.000001: si += 1

    # Remaining = open position (only from unmatched buys)
    remaining_buys = sum(b[0] for b in buy_q[bi:])
    if remaining_buys > 0.0001:
        pos_side = "LONG"
        net_qty = remaining_buys
        total_val = sum(b[0] * b[1] for b in buy_q[bi:])
        avg_entry = total_val / remaining_buys if remaining_buys > 0 else 0
        unrealized = (price - avg_entry) * remaining_buys
    else:
        pos_side = "FLAT"; net_qty = 0; avg_entry = 0; unrealized = 0

    fill_markers = [{"t": f["time"] / 1000, "side": f["side"], "price": f["price"], "p": f["price"]} for f in fills]

    data = {
        "symbol": symbol,
        "price": price,
        "equity": round(total_equity, 2),
        "pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized, 2),
        "total_pnl": round(unrealized_pnl_total, 2),
        "net_qty": round(net_qty, 6),
        "avg_entry": round(avg_entry, 2),
        "position_side": pos_side,
        "open_orders": len(orders_list),
        "filled_today": len(fills),
        "candles": candles,
        "orders": orders_list,
        "fills": fills,
        "completed_trades": completed,
        "fill_markers": fill_markers,
        "grid_levels": grid_levels,
    }
    return _to_float(data)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/data":
                params = parse_qs(urlparse(self.path).query)
                symbol = params.get("symbol", ["BTCUSDT"])[0]
                self._json(build_api(symbol))
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
    print(f"\n  📊 OpenQuant Dashboard v2")
    print(f"  → http://localhost:{port}")
    print(f"  → K线图 + 网格线 + 成交标记 + 自动刷新")
    print(f"  → 显示最近24小时成交\n")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
