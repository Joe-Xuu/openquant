"""
================================================================================
LIVE MONITOR WEB SERVER — Lightweight Async Dashboard (aiohttp.web)
================================================================================

Runs inside main.py's asyncio event loop with zero extra dependencies.
Exposes:
  GET /           — Self-contained mobile+desktop live dashboard (auto-refresh 5s)
  GET /api/status — Live JSON snapshot (equity, positions, macro, risk, trades)
  GET /health     — Health check endpoint for Docker / uptime monitors

Optional authentication via MONITOR_TOKEN env var (?token=... or X-Monitor-Token).
================================================================================
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, Optional

from aiohttp import web

logger = logging.getLogger("monitor.web_server")


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OpenQuant 实盘实时监控</title>
  <style>
    :root {
      --bg: #0b0f17; --card: #131a26; --border: #232e42;
      --fg: #e6edf7; --muted: #8b9bb4; --accent: #3b82f6;
      --green: #10b981; --red: #ef4444; --amber: #f59e0b; --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg); color: var(--fg);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 13px; line-height: 1.5; padding: 14px;
    }
    .wrap { max-width: 1120px; margin: 0 auto; display: flex; flex-direction: column; gap: 14px; }
    .hdr { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 10px; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
    .title { font-size: 17px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--green); display: inline-block; box-shadow: 0 0 8px var(--green); }
    .dot.warn { background: var(--amber); box-shadow: 0 0 8px var(--amber); }
    .dot.err { background: var(--red); box-shadow: 0 0 8px var(--red); }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 11px; font-weight: 600; background: #1e293b; border: 1px solid var(--border); }
    .pill.green { color: var(--green); border-color: rgba(16,185,129,.4); }
    .pill.red { color: var(--red); border-color: rgba(239,68,68,.4); }
    .pill.amber { color: var(--amber); border-color: rgba(245,158,11,.4); }
    .pill.blue { color: var(--accent); border-color: rgba(59,130,246,.4); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
    .lbl { font-size: 11px; color: var(--muted); }
    .val { font-size: 20px; font-weight: 700; font-family: ui-monospace, monospace; margin-top: 2px; }
    .sub { font-size: 11px; color: var(--muted); font-family: ui-monospace, monospace; margin-top: 2px; }
    .pos { color: var(--green); } .neg { color: var(--red); } .warn-txt { color: var(--amber); }
    table { width: 100%; border-collapse: collapse; font-family: ui-monospace, monospace; font-size: 12px; }
    th, td { padding: 7px 8px; border-bottom: 1px solid var(--border); text-align: left; white-space: nowrap; }
    th { color: var(--muted); font-weight: 600; font-family: inherit; }
    td.num, th.num { text-align: right; }
    .scroll { overflow-x: auto; }
    canvas { width: 100%; height: 150px; display: block; }
    h2 { font-size: 14px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hdr">
      <div>
        <div class="title"><span id="statusDot" class="dot"></span> OpenQuant 实盘实时监控</div>
        <div id="subInfo" class="sub">正在连接...</div>
      </div>
      <div id="badges" style="display:flex;gap:6px;flex-wrap:wrap;"></div>
    </div>

    <div id="kpis" class="grid"></div>

    <div class="card">
      <h2><span>净值实时曲线（PostgreSQL 历史快照）</span><span id="eqRange" class="sub"></span></h2>
      <canvas id="sparkCanvas"></canvas>
    </div>

    <div class="card">
      <h2><span>币种策略与持仓状态</span><span id="tickInfo" class="sub"></span></h2>
      <div class="scroll">
        <table>
          <thead>
            <tr>
              <th>币种</th><th>最新价</th><th>微观 Regime</th><th>1h 宏观</th><th>4h 做空允许</th>
              <th>趋势方向</th><th class="num">开仓价</th><th class="num">追踪止损</th><th class="num">持仓量</th><th>网格状态</th>
            </tr>
          </thead>
          <tbody id="symBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h2><span>最近成交记录</span><span id="tradeMeta" class="sub"></span></h2>
      <div class="scroll">
        <table>
          <thead>
            <tr><th>时间</th><th>币种</th><th>方向</th><th class="num">开仓价</th><th class="num">平仓价</th><th class="num">数量</th><th class="num">实现盈亏 ($)</th><th>状态</th></tr>
          </thead>
          <tbody id="tradeBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const qs = new URLSearchParams(location.search);
    const token = qs.get("token") || "";
    const apiUrl = "/api/status" + (token ? "?token=" + encodeURIComponent(token) : "");

    function fmtPx(p) {
      if (!p || p <= 0) return "—";
      if (p >= 100) return p.toFixed(2);
      if (p >= 1) return p.toFixed(4);
      return p.toFixed(5);
    }
    function fmtUp(sec) {
      const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60);
      return `${h}h ${m}m`;
    }

    function drawSpark(points) {
      const c = document.getElementById("sparkCanvas");
      const dpr = window.devicePixelRatio || 1;
      const w = c.parentElement.clientWidth - 24, h = 150;
      c.width = w * dpr; c.height = h * dpr;
      const ctx = c.getContext("2d");
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);
      if (!points || points.length < 2) {
        ctx.fillStyle = "#8b9bb4"; ctx.font = "12px sans-serif";
        ctx.fillText("等待净值快照积累...", 12, 78);
        return;
      }
      const vals = points.map(p => p.equity);
      let mn = Math.min(...vals), mx = Math.max(...vals);
      const pad = Math.max(0.5, (mx - mn) * 0.12);
      mn -= pad; mx += pad;
      const pL = 52, pR = 10, pT = 12, pB = 20, pw = w - pL - pR, ph = h - pT - pB;
      const xOf = i => pL + (i / (vals.length - 1)) * pw;
      const yOf = v => pT + (1 - (v - mn) / (mx - mn)) * ph;

      ctx.strokeStyle = "#232e42"; ctx.lineWidth = 0.7; ctx.fillStyle = "#8b9bb4"; ctx.font = "11px ui-monospace, monospace"; ctx.textAlign = "right";
      for (let g = 0; g <= 3; g++) {
        const y = pT + (g / 3) * ph;
        ctx.beginPath(); ctx.moveTo(pL, y); ctx.lineTo(w - pR, y); ctx.stroke();
        ctx.fillText("$" + (mx - (g / 3) * (mx - mn)).toFixed(2), pL - 6, y + 4);
      }
      ctx.strokeStyle = "#3b82f6"; ctx.lineWidth = 2.2;
      ctx.beginPath();
      vals.forEach((v, i) => { const x = xOf(i), y = yOf(v); if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); });
      ctx.stroke();
      ctx.lineTo(xOf(vals.length - 1), pT + ph); ctx.lineTo(xOf(0), pT + ph); ctx.closePath();
      ctx.fillStyle = "rgba(59,130,246,0.12)"; ctx.fill();
      document.getElementById("eqRange").textContent = `最低 $${Math.min(...vals).toFixed(2)} · 最高 $${Math.max(...vals).toFixed(2)}`;
    }

    async function refresh() {
      try {
        const r = await fetch(apiUrl);
        if (!r.ok) throw new Error("HTTP " + r.status);
        const d = await r.json();

        const dot = document.getElementById("statusDot");
        dot.className = "dot" + (d.risk.circuit_breaker_triggered ? " err" : (!d.running ? " warn" : ""));
        document.getElementById("subInfo").textContent =
          `状态: ${d.state} · 运行时长: ${fmtUp(d.uptime_sec)} · 更新于 ${new Date(d.server_time).toLocaleTimeString()}`;

        document.getElementById("badges").innerHTML = `
          <span class="pill blue">${d.market.toUpperCase()} ${d.leverage}x</span>
          <span class="pill ${d.testnet ? "amber" : "green"}">${d.testnet ? "TESTNET 模拟盘" : "MAINNET 实盘"}</span>
          <span class="pill ${d.wechat_enabled ? "green" : ""}">微信推送: ${d.wechat_enabled ? "已开启" : "未配置"}</span>
        `;

        const pnlCls = d.pnl_usd >= 0 ? "pos" : "neg";
        const cbTxt = d.risk.circuit_breaker_triggered ? "已触发熔断" : "正常";
        document.getElementById("kpis").innerHTML = [
          { l: "账户实时总净值", v: `$${d.equity.toFixed(2)}`, s: `初始 $${d.initial_equity.toFixed(2)}`, c: "" },
          { l: "本次运行盈亏", v: `${d.pnl_usd >= 0 ? "+" : ""}$${d.pnl_usd.toFixed(2)}`, s: `${d.pnl_pct >= 0 ? "+" : ""}${d.pnl_pct.toFixed(2)}%`, c: pnlCls },
          { l: "当前回撤 / 上限", v: `${d.risk.current_drawdown_pct.toFixed(2)}%`, s: `上限 ${d.risk.max_drawdown_pct}%`, c: "warn-txt" },
          { l: "今日亏损 / 上限", v: `${d.risk.daily_loss_pct.toFixed(2)}%`, s: `上限 ${d.risk.max_daily_loss_pct}%`, c: "" },
          { l: "风控熔断器", v: cbTxt, s: `连亏 ${d.risk.consecutive_losses} 笔`, c: d.risk.circuit_breaker_triggered ? "neg" : "pos" },
          { l: "累计信号 / Ticks", v: `${d.signal_count}`, s: `Ticks: ${d.tick_count}`, c: "" },
        ].map(k => `<div class="card"><div class="lbl">${k.l}</div><div class="val ${k.c}">${k.v}</div><div class="sub">${k.s}</div></div>`).join("");

        document.getElementById("tickInfo").textContent = `每 5 秒自动刷新`;
        document.getElementById("symBody").innerHTML = (d.symbols || []).map(s => {
          const mCls = s.macro_1h === "BULL" ? "green" : s.macro_1h === "BEAR" ? "red" : "amber";
          const dirCls = s.trend_direction === "LONG" ? "green" : s.trend_direction === "SHORT" ? "red" : "";
          return `
            <tr>
              <td style="font-weight:700">${s.symbol}</td>
              <td>${fmtPx(s.price)}</td>
              <td>${s.regime}</td>
              <td><span class="pill ${mCls}">${s.macro_1h}</span></td>
              <td>${s.short_macro_bear ? '<span class="pill red">允许做空</span>' : '<span class="pill">观望</span>'}</td>
              <td><span class="pill ${dirCls}">${s.trend_direction}</span></td>
              <td class="num">${fmtPx(s.trend_entry)}</td>
              <td class="num">${fmtPx(s.trend_stop)}</td>
              <td class="num">${s.trend_qty > 0 ? s.trend_qty.toFixed(2) : "—"}</td>
              <td>${s.grid_paused ? '<span class="pill amber">已暂停</span>' : (s.grid_active ? '<span class="pill green">运行中</span>' : '待部署')}</td>
            </tr>
          `;
        }).join("") || `<tr><td colspan="10" style="color:var(--muted)">等待行情预热...</td></tr>`;

        const trades = d.recent_trades || [];
        document.getElementById("tradeMeta").textContent = `最近 ${trades.length} 笔`;
        document.getElementById("tradeBody").innerHTML = trades.map(t => {
          const p = t.pnl_realized || 0;
          return `
            <tr>
              <td>${(t.entry_time || "").slice(0, 19).replace("T", " ")}</td>
              <td>${t.symbol}</td>
              <td>${t.side}</td>
              <td class="num">${fmtPx(t.entry_price)}</td>
              <td class="num">${fmtPx(t.exit_price)}</td>
              <td class="num">${(t.quantity || 0).toFixed(2)}</td>
              <td class="num ${p >= 0 ? "pos" : "neg"}">${p >= 0 ? "+" : ""}$${p.toFixed(3)}</td>
              <td>${t.status}</td>
            </tr>
          `;
        }).join("") || `<tr><td colspan="8" style="color:var(--muted)">暂无成交记录</td></tr>`;

        drawSpark(d.equity_history || []);
      } catch (e) {
        document.getElementById("statusDot").className = "dot err";
        document.getElementById("subInfo").textContent = "连接失败: " + e.message;
      }
    }
    refresh();
    setInterval(refresh, 5000);
    window.addEventListener("resize", refresh);
  </script>
</body>
</html>
"""


class MonitorWebServer:
    """Embedded aiohttp.web live monitoring server for TradingSystem."""

    def __init__(
        self,
        status_provider: Callable[[], Dict[str, Any]],
        host: Optional[str] = None,
        port: Optional[int] = None,
        token: Optional[str] = None,
    ):
        self._status_provider = status_provider
        self.host = host or os.getenv("MONITOR_HOST", "0.0.0.0")
        self.port = int(port if port is not None else os.getenv("MONITOR_PORT", "8080"))
        self.token = (token if token is not None else os.getenv("MONITOR_TOKEN", "")).strip()

        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/api/status", self._handle_status)
        self._app.router.add_get("/health", self._handle_health)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    def _is_authorized(self, request: web.Request) -> bool:
        if not self.token:
            return True
        q_tok = request.query.get("token", "")
        h_tok = request.headers.get("X-Monitor-Token", "")
        return q_tok == self.token or h_tok == self.token

    async def _handle_index(self, request: web.Request) -> web.Response:
        if not self._is_authorized(request):
            return web.Response(status=401, text="Unauthorized: append ?token=YOUR_MONITOR_TOKEN")
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")

    async def _handle_status(self, request: web.Request) -> web.Response:
        if not self._is_authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            data = self._status_provider()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"Monitor status error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        auth_note = "token-protected" if self.token else "open (set MONITOR_TOKEN for public VPS!)"
        logger.info(f"MonitorWebServer listening on http://{self.host}:{self.port} ({auth_note})")

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
