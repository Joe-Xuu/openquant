"""
================================================================================
LIVE MONITOR & CONTROL WEB SERVER — Lightweight Async Dashboard (aiohttp.web)
================================================================================

Runs inside main.py's asyncio event loop with zero extra dependencies.
Exposes:
  GET  /            — Self-contained mobile+desktop live dashboard & control panel
  GET  /api/status  — Live JSON snapshot (equity, positions, macro, risk, params, logs)
  POST /api/control — Safe remote control (pause / resume / reset_risk / emergency_stop)
  POST /api/params  — Validated hot-reload of safe strategy & risk parameters
  GET  /health      — Health check endpoint for Docker / uptime monitors

SECURITY:
  - Read access requires MONITOR_TOKEN when configured (?token=... or X-Monitor-Token).
  - Write endpoints (POST /api/control, POST /api/params) are STRICTLY DISABLED
    (HTTP 403 read-only mode) unless MONITOR_TOKEN is explicitly configured!
================================================================================
"""

from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict, Optional

from aiohttp import web

logger = logging.getLogger("monitor.web_server")


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OpenQuant 实盘监控与控制台</title>
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
    .wrap { max-width: 1140px; margin: 0 auto; display: flex; flex-direction: column; gap: 14px; }
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
    .btn {
      padding: 7px 14px; border-radius: 8px; border: 1px solid var(--border);
      background: #1e293b; color: var(--fg); font-size: 12px; font-weight: 600;
      cursor: pointer; transition: opacity .15s, border-color .15s;
    }
    .btn:hover:not(:disabled) { border-color: var(--accent); opacity: .92; }
    .btn:disabled { opacity: .4; cursor: not-allowed; }
    .btn.green { background: rgba(16,185,129,.16); border-color: rgba(16,185,129,.5); color: var(--green); }
    .btn.amber { background: rgba(245,158,11,.16); border-color: rgba(245,158,11,.5); color: var(--amber); }
    .btn.red { background: rgba(239,68,68,.16); border-color: rgba(239,68,68,.5); color: var(--red); }
    .btn.blue { background: var(--accent); border-color: var(--accent); color: #fff; }
    .form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(175px, 1fr)); gap: 10px; margin-top: 8px; }
    .field { display: flex; flex-direction: column; gap: 4px; }
    .field label { font-size: 11px; color: var(--muted); }
    .field input[type="number"] {
      background: var(--bg); color: var(--fg); border: 1px solid var(--border);
      border-radius: 6px; padding: 6px 8px; font-family: ui-monospace, monospace; font-size: 12px;
    }
    .toast {
      display: none; padding: 8px 12px; border-radius: 8px; font-size: 12px;
      border: 1px solid var(--border); background: #1e293b;
    }
    .toast.show { display: block; }
    .toast.ok { border-color: rgba(16,185,129,.5); color: var(--green); }
    .toast.err { border-color: rgba(239,68,68,.5); color: var(--red); }
    table { width: 100%; border-collapse: collapse; font-family: ui-monospace, monospace; font-size: 12px; }
    th, td { padding: 7px 8px; border-bottom: 1px solid var(--border); text-align: left; white-space: nowrap; }
    th { color: var(--muted); font-weight: 600; font-family: inherit; }
    td.num, th.num { text-align: right; }
    .scroll { overflow-x: auto; }
    .logbox {
      background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
      padding: 8px 10px; font-family: ui-monospace, monospace; font-size: 11px;
      max-height: 200px; overflow-y: auto; white-space: pre-wrap; color: var(--muted);
    }
    canvas { width: 100%; height: 150px; display: block; }
    h2 { font-size: 14px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; gap: 8px; flex-wrap: wrap; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hdr">
      <div>
        <div class="title"><span id="statusDot" class="dot"></span> OpenQuant 实盘监控与控制台</div>
        <div id="subInfo" class="sub">正在连接...</div>
      </div>
      <div id="badges" style="display:flex;gap:6px;flex-wrap:wrap;"></div>
    </div>

    <div id="toast" class="toast"></div>

    <!-- Control Bar -->
    <div class="card">
      <h2>
        <span>远程启停与风控操作</span>
        <span id="ctrlHint" class="sub"></span>
      </h2>
      <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;">
        <button id="btnPauseResume" class="btn amber" onclick="togglePause()">暂停开新仓</button>
        <button id="btnResetRisk" class="btn" onclick="resetRisk()">重置风控熔断/基准</button>
        <button id="btnEmergency" class="btn red" onclick="emergencyStop()">紧急全平并暂停</button>
      </div>
    </div>

    <div id="kpis" class="grid"></div>

    <div class="card">
      <h2><span>净值实时曲线（PostgreSQL 历史快照）</span><span id="eqRange" class="sub"></span></h2>
      <canvas id="sparkCanvas"></canvas>
    </div>

    <!-- Hot-Reload Parameters -->
    <div class="card">
      <h2>
        <span>策略与风控热更新参数（即时生效，严格范围保护）</span>
        <button id="btnSaveParams" class="btn blue" onclick="saveParams()">保存并热生效</button>
      </h2>
      <div class="form-grid">
        <div class="field">
          <label>网格资金占比 (0.10 ~ 0.75)</label>
          <input id="p_grid_pct" type="number" step="0.05" min="0.10" max="0.75" />
        </div>
        <div class="field">
          <label>趋势资金占比 (0.10 ~ 0.75)</label>
          <input id="p_trend_pct" type="number" step="0.05" min="0.10" max="0.75" />
        </div>
        <div class="field">
          <label>总回撤熔断线 % (5 ~ 35)</label>
          <input id="p_max_dd" type="number" step="1" min="5" max="35" />
        </div>
        <div class="field">
          <label>单日亏损上限 % (2 ~ 15)</label>
          <input id="p_max_daily" type="number" step="0.5" min="2" max="15" />
        </div>
        <div class="field">
          <label>趋势单笔风险 % (0.5 ~ 5.0)</label>
          <input id="p_risk_pct" type="number" step="0.5" min="0.5" max="5.0" />
        </div>
        <div class="field">
          <label>网格单格止盈 % (0.8 ~ 3.0)</label>
          <input id="p_grid_tp" type="number" step="0.1" min="0.8" max="3.0" />
        </div>
        <div class="field">
          <label>熊市网格清仓深度 % (0 ~ 15)</label>
          <input id="p_bear_depth" type="number" step="0.5" min="0" max="15" />
        </div>
        <div class="field" style="justify-content:center;">
          <label>趋势允许做空（仅合约有效）</label>
          <label style="display:flex;align-items:center;gap:6px;color:var(--fg);margin-top:4px;cursor:pointer;">
            <input id="p_allow_short" type="checkbox" /> 启用 4h 熊市做空
          </label>
        </div>
      </div>
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

    <div class="card">
      <h2><span>实时运行日志（最近 50 行）</span><span class="sub">免 SSH 手机排错</span></h2>
      <div id="logBox" class="logbox">等待日志...</div>
    </div>
  </div>

  <script>
    const qs = new URLSearchParams(location.search);
    const token = qs.get("token") || "";
    const authSuffix = token ? "?token=" + encodeURIComponent(token) : "";
    let lastState = null;
    let paramsLoaded = false;

    function showToast(msg, ok = true) {
      const el = document.getElementById("toast");
      el.textContent = msg;
      el.className = "toast show " + (ok ? "ok" : "err");
      setTimeout(() => { el.className = "toast"; }, 5000);
    }

    async function postJson(path, body) {
      const r = await fetch(path + authSuffix, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Monitor-Token": token },
        body: JSON.stringify(body),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || ("HTTP " + r.status));
      return data;
    }

    async function togglePause() {
      const isPaused = lastState && lastState.paused;
      const action = isPaused ? "resume" : "pause";
      const label = isPaused ? "恢复自动开仓" : "暂停新开仓（保留已有持仓止损与TP）";
      if (!confirm("确认" + label + "？")) return;
      try {
        const res = await postJson("/api/control", { action });
        showToast(res.message || "操作成功", true);
        refresh();
      } catch (e) { showToast("操作失败: " + e.message, false); }
    }

    async function resetRisk() {
      if (!confirm("确认以当前实时净值重置回撤高点并解除风控熔断？")) return;
      try {
        const res = await postJson("/api/control", { action: "reset_risk" });
        showToast(res.message || "风控基准已重置", true);
        refresh();
      } catch (e) { showToast("操作失败: " + e.message, false); }
    }

    async function emergencyStop() {
      const typed = prompt("⚠️ 紧急全平停机将撤销所有挂单、市价平掉全部持仓并暂停系统！\n请输入 STOP 确认：");
      if (typed !== "STOP") { if (typed !== null) showToast("输入不匹配，已取消", false); return; }
      try {
        const res = await postJson("/api/control", { action: "emergency_stop", confirm: "STOP" });
        showToast(res.message || "已执行紧急全平并暂停", true);
        refresh();
      } catch (e) { showToast("操作失败: " + e.message, false); }
    }

    async function saveParams() {
      const payload = {
        grid_capital_pct: parseFloat(document.getElementById("p_grid_pct").value),
        trend_capital_pct: parseFloat(document.getElementById("p_trend_pct").value),
        max_drawdown_pct: parseFloat(document.getElementById("p_max_dd").value),
        max_daily_loss_pct: parseFloat(document.getElementById("p_max_daily").value),
        risk_per_trade_pct: parseFloat(document.getElementById("p_risk_pct").value),
        profit_per_grid_pct: parseFloat(document.getElementById("p_grid_tp").value),
        bear_exit_depth_pct: parseFloat(document.getElementById("p_bear_depth").value),
        allow_short: document.getElementById("p_allow_short").checked,
      };
      if (!confirm("确认热更新以上参数？（将立即生效并推送微信通知）")) return;
      try {
        const res = await postJson("/api/params", payload);
        showToast(res.message || "参数已更新", true);
        paramsLoaded = false;
        refresh();
      } catch (e) { showToast("参数校验失败: " + e.message, false); }
    }

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
        const r = await fetch("/api/status" + authSuffix);
        if (!r.ok) throw new Error("HTTP " + r.status);
        const d = await r.json();
        lastState = d;

        const dot = document.getElementById("statusDot");
        dot.className = "dot" + ((d.risk && d.risk.circuit_breaker_triggered) ? " err" : (d.paused || !d.running ? " warn" : ""));
        document.getElementById("subInfo").textContent =
          `状态: ${d.paused ? "PAUSED (手动暂停)" : d.state} · 运行时长: ${fmtUp(d.uptime_sec)} · 更新于 ${new Date(d.server_time).toLocaleTimeString()}`;

        document.getElementById("badges").innerHTML = `
          <span class="pill blue">${d.market.toUpperCase()} ${d.leverage}x</span>
          <span class="pill ${d.testnet ? "amber" : "green"}">${d.testnet ? "TESTNET 模拟盘" : "MAINNET 实盘"}</span>
          <span class="pill ${d.controls_enabled ? "green" : "amber"}">${d.controls_enabled ? "远程控制已启用" : "只读模式 (未设口令)"}</span>
          <span class="pill ${d.wechat_enabled ? "green" : ""}">微信: ${d.wechat_enabled ? "已开启" : "未配置"}</span>
        `;

        // Control button states
        const canCtrl = Boolean(d.controls_enabled);
        const btnPR = document.getElementById("btnPauseResume");
        btnPR.disabled = !canCtrl;
        btnPR.textContent = d.paused ? "▶ 恢复自动开仓" : "⏸ 暂停开新仓";
        btnPR.className = "btn " + (d.paused ? "green" : "amber");
        document.getElementById("btnResetRisk").disabled = !canCtrl;
        document.getElementById("btnEmergency").disabled = !canCtrl;
        document.getElementById("btnSaveParams").disabled = !canCtrl;
        document.getElementById("ctrlHint").textContent = canCtrl
          ? (d.paused ? "当前已暂停新开仓，已有持仓止损与 TP 仍在守护中" : "正常运行中，所有操作均记审计日志并推微信")
          : "请在 .env 设置 MONITOR_TOKEN 后启用远程操控";

        // Populate params form once (avoid overwriting while user is typing)
        if (!paramsLoaded && d.params) {
          document.getElementById("p_grid_pct").value = d.params.grid_capital_pct;
          document.getElementById("p_trend_pct").value = d.params.trend_capital_pct;
          document.getElementById("p_max_dd").value = d.params.max_drawdown_pct;
          document.getElementById("p_max_daily").value = d.params.max_daily_loss_pct;
          document.getElementById("p_risk_pct").value = d.params.risk_per_trade_pct;
          document.getElementById("p_grid_tp").value = d.params.profit_per_grid_pct;
          document.getElementById("p_bear_depth").value = d.params.bear_exit_depth_pct;
          const sh = document.getElementById("p_allow_short");
          sh.checked = Boolean(d.params.allow_short);
          sh.disabled = d.market === "spot";
          paramsLoaded = true;
        }

        const rk = d.risk || {};
        const n = (v, dft = 0) => Number(v ?? dft);
        const ddPct = n(rk.drawdown_pct ?? rk.current_drawdown_pct);
        const maxDd = n(rk.max_drawdown_pct, 15);
        const expPct = n(rk.exposure_pct);
        const maxDaily = n(rk.daily_loss_pct_max ?? rk.max_daily_loss_pct, 5);
        const consLoss = n(rk.consecutive_losses);
        const pnlCls = n(d.pnl_usd) >= 0 ? "pos" : "neg";
        const cbTxt = rk.circuit_breaker_triggered ? "已触发熔断" : "正常";
        document.getElementById("kpis").innerHTML = [
          { l: "账户实时总净值", v: `$${n(d.equity).toFixed(2)}`, s: `初始 $${n(d.initial_equity).toFixed(2)}`, c: "" },
          { l: "本次运行盈亏", v: `${n(d.pnl_usd) >= 0 ? "+" : ""}$${n(d.pnl_usd).toFixed(2)}`, s: `${n(d.pnl_pct) >= 0 ? "+" : ""}${n(d.pnl_pct).toFixed(2)}%`, c: pnlCls },
          { l: "当前回撤 / 上限", v: `${ddPct.toFixed(2)}%`, s: `上限 ${maxDd.toFixed(1)}%`, c: "warn-txt" },
          { l: "当前敞口 / 日亏上限", v: `${expPct.toFixed(1)}%`, s: `单日亏损上限 ${maxDaily.toFixed(1)}%`, c: "" },
          { l: "风控熔断器", v: cbTxt, s: `连亏 ${consLoss} 笔`, c: rk.circuit_breaker_triggered ? "neg" : "pos" },
          { l: "累计信号 / Ticks", v: `${n(d.signal_count)}`, s: `Ticks: ${n(d.tick_count)}`, c: "" },
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
          const p = n(t.pnl_realized);
          return `
            <tr>
              <td>${(t.entry_time || "").slice(0, 19).replace("T", " ")}</td>
              <td>${t.symbol}</td>
              <td>${t.side}</td>
              <td class="num">${fmtPx(t.entry_price)}</td>
              <td class="num">${fmtPx(t.exit_price)}</td>
              <td class="num">${n(t.quantity).toFixed(2)}</td>
              <td class="num ${p >= 0 ? "pos" : "neg"}">${p >= 0 ? "+" : ""}$${p.toFixed(3)}</td>
              <td>${t.status}</td>
            </tr>
          `;
        }).join("") || `<tr><td colspan="8" style="color:var(--muted)">暂无成交记录</td></tr>`;

        const logs = d.recent_logs || [];
        document.getElementById("logBox").textContent = logs.length ? logs.join("\n") : "暂无日志...";
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
    """Embedded aiohttp.web live monitoring & safe remote control server."""

    def __init__(
        self,
        status_provider: Callable[[], Dict[str, Any]],
        control_handler: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
        params_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        token: Optional[str] = None,
    ):
        self._status_provider = status_provider
        self._control_handler = control_handler
        self._params_handler = params_handler
        self.host = host or os.getenv("MONITOR_HOST", "0.0.0.0")
        self.port = int(port if port is not None else os.getenv("MONITOR_PORT", "8080"))
        self.token = (token if token is not None else os.getenv("MONITOR_TOKEN", "")).strip()

        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/api/status", self._handle_status)
        self._app.router.add_post("/api/control", self._handle_control)
        self._app.router.add_post("/api/params", self._handle_params)
        self._app.router.add_get("/health", self._handle_health)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    def _is_authorized(self, request: web.Request) -> bool:
        if not self.token:
            return True
        q_tok = request.query.get("token", "")
        h_tok = request.headers.get("X-Monitor-Token", "")
        return q_tok == self.token or h_tok == self.token

    def _check_write_access(self, request: web.Request) -> Optional[web.Response]:
        """Enforce strict token protection on all mutating endpoints."""
        if not self.token:
            return web.json_response(
                {"error": "Read-only mode: set MONITOR_TOKEN in .env to enable remote controls"},
                status=403,
            )
        if not self._is_authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return None

    async def _handle_index(self, request: web.Request) -> web.Response:
        if not self._is_authorized(request):
            return web.Response(status=401, text="Unauthorized: append ?token=YOUR_MONITOR_TOKEN")
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")

    async def _handle_status(self, request: web.Request) -> web.Response:
        if not self._is_authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            data = self._status_provider()
            data["controls_enabled"] = bool(self.token and self._control_handler is not None)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"Monitor status error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_control(self, request: web.Request) -> web.Response:
        err = self._check_write_access(request)
        if err is not None:
            return err
        if self._control_handler is None:
            return web.json_response({"error": "control handler not configured"}, status=501)
        try:
            body = await request.json()
            action = str(body.get("action", "")).strip().lower()
            result = await self._control_handler(action, body)
            return web.json_response(result)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            logger.error(f"Monitor control error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_params(self, request: web.Request) -> web.Response:
        err = self._check_write_access(request)
        if err is not None:
            return err
        if self._params_handler is None:
            return web.json_response({"error": "params handler not configured"}, status=501)
        try:
            body = await request.json()
            result = await self._params_handler(body)
            return web.json_response(result)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            logger.error(f"Monitor params error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        auth_note = "token-protected (read+control)" if self.token else "read-only (set MONITOR_TOKEN to enable control)"
        logger.info(f"MonitorWebServer listening on http://{self.host}:{self.port} ({auth_note})")

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
