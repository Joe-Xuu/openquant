# OpenQuant — Algorithmic Trading System

> **Language:** [English](#english) | [中文](#chinese)  
> *Click to switch language / 点击切换语言*

---

<a id="english"></a>
# 🇬🇧 English

## Overview

**OpenQuant** is a modular algorithmic trading system built in Python. It runs a **parallel dual-engine architecture**: **Grid Trading** (always on, scalping micro-oscillations) and **Trend Following** (opportunistic, capturing macro moves). Both engines operate independently — the grid never stops, and trend trades overlay on top when a strong signal fires.

> The system doesn't predict the market — it reacts to it. Grid grinds. Trend hunts.

## Architecture

```
openquant/
├── core/              # State Machine & Double-Entry Ledger (PostgreSQL)
├── data/              # Market Data Ingestion (WebSocket) & Indicators
├── strategy/          # Grid Strategy, Trend Strategy, Regime Detector
├── execution/         # Exchange API Routing & Order Management
├── risk/              # Independent Risk Guard
├── config/            # JSON Configuration
├── tests/             # Unit Tests (100+ passing)
├── main.py            # Event Bus & Main Loop
├── backtest.py        # Historical Backtesting Engine
├── dashboard.py       # Real-Time Trading Dashboard
└── visualize.py       # Candlestick Chart Generator
```

**Hard rule:** The Brain (`strategy/`) NEVER imports the Hands (`execution/`). All communication goes through standardized, immutable `StrategySignal` data classes via the event bus in `main.py`.

## Project Status

| Feature | Status |
|---|---|
| Parallel Grid+Trend Engine | ✅ Live |
| Tracking Grid (price-following) | ✅ Live |
| Pyramid Capital Allocation | ✅ Live |
| Volatility-Adaptive Levels | ✅ Live |
| Grid Stop-Loss | ✅ Live |
| Unidirectional Deployment | ✅ Live |
| Double-Entry Ledger (PostgreSQL) | ✅ Live |
| Time-Sync (auto server clock) | ✅ Live |
| Risk Guard (6 checks) | ✅ Complete |
| Binance Spot Live Trading | ✅ **LIVE — DOGEUSDT** |
| Backtesting (dual engine) | ✅ Complete |
| Real-Time Web Dashboard | ✅ Complete |
| 100+ Unit Tests | ✅ Passing |

## Strategy Explained

### Dual Engine — Parallel, Not Mutually Exclusive

**Old architecture (regime-switching):** Detect if market is "trending" or "ranging" → pick ONE strategy.  
**New architecture (parallel):** Grid runs 100% of the time. Trend independently monitors for strong signals and fires when conditions align. **They don't fight — they coexist.**

```
                    ┌─────────────────┐
                    │   K-line Data    │
                    │   (WebSocket)    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   Indicators     │
                    │ EMA/MACD/ADX/ATR│
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │                             │
     ┌────────▼────────┐          ┌─────────▼────────┐
     │   GRID ENGINE    │          │  TREND ENGINE    │
     │   Always Running │          │  Opportunistic   │
     │   10-layer Trail  │          │  EMA Cross+MACD  │
     └────────┬────────┘          └─────────┬────────┘
              │                             │
              │     ┌───────────────┐       │
              └─────►   Risk Guard  ◄───────┘
                    │   6 Checks     │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │  Order Manager │
                    │  → Binance API │
                    └───────────────┘
```

### Strategy A: Grid Trading (Always On)

Place a ladder of limit buy orders below the current price. When price dips, buy. Automatically place a take-profit sell above entry. Round-trip captures 0.3% minus fees.

| Parameter | Value | Meaning |
|---|:--:|---|
| Levels | 3~5 per side | Capped by capital (min $1/level) |
| Range | ±3% | Grid covers 3% above/below current price |
| Profit | 0.3%/level | Target profit per round-trip |
| Rebalance | Drift > 1% | Auto-recenter around new price |

**Key innovations over vanilla grid:**

1. **Tracking reference price:** Grid always centers on the latest close, not a stale SMA. As price drifts, the grid follows — keeping buy levels close to market.

2. **Pyramid capital allocation:** Levels closest to the current price get more capital (exponential decay weight). Why? Higher fill probability.

   ```
   Capital per level (pyramid decay=0.5):
   Buy #1 (closest):   ~$2.70   57% of side capital
   Buy #2:             ~$1.35   29%
   Buy #3 (farthest):  ~$0.68   14% → skipped if < $1 min
   ```

3. **Volatility-adaptive density:** More levels in low-volatility ranging markets; fewer in high-volatility trending markets.

4. **Grid stop-loss:** If price drops 3% below the lowest buy level, emergency-close the entire grid — prevents catching a falling knife.

5. **Unidirectional deployment:** Checks actual account balances before placing orders. No DOGE? Skip sell levels. No USDT? Skip buy levels. No more "insufficient balance" error storms.

### Strategy B: Trend Following (Opportunistic)

Evaluated independently every tick. Only fires on true ENTRY or EXIT signals — never overrides the grid.

| Signal | Condition |
|------|------|
| LONG Entry | EMA12 crosses ABOVE EMA26 + MACD histogram > 0 + market not flat |
| SHORT Entry | EMA12 crosses BELOW EMA26 + MACD histogram < 0 + market not flat |
| Stop-Loss | Entry price ± 2× ATR |
| Trailing Stop | Ratchets at 3× ATR distance in favorable direction |
| Position Size | Kelly-style: risk 1% of capital per trade |
| Cooldown | First 30 ticks (~2.5h) after startup: no trend trades |

### Regime Detector (Informational)

The five-factor scoring model still runs, but it no longer MUTES the grid. Its output is used to:
- Tune grid density (more levels when ranging)
- Gate trend entries (higher confidence required in ambiguous markets)
- Provide diagnostic insight

| Factor | Weight | What It Measures |
|---|---|---|
| Trend Strength (ADX) | 30% | Is there a clear directional move? |
| Volatility Regime | 25% | Is volatility expanding or contracting? |
| Momentum (MACD + EMA) | 20% | Are short-term and long-term forces aligned? |
| Volume Profile | 15% | Does volume confirm the price move? |
| Market Microstructure | 10% | Are candles decisive or indecisive? |

Hysteresis: score > 0.70 → classify as trending; score < 0.30 → classify as ranging; otherwise hold previous.

---

## Trade Lifecycle: From Signal to Settlement

Here's exactly what happens when a grid level executes:

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: GRID DEPLOYMENT                                    │
│  _process_symbol_tick (every 5 min)                         │
│  → compute_grid(reference_price=close, atr, is_ranging)     │
│  → generate_signal → dispatch                               │
│  → BUY LIMIT 20 DOGE @ 0.0792 → 挂在 Binance               │
│  → 账户 USDT 被锁定 $1.58                                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: EXCHANGE FILL (Binance)                            │
│  价格跌到 0.0792 → 撮合成交                                  │
│  → 账户多了 19.98 DOGE（扣 0.1% 手续费后）                    │
│  → USDT 锁定释放                                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼  (最多 30 秒后)
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: RECONCILIATION DETECTS FILL                        │
│  _reconciliation_loop (every 30s)                           │
│  → GET /myTrades?symbol=DOGEUSDT&limit=50                   │
│  → 发现新成交: BUY 20 DOGE @ 0.0792                          │
│  → 日志: "Fill: DOGEUSDT BUY 20 @ $0.0792"                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼  (同一轮 reconciliation)
┌─────────────────────────────────────────────────────────────┐
│  STEP 4: AUTO PLACE TAKE-PROFIT                             │
│  → 计算止盈价: 0.0792 × 1.003 = 0.07944                     │
│  → 查 DOGE 实际余额: 19.98 → 取整到整数: 19 DOGE             │
│  → 去重检查: 这个 fill ID 是否已处理过？→ 否                  │
│  → SELL LIMIT 19 DOGE @ 0.07944 → 挂在 Binance              │
│  → 记录到 _tp_attempted（绝不复重）                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  STEP 5: TP FILL (Binance)                                  │
│  价格涨到 0.07944 → 撮合成交                                 │
│  → 卖出 19 DOGE → 拿回 ~$1.51 USDT                           │
│  → 净赚: ~0.3% − 0.2% 手续费 = ~$0.002                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼  (最多 30 秒后)
┌─────────────────────────────────────────────────────────────┐
│  STEP 6: RECONCILIATION DETECTS TP FILL                     │
│  → 发现 SELL 成交, 记录到账本                                  │
│  → 无新动作（卖完即止）                                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼  (下次 tick)
┌─────────────────────────────────────────────────────────────┐
│  STEP 7: GRID CONTINUES                                     │
│  _process_symbol_tick (每 5 分钟)                             │
│  → 检查价格偏离是否 > 1%                                      │
│  → 没偏离: 买单还在，什么都不做                                │
│  → 偏离了: 取消旧单，围绕新价格重建网格                         │
│  → 账户 USDT 够 → 新买单自动补上                              │
└─────────────────────────────────────────────────────────────┘
```

**Key timing:**
- Fill → system awareness: **0–30 seconds** (reconciliation poll interval)
- Fill → TP placed: **0–30 seconds** (same reconciliation cycle)
- The system is **poll-based**, not event-driven. Reconciliation bridges the gap.

---

## Data Flow

```
WebSocket → MarketDataEngine → OHLCV → Indicators (EMA/MACD/ADX/ATR/RSI/BB)
   → GridStrategy: always evaluates, may rebalance
   → TrendStrategy: independently evaluates, fires on strong ENTRY/EXIT
   → RegimeDetector: scores market (ranging/trending) for parameter tuning
   → RiskGuard (6 checks) → APPROVED / BLOCKED / MODIFIED
   → OrderManager → Binance API
   → LedgerEngine (PostgreSQL double-entry bookkeeping)
```

## Safety

| Layer | What It Does |
|---|---|
| **Strategy** | Computes signals only — never touches API or database |
| **Risk Guard** | 6 pre-execution checks: drawdown, daily loss, position size, exposure, dust orders, circuit breaker |
| **Grid Stop-Loss** | Price drops 3% below grid floor → emergency close all |
| **Time Sync** | Auto-syncs Binance server time at startup; auto-corrects on recvWindow errors |
| **Watchdog** | Independent async task: warns if main loop stalls > 2 tick intervals |
| **Per-Tick Timeout** | 30-second hard limit per tick; hung ticks are skipped |

## Double-Entry Ledger (PostgreSQL)

- Thread-safe connection pool (psycopg2, 2–10 connections)
- SERIALIZABLE isolation for ACID compliance
- Every trade: at least two journal lines (one debit, one credit)
- Immutable audit trail (corrections via reversing entries, never UPDATEs)
- `NUMERIC(20,8)` — no floating-point rounding errors in accounting

## Tech Stack

- **Language:** Python 3.10+
- **Data:** pandas, numpy, pandas-ta
- **Database:** PostgreSQL 16 (Docker or native)
- **Exchange:** Binance REST API & WebSocket (aiohttp)
- **Testing:** pytest (100+ tests)
- **Dashboard:** HTTP server on port 8080

## Quick Start

```bash
# Clone
git clone https://github.com/Joe-Xuu/openquant.git
cd openquant

# Virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install
pip install -r requirements.txt

# Database (Docker)
docker-compose up -d   # starts PostgreSQL 16

# Configure
cp .env.example .env
# Edit .env with your Binance API keys

# Unit tests
python -m pytest tests/ -v

# Run
python main.py
```

### Live Trading
```bash
# 1. Set BINANCE_TESTNET=false in .env
# 2. Fill in BINANCE_API_KEY and BINANCE_API_SECRET
# 3. Whitelist your IP in Binance API Management

python main.py
```

### Dashboard
```bash
# Real-time dashboard at http://localhost:8080
python dashboard.py
```

### Backtest
```bash
# Run backtest on historical data
python backtest.py --symbol DOGEUSDT --capital 12
```

### Visualization
```bash
# Generate candlestick charts with trade markers
python visualize.py
```

---

<a id="chinese"></a>
# 🇨🇳 中文

## 概述

**OpenQuant** 是一个用 Python 构建的模块化算法交易系统。采用**并行双引擎架构**：**网格交易**（常驻运行，蚕食微幅震荡）和**趋势跟随**（伺机而动，捕获宏观波段）。两个引擎独立并行——网格永不停，趋势信号强时叠加介入。

> 系统不预测市场——它对市场做出反应。网格磨小利，趋势抓大波。

## 系统架构

```
openquant/
├── core/              # 状态机 & 复式记账账本 (PostgreSQL)
├── data/              # 行情数据 (WebSocket) & 技术指标
├── strategy/          # 网格策略、趋势策略、状态检测器
├── execution/         # 交易所 API 路由 & 订单管理
├── risk/              # 独立风险守卫
├── config/            # JSON 配置
├── tests/             # 单元测试 (100+ 通过)
├── main.py            # 事件总线 & 主循环
├── backtest.py        # 历史回测引擎
├── dashboard.py       # 实时交易看板
└── visualize.py       # K线图生成器
```

**铁律：** 大脑（`strategy/`）绝不导入双手（`execution/`）。所有通信通过标准化不可变的 `StrategySignal` 数据类，经由 `main.py` 中的事件总线传递。

## 项目状态

| 功能 | 状态 |
|---|---|
| 并行网格+趋势引擎 | ✅ 实盘运行 |
| 追踪网格（跟随市价） | ✅ 实盘运行 |
| 金字塔资金分配 | ✅ 实盘运行 |
| 波动率自适应层数 | ✅ 实盘运行 |
| 网格总止损 | ✅ 实盘运行 |
| 单向部署 | ✅ 实盘运行 |
| 复式记账 (PostgreSQL) | ✅ 实盘运行 |
| 自动对时 | ✅ 实盘运行 |
| 风险守卫 (6项检查) | ✅ 完成 |
| 币安现货实盘 | ✅ **实盘 — DOGEUSDT** |
| 回测 (双引擎) | ✅ 完成 |
| 实时网页看板 | ✅ 完成 |
| 100+ 单元测试 | ✅ 通过 |

## 策略详解

### 双引擎并行 — 不互斥

**旧架构（状态切换）：** 检测趋势/震荡 → 选一个策略跑。  
**新架构（并行）：** 网格 100% 时间在跑。趋势独立监控，只在强信号时出手。**两者不打架，共存。**

```
                    ┌─────────────────┐
                    │    K线数据 (WS)   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   技术指标计算    │
                    │ EMA/MACD/ADX/ATR│
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │                             │
     ┌────────▼────────┐          ┌─────────▼────────┐
     │   网格引擎       │          │   趋势引擎        │
     │   永远在线       │          │   伺机而动        │
     │   追踪网格       │          │   EMA交叉+MACD   │
     └────────┬────────┘          └─────────┬────────┘
              │                             │
              │     ┌───────────────┐       │
              └─────►   风险守卫     ◄───────┘
                    │   6项检查      │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │   订单管理器   │
                    │   → 币安 API  │
                    └───────────────┘
```

### 策略 A：网格交易（常驻运行）

在当前价下方挂一排限价买单。价格跌到就吃进，自动在上方挂止盈卖单。一轮买卖吃 0.3% 差价。

| 参数 | 值 | 含义 |
|---|:--:|---|
| 层数 | 3~5/边 | 按资本自动封顶（每层≥$1） |
| 范围 | ±3% | 网格覆盖当前价上下 3% |
| 利润 | 0.3%/层 | 每笔目标利润 |
| 重平衡 | 偏离 >1% | 自动围绕新价格重建 |

**相比传统网格的创新：**

1. **追踪参考价：** 网格始终以最新收盘价为中心，而非过时的均线。价格漂移，网格跟随——买单始终紧贴市价。

2. **金字塔资金分配：** 离市价越近的层分配越多资金（指数衰减权重），因为成交概率更高。

3. **波动率自适应密度：** 低波动横盘时加密层数；高波动趋势时减疏层数。

4. **网格总止损：** 价格跌破网格下界 3% → 紧急清仓，防止接飞刀。

5. **单向部署：** 下单前检查实际余额。没 DOGE 不挂卖单，没 USDT 不挂买单。消灭 "insufficient balance" 错误洪流。

### 策略 B：趋势跟随（伺机而动）

每 tick 独立评估。只在真正的入场/离场信号时出手——绝不压制网格。

| 信号 | 条件 |
|------|------|
| 做多 | EMA12 上穿 EMA26 + MACD柱>0 + 非横盘 |
| 做空 | EMA12 下穿 EMA26 + MACD柱<0 + 非横盘 |
| 止损 | 入价 ± 2×ATR |
| 移动止盈 | 有利方向 3×ATR 追踪（只进不退） |
| 仓位 | Kelly风格：每笔冒 1% 本金风险 |
| 冷却期 | 启动后前 30 tick(~2.5h)：趋势不交易 |

### 状态检测器（信息参考）

五因子评分模型仍在运行，但不再压制网格。输出用于：
- 调整网格密度（震荡市加密）
- 控制趋势入场门槛（模糊市提高要求）
- 提供诊断参考

| 因子 | 权重 | 衡量内容 |
|---|---|---|
| 趋势强度 (ADX) | 30% | 是否存在明确方向运动？ |
| 波动率状态 | 25% | 波动率在扩张还是收缩？ |
| 动量 (MACD + EMA) | 20% | 短期和长期力量是否一致？ |
| 成交量画像 | 15% | 成交量是否确认价格走势？ |
| 市场微观结构 | 10% | K线实体/影线比是否果断？ |

迟滞门：score > 0.70 → 趋势；score < 0.30 → 震荡；中间 → 维持。

---

## 交易生命周期：从信号到结算

```
┌──────────────────────────────────────────────────┐
│  第1步：网格部署                                    │
│  _process_symbol_tick (每 5 分钟)                   │
│  → compute_grid(参考价=收盘价)                      │
│  → BUY LIMIT 20 DOGE @ 0.0792 → 挂在币安           │
│  → 账户 USDT 被锁定                                 │
└──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│  第2步：币安成交                                    │
│  价格跌到 0.0792 → 撮合成交                         │
│  → 账户多了 19.98 DOGE（扣 0.1% 手续费）             │
└──────────────────────────────────────────────────┘
                         │
                         ▼  (最多 30 秒)
┌──────────────────────────────────────────────────┐
│  第3步：对账发现成交                                 │
│  _reconciliation_loop (每 30 秒)                   │
│  → GET /myTrades → 发现新成交                       │
│  → 日志: "Fill: DOGEUSDT BUY 20 @ $0.0792"       │
└──────────────────────────────────────────────────┘
                         │
                         ▼  (同一轮对账)
┌──────────────────────────────────────────────────┐
│  第4步：自动挂止盈                                  │
│  → 止盈价 = 0.0792 × 1.003 = 0.07944              │
│  → 查 DOGE 实际余额: 19.98 → 取整: 19 DOGE         │
│  → 去重检查: 此成交ID是否已处理？→ 否                │
│  → SELL LIMIT 19 DOGE @ 0.07944 → 挂在币安         │
│  → 记入 _tp_attempted (绝不复重)                    │
└──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│  第5步：止盈成交                                    │
│  价格涨到 0.07944 → 撮合成交                        │
│  → 卖出 19 DOGE → 拿回 USDT                        │
│  → 净赚: ~0.3% − 0.2% 手续费                       │
└──────────────────────────────────────────────────┘
                         │
                         ▼  (最多 30 秒)
┌──────────────────────────────────────────────────┐
│  第6步：对账确认                                    │
│  → 发现 SELL 成交，记录到账本                        │
│  → 无新动作（卖完即止）                             │
└──────────────────────────────────────────────────┘
                         │
                         ▼  (下次 tick)
┌──────────────────────────────────────────────────┐
│  第7步：网格继续运转                                 │
│  _process_symbol_tick (每 5 分钟)                   │
│  → 检查价格偏离 >1%？→ 否：什么都不做                │
│  → 是：取消旧单，围绕新价格重建网格                   │
│  → USDT 够 → 新买单自动补上                         │
└──────────────────────────────────────────────────┘
```

**关键时间：**
- 成交 → 系统感知：**0~30 秒**（对账轮询间隔）
- 成交 → 止盈挂上：**0~30 秒**（同一轮对账完成）
- 系统是**轮询制**，非事件驱动。对账是感知成交的唯一途径。

---

## 安全防线

| 层级 | 职责 |
|---|---|
| **策略层** | 只计算信号 — 绝不接触 API 或数据库 |
| **风险守卫** | 6 项执行前检查：回撤、日内亏损、头寸规模、敞口、粉尘订单、熔断 |
| **网格止损** | 价格跌破网格下界 3% → 紧急全平 |
| **自动对时** | 启动时与币安服务器同步时钟；recvWindow 错误自动纠正 |
| **看门狗** | 独立异步任务：主循环停滞超 2 个 tick 间隔 → 报警 |
| **逐 tick 超时** | 每个 tick 硬限 30 秒，超时跳过不阻塞 |

## 复式记账 (PostgreSQL)

- 线程安全连接池（psycopg2，2~10 连接）
- SERIALIZABLE 隔离级别，ACID 合规
- 每笔交易至少两条分录（一借一贷）
- 不可变审计追溯（修正通过冲销分录，绝不 UPDATE）
- `NUMERIC(20,8)` — 杜绝浮点舍入误差

## 技术栈

- **语言：** Python 3.10+
- **数据处理：** pandas, numpy, pandas-ta
- **数据库：** PostgreSQL 16 (Docker 或原生)
- **交易所：** Binance REST API & WebSocket (aiohttp)
- **测试：** pytest (100+ 项)
- **看板：** HTTP 服务，端口 8080

## 快速开始

```bash
# 克隆
git clone https://github.com/Joe-Xuu/openquant.git
cd openquant

# 虚拟环境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 数据库 (Docker)
docker-compose up -d   # 启动 PostgreSQL 16

# 配置
cp .env.example .env
# 编辑 .env 填入币安 API 密钥

# 测试
python -m pytest tests/ -v

# 启动
python main.py
```

### 实盘交易
```bash
# 1. .env 中设置 BINANCE_TESTNET=false
# 2. 填入 BINANCE_API_KEY 和 BINANCE_API_SECRET
# 3. 去币安 API 管理页面把 IP 加入白名单

python main.py
```

### 看板
```bash
python dashboard.py   # http://localhost:8080
```

### 回测
```bash
python backtest.py --symbol DOGEUSDT --capital 12
```

### 可视化
```bash
python visualize.py   # K线图保存到 charts/ 目录
```

---

## 📝 License

MIT — see [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This software is for **educational and research purposes only**. Trading cryptocurrencies involves substantial risk of loss and is not suitable for all investors. Past performance is not indicative of future results. The authors assume no responsibility for any financial losses incurred through the use of this software.

**本软件仅供学习和研究使用。加密货币交易存在重大亏损风险，并不适合所有投资者。历史表现不代表未来结果。作者对使用本软件产生的任何财务损失不承担责任。**
