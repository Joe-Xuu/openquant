# OpenQuant — Algorithmic Trading System

> **Language:** [English](#english) | [中文](#chinese)  
> *Click to switch language / 点击切换语言*

---

<a id="english"></a>
# 🇬🇧 English

## Overview

**OpenQuant** is a modular, high-frequency-resilient algorithmic trading system built from scratch in Python. It uses a **Regime-Switching Model** that dynamically routes between **Grid Trading** (for ranging markets) and **Trend Following** (for trending markets).

> The system doesn't predict the market — it reacts to it. When the market is quiet, it scalps the spread. When the market trends, it rides the momentum.

## Architecture

```
openquant/
├── core/              # State Machine & Double-Entry Ledger (Memory)
├── data/              # Market Data Ingestion & Indicators (Sensors)
├── strategy/          # Multi-Factor Scoring & Signal Generation (Brain)
├── execution/         # Exchange API Routing & Order Management (Hands)
├── risk/              # Independent Risk Guard (Immune System)
├── config/            # JSON Configuration
├── tests/             # Unit Tests (101 passing)
└── main.py            # Event Bus & Main Loop
```

**Hard rule:** The Brain (`strategy/`) NEVER imports the Hands (`execution/`). All communication goes through standardized, immutable `StrategySignal` data classes via the event bus in `main.py`.

## Strategy Explained

### The Core Problem: Regime Detection

Markets alternate between two structural regimes:

| Regime | Characteristics | Optimal Strategy |
|---|---|---|
| 🟢 **Ranging** | Price oscillates in a band, low ADX, suppressed volatility | Grid Trading — buy low, sell high |
| 🔴 **Trending** | Price moves directionally, high ADX, expanding volatility | Trend Following — ride momentum with trailing stops |

Using the wrong strategy in the wrong regime → guaranteed losses. The Regime Detector solves this with a **five-factor scoring model**:

| Factor | Weight | What It Measures |
|---|---|---|
| Trend Strength (ADX) | 30% | Is there a clear directional move? |
| Volatility Regime | 25% | Is volatility expanding or contracting? |
| Momentum (MACD + EMA) | 20% | Are short-term and long-term forces aligned? |
| Volume Profile | 15% | Does volume confirm the price move? |
| Market Microstructure | 10% | Are candles decisive or indecisive? |

Hysteresis thresholds prevent flickering between regimes.

### Strategy A: Grid Trading (Ranging)

Place a ladder of limit orders above and below the reference price. When price oscillates, each round-trip captures a small profit.

```
Price ↑
  $105,000 ── Sell #10
  $104,500 ── Sell #9
      ...
  $100,500 ── Sell #1
  ─────────── Reference ($100,000)
   $99,500 ── Buy #1
      ...
   $95,500 ── Buy #9
   $95,000 ── Buy #10
Price ↓
```

- **Geometric grid:** Constant *percentage* spacing (matches crypto's log-normal distribution)
- **Arithmetic grid:** Constant *dollar* spacing (for stable assets)
- **Dynamic bounds:** ATR-based expansion during high volatility
- **Collision detection:** Prevents buy/sell overlap that would cause instant loss

### Strategy B: Trend Following (Trending)

Enter when EMA crossover aligns with MACD confirmation. Exit on stop-loss, trailing stop, or regime switch.

```
Entry (LONG):  EMA-12 crosses ABOVE EMA-26  +  MACD histogram > 0
Stop-loss:     Entry price − 2× ATR
Trailing stop: Follows price at 3× ATR distance (ratchets up only)
Exit:          Stop hit | EMA cross reversed | Regime switched to RANGING
```

Position sizing uses Kelly-style volatility adjustment: risk a fixed % of capital per trade, scale position size by stop distance.

## Data Flow

```
WebSocket → MarketDataEngine → OHLCV → Indicators (EMA/MACD/ADX/ATR/RSI/BB)
   → RegimeDetector (score 0–1)
   → GridStrategy or TrendStrategy → StrategySignal
   → RiskGuard (6 checks) → APPROVED / BLOCKED / MODIFIED
   → OrderManager → Binance API
   → LedgerEngine (double-entry bookkeeping)
```

## Safety: Three Lines of Defense

| Layer | What It Does |
|---|---|
| **Strategy** | Computes signals only — never touches API or database |
| **Risk Guard** | 6 pre-execution checks: drawdown, daily loss, position size, exposure, dust orders, circuit breaker |
| **Circuit Breaker** | Pauses trading after N consecutive losses or volatility spikes |

## Double-Entry Ledger

The system maintains its own SQLite-based accounting ledger with full double-entry bookkeeping:

- Every trade produces at least two journal lines (one debit, one credit)
- WAL mode + explicit locks for concurrent read/write safety
- **Never trusts exchange API responses** for internal state — the ledger is always the source of truth
- Immutable audit trail (corrections are reversing entries, never UPDATEs)

## Tech Stack

- **Language:** Python 3.10+
- **Data:** pandas, numpy
- **Indicators:** Self-contained computation (no external TA library dependency)
- **Database:** SQLite3 with WAL journaling
- **Exchange:** Binance REST API & WebSocket (aiohttp)
- **Testing:** pytest (101 tests, 0 failures)

## Quick Start

```bash
# Clone
git clone https://github.com/Joe-Xuu/openquant.git
cd openquant

# Virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install
pip install -r requirements.txt

# Unit tests
python -m pytest tests/ -v
```

### Backtest
```bash
# Download historical data
python -c "
import requests, json
# See backtest.py for full data-fetching logic
"

# Run backtest (3 symbols × 3 strategies ≈ 5 min)
python backtest.py
```

### Visualization
```bash
# Generate candlestick charts with buy/sell markers
python visualize.py --symbol BTCUSDT --strategy grid_only
# Charts saved to charts/ directory
```

### Live Trading
```bash
# Configure API keys
cp .env.example .env
# Edit .env with your Binance API keys

# Run live (use testnet first!)
python main.py
```

---

<a id="chinese"></a>
# 🇨🇳 中文

## 概述

**OpenQuant** 是一个用 Python 从零构建的模块化、抗高频行情的算法交易系统。它采用**状态切换模型**，在**网格交易**（震荡市）和**趋势跟随**（趋势市）之间动态切换。

> 系统不预测市场——它对市场做出反应。市场安静时蚕食差价，市场暴走时顺势而为。

## 系统架构

```
openquant/
├── core/              # 状态机 & 复式记账账本（记忆）
├── data/              # 行情数据摄取 & 技术指标（传感器）
├── strategy/          # 多因子评分 & 信号生成（大脑）
├── execution/         # 交易所 API 路由 & 订单管理（双手）
├── risk/              # 独立风险守卫（免疫系统）
├── config/            # JSON 配置
├── tests/             # 单元测试（101 项全部通过）
└── main.py            # 事件总线 & 主循环
```

**铁律：** 大脑（`strategy/`）绝不导入双手（`execution/`）。所有通信通过标准化、不可变的 `StrategySignal` 数据类，经由 `main.py` 中的事件总线传递。

## 策略详解

### 核心问题：市场状态识别

市场在两种结构性状态之间交替：

| 状态 | 特征 | 最优策略 |
|---|---|---|
| 🟢 **震荡市** | 价格在区间内波动，ADX 低，波动率被抑制 | 网格交易 — 低买高卖 |
| 🔴 **趋势市** | 价格持续单向运动，ADX 高，波动率扩张 | 趋势跟随 — 顺势操作，跟踪止盈 |

在错误的市场用错误的策略 → 必然亏损。状态检测器通过**五因子评分模型**解决这个问题：

| 因子 | 权重 | 衡量内容 |
|---|---|---|
| 趋势强度 (ADX) | 30% | 是否存在明确的定向运动？ |
| 波动率状态 | 25% | 波动率在扩张还是收缩？ |
| 动量 (MACD + EMA) | 20% | 短期和长期力量是否一致？ |
| 成交量画像 | 15% | 成交量是否确认价格走势？ |
| 市场微观结构 | 10% | K 线是否果断（实体大影线小）？ |

磁滞阈值防止在两种状态之间反复跳转。

### 策略 A：网格交易（震荡市）

在参考价格上下各挂一排限价单。价格来回震荡时，每一轮买卖赚取小额利润。

```
价格 ↑
  ¥105,000 ── 卖单#10
  ¥104,500 ── 卖单#9
      ...
  ¥100,500 ── 卖单#1
  ─────────── 参考价 (¥100,000)
   ¥99,500 ── 买单#1
      ...
   ¥95,500 ── 买单#9
   ¥95,000 ── 买单#10
价格 ↓
```

- **等比网格：** 固定*百分比*间距（匹配加密货币的对数正态分布）
- **等差网格：** 固定*金额*间距（适合价格稳定的资产）
- **动态区间：** 高波动率时基于 ATR 自动扩大区间
- **重叠检测：** 防止买卖价格交叉导致瞬间亏损

### 策略 B：趋势跟随（趋势市）

当 EMA 交叉与 MACD 确认一致时入场。止损、跟踪止损或状态切换时离场。

```
入场（做多）：EMA-12 上穿 EMA-26  +  MACD 柱 > 0
止损：       入场价 − 2× ATR
跟踪止损：   保持 3× ATR 距离跟随价格上涨（只上移不下移）
离场：       触及止损 | EMA 反转 | 市场切换回震荡
```

头寸规模采用凯利风格的波动率调整：每笔交易承担固定比例的资金风险，根据止损距离反推仓位大小。

## 数据流

```
WebSocket → MarketDataEngine → OHLCV → 指标计算 (EMA/MACD/ADX/ATR/RSI/BB)
   → 状态检测器 (评分 0–1)
   → 网格策略 或 趋势策略 → StrategySignal
   → 风险守卫 (6 项检查) → 通过 / 拦截 / 修改
   → 订单管理器 → Binance API
   → 复式账本 (双重记账)
```

## 安全防线：三道关卡

| 层级 | 职责 |
|---|---|
| **策略层** | 只计算信号 — 绝不接触 API 或数据库 |
| **风险守卫** | 6 项执行前检查：回撤、日内亏损、头寸规模、敞口、粉尘订单、熔断 |
| **熔断器** | 连续亏损 N 笔或波动率飙升 → 暂停交易 |

## 复式记账

系统维护自己的基于 SQLite 的会计账本，实现完整的复式记账：

- 每笔交易至少产生两条分录（一借一贷）
- WAL 模式 + 显式锁保证并发读写安全
- **绝不信任交易所 API 返回值**作为内部状态 — 账本始终是唯一真相来源
- 不可变审计追溯（修正通过冲销分录，绝不修改原记录）

## 技术栈

- **语言：** Python 3.10+
- **数据处理：** pandas, numpy
- **技术指标：** 自包含计算（不依赖外部 TA 库）
- **数据库：** SQLite3 + WAL 日志模式
- **交易所：** Binance REST API & WebSocket (aiohttp)
- **测试：** pytest（101 项测试，0 失败）

## 快速开始

```bash
# 克隆
git clone https://github.com/Joe-Xuu/openquant.git
cd openquant

# 虚拟环境
python3 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 运行测试
python -m pytest tests/ -v
```

### 回测
```bash
# 运行回测（3 个标的 × 3 种策略 ≈ 5 分钟）
python backtest.py
```

### 可视化
```bash
# 生成带买卖标记的 K 线图
python visualize.py --symbol BTCUSDT --strategy grid_only
# 图表保存到 charts/ 目录
```

### 实盘交易
```bash
# 配置 API 密钥
cp .env.example .env
# 编辑 .env 填入你的 Binance API 密钥

# 启动（先用模拟盘！）
python main.py
```

---

## 📝 License

MIT — see [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This software is for **educational and research purposes only**. Trading cryptocurrencies involves substantial risk of loss and is not suitable for all investors. Past performance is not indicative of future results. The authors assume no responsibility for any financial losses incurred through the use of this software.

**本软件仅供学习和研究使用。加密货币交易存在重大亏损风险，并不适合所有投资者。历史表现不代表未来结果。作者对使用本软件产生的任何财务损失不承担责任。**
