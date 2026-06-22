"""
================================================================================
BACKTEST VISUALIZATION — Candlestick Charts with Buy/Sell Markers
================================================================================

Extracts the best-performing 100-candle segments from the backtest and
plots professional candlestick charts with trade entry/exit markers.

USAGE:
    python visualize.py
    python visualize.py --symbol BTCUSDT --strategy grid_only
================================================================================
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.dates import DateFormatter, date2num
from datetime import datetime
from typing import Dict, List, Tuple

from data.indicators import compute_all
from strategy.grid_strategy import GridStrategy
from strategy.trend_strategy import TrendStrategy, TrendState
from strategy.regime_detector import MarketRegime, RegimeDetector


# ============================================================================
# Configuration
# ============================================================================

plt.rcParams.update({
    'figure.facecolor': '#1a1a2e',
    'axes.facecolor': '#16213e',
    'axes.edgecolor': '#2a2a4a',
    'axes.labelcolor': '#cccccc',
    'text.color': '#cccccc',
    'xtick.color': '#888888',
    'ytick.color': '#888888',
    'grid.color': '#2a2a4a',
    'grid.alpha': 0.5,
    'figure.dpi': 150,
})

COLORS = {
    'bull': '#00d4aa',
    'bear': '#ff4757',
    'buy': '#00ff88',
    'sell': '#ff4444',
    'tp': '#ffd700',
    'sl': '#ff6b6b',
    'volume_up': '#00d4aa',
    'volume_down': '#ff4757',
    'grid_buy': '#00b4d8',
    'grid_sell': '#ff6b00',
}


# ============================================================================
# Data Loading
# ============================================================================

def load_data(filepath: str = "data/sampled_10k.json") -> dict:
    """Load sampled backtest data."""
    with open(filepath, "r") as f:
        return json.load(f)


# ============================================================================
# Backtest Mini-Runner (Trace Mode)
# ============================================================================

def run_traced_backtest(
    ohlcv: List[Dict],
    strategy_mode: str = "grid_only",
    capital: float = 10000.0,
    lookback: int = 500,
):
    """
    Run a mini backtest that records every trade event for plotting.
    Returns (events, equity_curve, indicators_list).
    """
    from backtest import BacktestConfig, BacktestEngine
    from strategy.signal import SignalAction

    cfg = BacktestConfig(
        symbol="BTCUSDT",
        initial_capital=capital,
        lookback_candles=lookback,
        max_position_pct=25.0,
        confidence_threshold_trend=0.92,
        min_strategy_bars=96,
    )

    engine = BacktestEngine(cfg)
    engine._reset_state()

    events = []  # List of {index, type, price, metadata}
    trade_pairs = []  # List of (entry_idx, exit_idx, pnl)
    active_trade = None

    for i in range(cfg.warmup_candles, len(ohlcv)):
        start = max(0, i - lookback)
        window = ohlcv[start:i + 1]
        bar = ohlcv[i]

        indicators = compute_all(cfg.symbol, window)

        if strategy_mode == "grid_only":
            regime = engine._force_grid_regime(indicators)
        elif strategy_mode == "trend_only":
            regime = engine._force_trend_regime(indicators)
        else:
            regime = engine.regime_detector.detect(
                ohlcv=window, current_regime=engine._current_regime,
                adx=indicators.adx, plus_di=indicators.plus_di,
                minus_di=indicators.minus_di,
                ema_fast=indicators.ema_fast, ema_slow=indicators.ema_slow,
                macd_hist=indicators.macd_hist, atr=indicators.atr,
                volume=indicators.volume,
            )
        engine._current_regime = regime.regime

        # Track grid fills
        prev_trades = len(engine.trades)
        engine._check_grid_fills(bar)
        for t in engine.trades[prev_trades:]:
            entry_idx = max(0, i - 1)
            events.append({
                'index': entry_idx, 'type': 'buy',
                'price': t.entry_price, 'strategy': 'grid',
                'qty': t.quantity, 'pnl': t.pnl,
            })
            events.append({
                'index': i, 'type': 'sell',
                'price': t.exit_price, 'strategy': 'grid',
                'qty': t.quantity, 'pnl': t.pnl,
            })
            trade_pairs.append((entry_idx, i, t.pnl))

        # Track trend entries/exits
        prev_trades2 = len(engine.trades)
        engine._process_tick(bar, indicators, regime, strategy_mode)
        for t in engine.trades[prev_trades2:]:
            events.append({
                'index': i, 'type': 'buy' if t.side == 'BUY' else 'sell',
                'price': t.entry_price if t.side == 'BUY' else t.exit_price,
                'strategy': 'trend',
                'qty': t.quantity, 'pnl': t.pnl,
            })

        # Record equity
        pos_value = engine.position_qty * bar["close"]
        if engine.position_direction == "SHORT":
            equity = engine.cash + engine.short_margin_locked - pos_value
        else:
            equity = engine.cash + pos_value
        engine.equity_curve.append({
            'index': i, 'equity': equity, 'price': bar['close'],
        })

        # Record indicators
        if i >= cfg.warmup_candles:
            pass  # indicators already computed

    return events, engine.equity_curve, engine.trades


# ============================================================================
# Best Segment Finder
# ============================================================================

def find_best_segment(
    equity_curve: List[Dict],
    window: int = 100,
    metric: str = "sharpe",
) -> Tuple[int, int, float]:
    """
    Find the best performing 100-candle window.

    Args:
        equity_curve: List of {index, equity, price} dicts.
        window: Number of candles in the segment.
        metric: "sharpe" or "return".

    Returns:
        (start_index, end_index, score).
    """
    best_score = -float('inf')
    best_start = 0

    for i in range(len(equity_curve) - window):
        segment = equity_curve[i:i + window]
        returns = []
        for j in range(1, len(segment)):
            if segment[j - 1]['equity'] > 0:
                returns.append(
                    segment[j]['equity'] / segment[j - 1]['equity'] - 1
                )

        if len(returns) < 10:
            continue

        if metric == "sharpe":
            mean_ret = sum(returns) / len(returns)
            std_ret = (sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)) ** 0.5
            score = mean_ret / std_ret if std_ret > 0 else 0
        else:
            score = segment[-1]['equity'] / segment[0]['equity'] - 1

        if score > best_score:
            best_score = score
            best_start = i

    return best_start, best_start + window, best_score


# ============================================================================
# Chart Plotting
# ============================================================================

def plot_candlestick_with_trades(
    ohlcv: List[Dict],
    events: List[Dict],
    trades: List,
    segment_start: int,
    segment_end: int,
    title: str,
    save_path: str,
):
    """
    Plot a professional candlestick chart with buy/sell markers and P&L labels.
    Uses integer x-indices for perfect alignment — no overlapping candles.
    """
    segment = ohlcv[segment_start:segment_end]
    n = len(segment)

    # Match trades to events: pair buys with sells, use actual P&L
    trade_pairs = []  # (buy_idx, sell_idx, buy_price, sell_price, pnl, pnl_pct)
    pending_buys = []  # list of {idx, price, qty, pnl} — FIFO queue
    segment_events = []
    seen_indices = set()
    for e in events:
        rel_idx = e['index'] - segment_start
        if rel_idx < 0 or rel_idx >= n:
            continue
        if (rel_idx, e['type'], e.get('price', 0)) in seen_indices:
            continue
        seen_indices.add((rel_idx, e['type'], e.get('price', 0)))
        segment_events.append(e)

        if e['type'] == 'buy':
            pending_buys.append({
                'idx': rel_idx, 'price': e['price'],
                'qty': e.get('qty', 0), 'pnl': e.get('pnl', 0),
            })
        elif e['type'] == 'sell' and pending_buys:
            buy = pending_buys.pop(0)  # FIFO
            pnl = buy['pnl'] if buy['pnl'] != 0 else (
                (e['price'] - buy['price']) * (buy['qty'] if buy['qty'] > 0 else 0.001)
            )
            pnl_pct = (e['price'] / buy['price'] - 1) * 100 if buy['price'] > 0 else 0
            trade_pairs.append((buy['idx'], rel_idx, buy['price'], e['price'], pnl, pnl_pct))

    # Data
    dates = [datetime.fromisoformat(c['timestamp']) for c in segment]
    x = list(range(n))
    opens = [c['open'] for c in segment]
    highs = [c['high'] for c in segment]
    lows = [c['low'] for c in segment]
    closes = [c['close'] for c in segment]
    volumes = [c['volume'] for c in segment]
    colors_candle = [COLORS['bull'] if c >= o else COLORS['bear'] for o, c in zip(opens, closes)]

    # Candle width: 70% of the gap between bars
    body_width = 0.65
    wick_width = 0.4

    # Create figure
    fig = plt.figure(figsize=(22, 11))
    gs = fig.add_gridspec(4, 1, height_ratios=[3.5, 0.8, 1.2, 0.5], hspace=0.06)
    ax_price = fig.add_subplot(gs[0])
    ax_volume = fig.add_subplot(gs[1], sharex=ax_price)
    ax_equity = fig.add_subplot(gs[2])
    ax_info = fig.add_subplot(gs[3])
    ax_info.axis('off')

    # ---- Price: Candlesticks ----
    for i in range(n):
        color = colors_candle[i]
        body_bottom = min(opens[i], closes[i])
        body_height = abs(closes[i] - opens[i])
        body_height = max(body_height, highs[i] * 0.0001)  # minimum visible body

        # Body rectangle
        ax_price.add_patch(plt.Rectangle(
            (i - body_width / 2, body_bottom),
            body_width, body_height,
            facecolor=color, edgecolor='#00000022', linewidth=0.3, alpha=0.95, zorder=2,
        ))
        # Wick line
        ax_price.plot(
            [i, i], [lows[i], highs[i]],
            color=color, linewidth=1.0, alpha=0.85, zorder=1,
        )

    # ---- Buy/Sell markers with P&L labels ----
    buy_labeled = set()
    sell_labeled = set()
    for buy_idx, sell_idx, bp, sp, pnl, pnl_pct in trade_pairs:
        is_win = pnl > 0
        marker_color = COLORS['buy'] if is_win else COLORS['sell']

        # Buy marker (below the candle)
        buy_y = lows[buy_idx] - (highs[buy_idx] - lows[buy_idx]) * 0.15
        if buy_idx not in buy_labeled:
            ax_price.scatter(buy_idx, bp, marker='^', s=140, color=COLORS['buy'],
                           edgecolors='white', linewidth=1.2, zorder=10)
            ax_price.annotate(
                f'${bp:,.0f}', (buy_idx, buy_y),
                fontsize=6.5, color=COLORS['buy'], ha='center', va='top',
                fontweight='bold',
            )
            buy_labeled.add(buy_idx)

        # Sell marker (above the candle) with P&L
        sell_y = highs[sell_idx] + (highs[sell_idx] - lows[sell_idx]) * 0.2
        if sell_idx not in sell_labeled:
            ax_price.scatter(sell_idx, sp, marker='v', s=140, color=marker_color,
                           edgecolors='white', linewidth=1.2, zorder=10)
            pnl_str = f'+${pnl:,.0f} ({pnl_pct:+.2f}%)' if is_win else f'-${abs(pnl):,.0f} ({pnl_pct:+.2f}%)'
            ax_price.annotate(
                pnl_str, (sell_idx, sell_y),
                fontsize=7, color=COLORS['buy'] if is_win else COLORS['sell'],
                ha='center', va='bottom', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#1a1a2e',
                         edgecolor=marker_color, alpha=0.9, linewidth=0.8),
            )
            sell_labeled.add(sell_idx)

        # Dashed connection line
        if buy_idx != sell_idx:
            ax_price.plot([buy_idx, sell_idx], [bp, sp],
                         color=marker_color, linewidth=0.8, linestyle=':',
                         alpha=0.4, zorder=1)

    # ---- Price labels & grid ----
    ax_price.set_ylabel('Price (USDT)', fontsize=9, color='#aaaaaa')
    ax_price.grid(True, alpha=0.25, linewidth=0.5)
    ax_price.set_title(title, fontsize=13, fontweight='bold', color='#ffffff', pad=8)

    # Legend
    legend_elements = [
        mpatches.Patch(color=COLORS['bull'], alpha=0.8, label='Bull Candle'),
        mpatches.Patch(color=COLORS['bear'], alpha=0.8, label='Bear Candle'),
        plt.Line2D([0], [0], marker='^', color='w', markerfacecolor=COLORS['buy'],
                   markersize=10, label='Buy Entry'),
        plt.Line2D([0], [0], marker='v', color='w', markerfacecolor=COLORS['sell'],
                   markersize=10, label='Sell Exit'),
    ]
    ax_price.legend(handles=legend_elements, loc='upper left', framealpha=0.85,
                    facecolor='#1a1a2e', edgecolor='#2a2a4a', labelcolor='#cccccc',
                    fontsize=8, ncol=4)

    # Price range in corner
    price_lo, price_hi = min(lows), max(highs)
    ax_price.text(0.99, 0.01, f'Range: ${price_lo:,.2f} – ${price_hi:,.2f}',
                  transform=ax_price.transAxes, fontsize=7.5, color='#666666', ha='right')

    # ---- Volume ----
    vol_width = body_width * 0.75
    max_vol = max(volumes) if volumes else 1
    for i in range(n):
        vcolor = COLORS['volume_up'] if closes[i] >= opens[i] else COLORS['volume_down']
        alpha = 0.35 + 0.25 * (volumes[i] / max_vol)  # brighter for higher volume
        ax_volume.bar(i, volumes[i], width=vol_width, color=vcolor, alpha=alpha, edgecolor='none')
    ax_volume.set_ylabel('Vol', fontsize=7, color='#777777')
    ax_volume.yaxis.set_tick_params(labelsize=6, colors='#555555')
    ax_volume.grid(True, alpha=0.15)
    ax_volume.set_ylim(0, max_vol * 1.3)

    # ---- Equity curve ----
    initial = closes[0]
    equity_vals = [(c / initial - 1) * 100 for c in closes]
    ax_equity.fill_between(x, 0, equity_vals,
                           where=[v >= 0 for v in equity_vals],
                           color=COLORS['bull'], alpha=0.25, linewidth=0)
    ax_equity.fill_between(x, 0, equity_vals,
                           where=[v < 0 for v in equity_vals],
                           color=COLORS['bear'], alpha=0.25, linewidth=0)
    ax_equity.plot(x, equity_vals, color='#ffffff', linewidth=1.2)
    ax_equity.axhline(y=0, color='#555555', linewidth=0.5, linestyle='--')
    ax_equity.set_ylabel('B&H %', fontsize=7, color='#777777')
    ax_equity.yaxis.set_tick_params(labelsize=6, colors='#555555')
    ax_equity.grid(True, alpha=0.15)
    total_ret = (closes[-1] / initial - 1) * 100
    ax_equity.text(0.99, 0.88, f'B&H return: {total_ret:+.2f}%',
                   transform=ax_equity.transAxes, fontsize=7.5,
                   color='#cccccc', ha='right')

    # ---- X-axis tick labels ----
    tick_step = max(1, n // 12)  # ~12 ticks
    tick_positions = list(range(0, n, tick_step))
    tick_labels = [dates[i].strftime('%m/%d %H:%M') for i in tick_positions]
    for ax in [ax_price, ax_volume, ax_equity]:
        ax.set_xticks(tick_positions)
    ax_equity.set_xticklabels(tick_labels, rotation=30, ha='right', fontsize=7, color='#888888')
    plt.setp(ax_price.get_xticklabels(), visible=False)
    plt.setp(ax_volume.get_xticklabels(), visible=False)

    # ---- Info footer ----
    wins = sum(1 for _, _, _, _, p, _ in trade_pairs if p > 0)
    info = (
        f"Candles: {n}  |  Trades: {len(trade_pairs)}  |  "
        f"Winners: {wins}/{len(trade_pairs)} ({wins/max(1,len(trade_pairs))*100:.0f}%)  |  "
        f"Date: {dates[0].strftime('%Y-%m-%d %H:%M')} → {dates[-1].strftime('%Y-%m-%d %H:%M')}  |  "
        f"Price: ${opens[0]:,.2f} → ${closes[-1]:,.2f}"
    )
    ax_info.text(0.02, 0.5, info, transform=ax_info.transAxes,
                 fontsize=7.5, color='#777777', va='center')

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
    plt.close()
    print(f"  Saved: {save_path}  ({len(trade_pairs)} trades, {wins} wins)")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="OpenQuant Backtest Visualizer")
    parser.add_argument("--symbol", type=str, default="BTCUSDT",
                       help="Trading pair to visualize")
    parser.add_argument("--strategy", type=str, default="grid_only",
                       choices=["grid_only", "trend_only", "regime_switch"],
                       help="Strategy to visualize")
    parser.add_argument("--capital", type=float, default=10000.0,
                       help="Initial capital")
    parser.add_argument("--data", type=str, default="data/sampled_10k.json",
                       help="Path to backtest data file")
    parser.add_argument("--output", type=str, default="charts",
                       help="Output directory for charts")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  OpenQuant Backtest Visualizer")
    print(f"  Symbol: {args.symbol}  |  Strategy: {args.strategy}")
    print(f"{'='*60}\n")

    # Load data
    data = load_data(args.data)
    if args.symbol not in data:
        print(f"Symbol {args.symbol} not found in {args.data}")
        print(f"Available: {list(data.keys())}")
        return
    ohlcv = data[args.symbol]

    # Run traced backtest
    print(f"Running traced backtest on {len(ohlcv)} candles...")
    events, equity_curve, trades = run_traced_backtest(
        ohlcv, strategy_mode=args.strategy, capital=args.capital,
    )
    print(f"  Found {len(events)} trade events, {len(trades)} completed trades")

    # Find best segments
    segments = [
        ("sharpe", "Best Sharpe"),
        ("return", "Best Return"),
    ]

    for metric, label in segments:
        start, end, score = find_best_segment(equity_curve, window=100, metric=metric)
        segment_ohlcv = ohlcv[start:end]

        title = (
            f"{args.symbol} — {args.strategy.replace('_',' ').title()} "
            f"[{label}: {score:.3f}]"
        )
        filename = f"{args.output}/{args.symbol}_{args.strategy}_{metric}.png"

        plot_candlestick_with_trades(
            ohlcv=ohlcv,
            events=events,
            trades=trades,
            segment_start=start,
            segment_end=end,
            title=title,
            save_path=filename,
        )

    print(f"\nDone! Charts saved to: {args.output}/")
    print(f"  {args.symbol}_{args.strategy}_sharpe.png")
    print(f"  {args.symbol}_{args.strategy}_return.png")


if __name__ == "__main__":
    main()
