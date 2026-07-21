#!/usr/bin/env python3
r"""
OpenQuant — 交易历史分析工具 / Trade Analytics
纯 Tkinter 桌面应用，matplotlib 绑图

运行: .venv\Scripts\python analytics.py
"""

import re
import os
import sys
import tkinter as tk
from datetime import datetime, timezone
from tkinter import messagebox, ttk, filedialog

import matplotlib

matplotlib.use("TkAgg")
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from tkcalendar import Calendar

# ── CJK font setup ──
_cjk_font = None
for _f in fm.fontManager.ttflist:
    if "Microsoft YaHei" in _f.name:
        _cjk_font = _f.name
        break
if not _cjk_font:
    for _f in fm.fontManager.ttflist:
        if "SimHei" in _f.name:
            _cjk_font = _f.name
            break
if not _cjk_font:
    for _f in fm.fontManager.ttflist:
        if "SimSun" in _f.name:
            _cjk_font = _f.name
            break
if _cjk_font:
    plt.rcParams["font.sans-serif"] = [_cjk_font, "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv

load_dotenv()

from core.local_ledger import get_ledger

# ---------------------------------------------------------------------------
# Data Layer
# ---------------------------------------------------------------------------


def fetch_data():
    """Load all trade data from PostgreSQL. Returns (initial_equity, grid_fills_df, trades_df)."""
    ledger = get_ledger()

    # Initial equity
    initial_equity_str = ledger.get_metadata("initial_equity")
    try:
        initial_equity = float(initial_equity_str) if initial_equity_str else 0.0
    except (ValueError, TypeError):
        initial_equity = 0.0
    if initial_equity <= 0:
        initial_equity = ledger.get_account_balance("EQUITY-INITIAL")

    # Grid fills
    gf_rows = ledger.get_all_grid_fills(limit=50000)
    grid_fills = pd.DataFrame(gf_rows) if gf_rows else pd.DataFrame()

    # Closed trend trades
    trades = ledger.get_trade_history(status="CLOSED", limit=50000)
    trades_data = []
    for t in trades:
        trades_data.append(
            {
                "trade_id": t.trade_id,
                "symbol": t.symbol,
                "side": t.side,
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "entry_price": t.entry_price_avg,
                "exit_price": t.exit_price_avg,
                "quantity": t.quantity,
                "pnl_realized": t.pnl_realized,
                "pnl_pct": t.pnl_realized_pct,
                "fee_total": t.fee_total,
            }
        )
    trades_df = pd.DataFrame(trades_data) if trades_data else pd.DataFrame()

    ledger.close()
    return initial_equity, grid_fills, trades_df


def parse_date_flex(s: str) -> str:
    """
    Parse a human-entered date string into YYYY-MM-DD.
    Supports: 2026/7/17, 7/17, 7/17/2026, 7.17, 7.17.2026, 2026.7.17,
    2026-07-01, 20260701, July 17 2026, etc.
    If no year given, defaults to current year.
    Raises ValueError if unrecognized.
    """
    s = s.strip()
    if not s:
        return ""

    # Already canonical
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # 20260701 (compact)
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"

    # Normalize separators: / or . or - → /
    # Split on common separators, but only if not ambiguous
    parts = re.split(r"[/.\-]", s)
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(f"无法识别日期: '{s}'")

    # Try to parse each part
    nums = []
    for p in parts:
        try:
            nums.append(int(p))
        except ValueError:
            raise ValueError(f"无法识别日期: '{s}'")

    if len(nums) == 2:
        # m/d or m/d/y with implicit year
        mo, d = nums
        y = datetime.now().year
        return f"{y:04d}-{mo:02d}-{d:02d}"

    # 3 parts: could be y/m/d or m/d/y
    a, b, c = nums
    if a > 31:
        # y/m/d (year first, e.g. 2026/7/17 or 2026.7.17)
        return f"{a:04d}-{b:02d}-{c:02d}"
    elif c > 31:
        # m/d/y (year last, e.g. 7/17/2026)
        return f"{c:04d}-{a:02d}-{b:02d}"
    else:
        # Ambiguous: assume y/m/d if a has 4 digits, else m/d/y
        if a >= 1000:
            return f"{a:04d}-{b:02d}-{c:02d}"
        else:
            return f"{c:04d}-{a:02d}-{b:02d}"


def _parse_date(s, label="日期"):
    """Parse date string → tz-aware UTC Timestamp. Empty string = no filter."""
    s_fmt = parse_date_flex(s)
    if not s_fmt:
        return None
    try:
        return pd.Timestamp(s_fmt).tz_localize("UTC")
    except Exception:
        raise ValueError(f"{label}格式错误: '{s}'。请使用如 2026-07-01 或 7/17 的格式")


def build_unified_trades(grid_fills, trades_df, start_str, end_str, symbol_filter):
    """Filter and merge grid + trend into one unified table, ordered by entry time."""
    unified = []
    start_dt = _parse_date(start_str, "起始日期") if start_str else None
    end_dt = _parse_date(end_str, "结束日期") if end_str else None
    if end_dt is not None:
        end_dt = end_dt + pd.Timedelta(days=1)

    # ── Grid fills (sell_filled=TRUE) ──
    if not grid_fills.empty and "buy_time" in grid_fills.columns:
        mask = grid_fills.get("sell_filled", pd.Series([False] * len(grid_fills))) == True  # noqa: E712
        gf = grid_fills[mask].copy()
        gf["buy_time_dt"] = pd.to_datetime(gf["buy_time"], utc=True)
        if "sell_fill_time" in gf.columns:
            gf["sell_fill_time_dt"] = pd.to_datetime(gf["sell_fill_time"], utc=True)

        if start_dt:
            gf = gf[gf["buy_time_dt"] >= start_dt]
        if end_dt:
            gf = gf[gf["buy_time_dt"] < end_dt]
        if symbol_filter != "全部" and "symbol" in gf.columns:
            gf = gf[gf["symbol"] == symbol_filter]

        for _, row in gf.iterrows():
            bp = float(row.get("buy_price", 0))
            sp = float(row.get("sell_fill_price", 0) or row.get("sell_price", 0))
            qty = float(row.get("buy_qty", 0))
            pnl = (sp - bp) * qty
            pnl_pct = (sp / bp - 1) * 100 if bp > 0 else 0.0
            unified.append(
                {
                    "source": "网格",
                    "symbol": row.get("symbol", ""),
                    "entry_time": row.get("buy_time", ""),
                    "exit_time": row.get("sell_fill_time", ""),
                    "entry_price": bp,
                    "exit_price": sp,
                    "qty": qty,
                    "pnl": round(pnl, 8),
                    "pnl_pct": round(pnl_pct, 4),
                }
            )

    # ── Trend trades ──
    if not trades_df.empty and "entry_time" in trades_df.columns:
        tf = trades_df.copy()
        tf["entry_time_dt"] = pd.to_datetime(tf["entry_time"], utc=True)
        if start_dt:
            tf = tf[tf["entry_time_dt"] >= start_dt]
        if end_dt:
            tf = tf[tf["entry_time_dt"] < end_dt]
        if symbol_filter != "全部" and "symbol" in tf.columns:
            tf = tf[tf["symbol"] == symbol_filter]

        for _, row in tf.iterrows():
            unified.append(
                {
                    "source": "趋势",
                    "symbol": row.get("symbol", ""),
                    "entry_time": row.get("entry_time", ""),
                    "exit_time": row.get("exit_time", ""),
                    "entry_price": float(row.get("entry_price", 0) or 0),
                    "exit_price": float(row.get("exit_price", 0) or 0),
                    "qty": float(row.get("quantity", 0)),
                    "pnl": round(float(row.get("pnl_realized", 0) or 0), 8),
                    "pnl_pct": round(float(row.get("pnl_pct", 0) or 0), 4),
                }
            )

    if not unified:
        return []

    df = pd.DataFrame(unified)
    df = df.sort_values("entry_time", ascending=True).reset_index(drop=True)
    return df.to_dict("records")


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------


class AnalyticsApp:
    def __init__(self, root):
        self.root = root
        self.root.title("OpenQuant Trade Analytics")
        self.root.geometry("1280x900")
        self.root.configure(bg="#1a1a2e")

        # Theme colors
        self.bg = "#1a1a2e"
        self.card_bg = "#16213e"
        self.fg = "#e0e0e0"
        self.accent = "#4fc3f7"
        self.green = "#4caf50"
        self.red = "#f44336"
        self.yellow = "#ffb74d"
        self.muted = "#888888"

        # Set ttk theme
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "TFrame",
            background=self.bg,
        )
        style.configure(
            "Card.TFrame",
            background=self.card_bg,
            relief="flat",
            borderwidth=1,
        )
        style.configure(
            "TLabel",
            background=self.bg,
            foreground=self.fg,
            font=("Segoe UI", 10),
        )
        style.configure(
            "Card.TLabel",
            background=self.card_bg,
            foreground=self.fg,
        )
        style.configure(
            "Header.TLabel",
            background=self.bg,
            foreground=self.accent,
            font=("Segoe UI", 16, "bold"),
        )
        style.configure(
            "MetricValue.TLabel",
            background=self.card_bg,
            foreground=self.fg,
            font=("Segoe UI", 18, "bold"),
        )
        style.configure(
            "MetricLabel.TLabel",
            background=self.card_bg,
            foreground=self.muted,
            font=("Segoe UI", 9),
        )
        style.configure(
            "TButton",
            background="#0d7377",
            foreground=self.fg,
            font=("Segoe UI", 10),
            borderwidth=0,
            padding=6,
        )
        style.configure(
            "TEntry",
            fieldbackground="#2a2a4a",
            foreground=self.fg,
            insertcolor=self.fg,
        )
        style.configure(
            "Treeview",
            background=self.card_bg,
            foreground=self.fg,
            fieldbackground=self.card_bg,
            font=("Consolas", 9),
        )
        style.configure(
            "Treeview.Heading",
            background="#0f3460",
            foreground=self.fg,
            font=("Segoe UI", 9, "bold"),
        )
        style.map("Treeview", background=[("selected", "#0d7377")])

        # Data
        self.initial_equity, self.grid_fills, self.trades_df = fetch_data()
        self.filtered_trades = []
        self.symbols = self._collect_symbols()

        # Build UI
        self._build_header()
        self._build_controls()
        self._build_metrics()
        self._build_charts()
        self._build_table()

        # Initial load
        self._refresh()

    def _collect_symbols(self):
        syms = ["全部"]
        if not self.grid_fills.empty and "symbol" in self.grid_fills.columns:
            syms += sorted(self.grid_fills["symbol"].unique().tolist())
        if not self.trades_df.empty and "symbol" in self.trades_df.columns:
            for s in self.trades_df["symbol"].unique():
                if s not in syms:
                    syms.append(s)
        return syms

    def _data_date_range(self):
        """Return (min_date_str, max_date_str) from all available trade data."""
        dates = []
        if not self.grid_fills.empty and "buy_time" in self.grid_fills.columns:
            dates.extend(pd.to_datetime(self.grid_fills["buy_time"], utc=True).dropna().tolist())
        if not self.trades_df.empty and "entry_time" in self.trades_df.columns:
            dates.extend(pd.to_datetime(self.trades_df["entry_time"], utc=True).dropna().tolist())
        if dates:
            return min(dates).strftime("%Y-%m-%d"), max(dates).strftime("%Y-%m-%d")
        # Fallback: today's date
        today = datetime.now().strftime("%Y-%m-%d")
        return today, today

    # ------------------------------------------------------------------
    # UI Sections
    # ------------------------------------------------------------------

    def _build_header(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill="x", padx=16, pady=(12, 0))

        ttk.Label(frame, text="📊 OpenQuant Trade Analytics", style="Header.TLabel").pack(
            side="left"
        )
        ttk.Label(
            frame,
            text=f"初始权益: ${self.initial_equity:,.4f}",
            style="TLabel",
        ).pack(side="right")

    def _build_controls(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill="x", padx=16, pady=8)

        # Date range — pre-populate from data, with calendar picker buttons
        min_d, max_d = self._data_date_range()
        ttk.Label(frame, text="起始:").pack(side="left", padx=(0, 2))
        self.start_var = tk.StringVar(value=min_d)
        start_entry = ttk.Entry(frame, textvariable=self.start_var, width=11)
        start_entry.pack(side="left")
        ttk.Button(
            frame, text="📅", width=2,
            command=lambda: self._pick_date(self.start_var),
        ).pack(side="left", padx=(0, 12))

        ttk.Label(frame, text="结束:").pack(side="left", padx=(0, 2))
        self.end_var = tk.StringVar(value=max_d)
        end_entry = ttk.Entry(frame, textvariable=self.end_var, width=11)
        end_entry.pack(side="left")
        ttk.Button(
            frame, text="📅", width=2,
            command=lambda: self._pick_date(self.end_var),
        ).pack(side="left", padx=(0, 12))

        # Symbol
        ttk.Label(frame, text="交易对:").pack(side="left", padx=(0, 4))
        self.symbol_var = tk.StringVar(value="全部")
        self.sym_combo = ttk.Combobox(
            frame,
            textvariable=self.symbol_var,
            values=self.symbols,
            state="readonly",
            width=10,
        )
        self.sym_combo.pack(side="left", padx=(0, 16))

        # Refresh button
        refresh_btn = ttk.Button(frame, text="🔄 刷新", command=self._refresh)
        refresh_btn.pack(side="left", padx=(0, 8))

        # Export button
        export_btn = ttk.Button(frame, text="📥 导出 CSV", command=self._export_csv)
        export_btn.pack(side="left")

    def _pick_date(self, target_var: tk.StringVar):
        """Open a calendar popup, then write the selected date back to target_var."""

        def _select():
            sel = cal.selection_get()
            if sel:
                target_var.set(sel.strftime("%Y-%m-%d"))
            top.destroy()

        top = tk.Toplevel(self.root)
        top.title("选择日期")
        top.resizable(False, False)
        # Position near the middle of the parent
        top.transient(self.root)
        top.grab_set()

        # Default to whatever is currently in the field, or today
        try:
            init_str = parse_date_flex(target_var.get())
            init_date = datetime.strptime(init_str, "%Y-%m-%d") if init_str else datetime.now()
        except Exception:
            init_date = datetime.now()

        cal = Calendar(
            top,
            selectmode="day",
            year=init_date.year,
            month=init_date.month,
            day=init_date.day,
            date_pattern="yyyy-mm-dd",
            showweeknumbers=False,
            firstweekday="sunday",
        )
        cal.pack(padx=10, pady=10)

        ttk.Button(top, text="确定", command=_select).pack(pady=(0, 10))

    def _build_metrics(self):
        self.metrics_frame = ttk.Frame(self.root)
        self.metrics_frame.pack(fill="x", padx=16, pady=4)

        # 7 metric cards
        self.metric_labels = {}
        self.metric_values = {}
        self.metric_containers = []
        metrics_names = [
            ("total_pnl", "总盈亏"),
            ("return_rate", "收益率"),
            ("win_rate", "胜率"),
            ("trade_count", "交易笔数"),
            ("max_dd", "最大回撤"),
            ("profit_factor", "盈亏比"),
            ("max_consec_loss", "最大连亏"),
        ]

        for key, label in metrics_names:
            card = ttk.Frame(self.metrics_frame, style="Card.TFrame")
            # Use uniform relative width: each card gets 1/7 of the space
            card.pack(side="left", expand=True, fill="both", padx=3, pady=4)
            self.metric_containers.append(card)

            val_lbl = ttk.Label(card, text="—", style="MetricValue.TLabel")
            val_lbl.pack(anchor="center", pady=(8, 0))

            desc_lbl = ttk.Label(card, text=label, style="MetricLabel.TLabel")
            desc_lbl.pack(anchor="center", pady=(0, 8))

            self.metric_values[key] = val_lbl
            self.metric_labels[key] = desc_lbl

    def _build_charts(self):
        charts_frame = ttk.Frame(self.root)
        charts_frame.pack(fill="both", expand=True, padx=16, pady=4)

        # Left: equity curve
        left_frame = ttk.Frame(charts_frame)
        left_frame.pack(side="left", fill="both", expand=True)

        self.fig_equity = Figure(figsize=(7, 3), dpi=100, facecolor=self.bg)
        self.ax_equity = self.fig_equity.add_subplot(111)
        self.ax_equity.set_facecolor(self.card_bg)
        self.canvas_equity = FigureCanvasTkAgg(self.fig_equity, left_frame)
        self.canvas_equity.get_tk_widget().pack(fill="both", expand=True)

        # Right: per-trade bar chart
        right_frame = ttk.Frame(charts_frame)
        right_frame.pack(side="right", fill="both", expand=True)

        self.fig_bars = Figure(figsize=(7, 3), dpi=100, facecolor=self.bg)
        self.ax_bars = self.fig_bars.add_subplot(111)
        self.ax_bars.set_facecolor(self.card_bg)
        self.canvas_bars = FigureCanvasTkAgg(self.fig_bars, right_frame)
        self.canvas_bars.get_tk_widget().pack(fill="both", expand=True)

    def _build_table(self):
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 12))

        columns = ("#", "来源", "交易对", "入场时间", "离场时间", "入场价", "离场价", "数量", "盈亏", "收益率%")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=10)

        col_widths = [40, 50, 70, 140, 140, 80, 80, 60, 80, 70]
        for col, width in zip(columns, col_widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor="center")

        # Scrollbars
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Tag colors
        self.tree.tag_configure("profit", foreground=self.green)
        self.tree.tag_configure("loss", foreground=self.red)

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def _refresh(self):
        # Re-fetch from DB every time to catch new trades since last refresh
        self.initial_equity, self.grid_fills, self.trades_df = fetch_data()
        self.symbols = self._collect_symbols()
        self.sym_combo["values"] = self.symbols

        try:
            trades = build_unified_trades(
                self.grid_fills,
                self.trades_df,
                self.start_var.get().strip(),
                self.end_var.get().strip(),
                self.symbol_var.get(),
            )
        except ValueError as e:
            messagebox.showerror("日期格式错误", str(e))
            return

        self.filtered_trades = trades
        self._update_metrics()
        self._update_charts()
        self._update_table()

    # ------------------------------------------------------------------
    # Metrics Update
    # ------------------------------------------------------------------

    def _update_metrics(self):
        trades = self.filtered_trades
        if not trades:
            for key in self.metric_values:
                self.metric_values[key].config(text="—")
            return

        df = pd.DataFrame(trades)
        pnls = df["pnl"].values
        total_pnl = sum(pnls)
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        win_rate = wins / n * 100 if n else 0
        total_profit = sum(p for p in pnls if p > 0)
        total_loss_val = abs(sum(p for p in pnls if p < 0))
        profit_factor = total_profit / total_loss_val if total_loss_val > 0 else float("inf")
        return_rate = total_pnl / self.initial_equity * 100 if self.initial_equity > 0 else 0

        # Max drawdown
        cumsum = pd.Series(pnls).cumsum()
        running_max = cumsum.cummax()
        dd = (cumsum - running_max).min()

        # Max consecutive losses
        max_cl = 0
        current = 0
        for p in pnls:
            if p < 0:
                current += 1
                max_cl = max(max_cl, current)
            else:
                current = 0

        vals = {
            "total_pnl": f"${total_pnl:+.4f}",
            "return_rate": f"{return_rate:+.2f}%",
            "win_rate": f"{win_rate:.1f}%",
            "trade_count": str(n),
            "max_dd": f"${dd:.4f}",
            "profit_factor": f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞",
            "max_consec_loss": str(max_cl),
        }

        for key, val in vals.items():
            if key in self.metric_values:
                self.metric_values[key].config(text=val)

        # Color the P&L value
        color = self.green if total_pnl >= 0 else self.red
        self.metric_values["total_pnl"].config(foreground=color)

    # ------------------------------------------------------------------
    # Charts Update
    # ------------------------------------------------------------------

    def _update_charts(self):
        trades = self.filtered_trades

        # ── Equity curve ──
        self.ax_equity.clear()
        self.ax_equity.set_facecolor(self.card_bg)
        self.ax_equity.tick_params(colors=self.muted, labelsize=8)
        self.ax_equity.spines["bottom"].set_color(self.muted)
        self.ax_equity.spines["left"].set_color(self.muted)
        self.ax_equity.spines["top"].set_visible(False)
        self.ax_equity.spines["right"].set_visible(False)

        if trades:
            df = pd.DataFrame(trades)
            df["time"] = pd.to_datetime(df["entry_time"], utc=True)
            df = df.sort_values("time")
            df["cumsum"] = df["pnl"].cumsum()

            self.ax_equity.plot(
                df["time"],
                df["cumsum"],
                color=self.accent,
                linewidth=1.5,
            )
            self.ax_equity.fill_between(
                df["time"],
                0,
                df["cumsum"],
                alpha=0.15,
                color=self.accent,
            )
            self.ax_equity.axhline(y=0, color=self.muted, linewidth=0.5, linestyle="--")
            self.ax_equity.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            self.ax_equity.set_title("累计盈亏", color=self.fg, fontsize=11, pad=8)
        else:
            self.ax_equity.text(
                0.5, 0.5, "无数据", transform=self.ax_equity.transAxes,
                ha="center", va="center", color=self.muted,
            )

        self.fig_equity.tight_layout(pad=2.0)
        self.canvas_equity.draw()

        # ── Per-trade bar chart ──
        self.ax_bars.clear()
        self.ax_bars.set_facecolor(self.card_bg)
        self.ax_bars.tick_params(colors=self.muted, labelsize=8)
        self.ax_bars.spines["bottom"].set_color(self.muted)
        self.ax_bars.spines["left"].set_color(self.muted)
        self.ax_bars.spines["top"].set_visible(False)
        self.ax_bars.spines["right"].set_visible(False)

        if trades:
            pnls = [t["pnl"] for t in trades]
            colors = [self.green if p > 0 else self.red for p in pnls]
            x = range(1, len(pnls) + 1)
            self.ax_bars.bar(x, pnls, color=colors, width=0.8)
            self.ax_bars.axhline(y=0, color=self.muted, linewidth=0.5)
            self.ax_bars.set_title("每笔盈亏", color=self.fg, fontsize=11, pad=8)
        else:
            self.ax_bars.text(
                0.5, 0.5, "无数据", transform=self.ax_bars.transAxes,
                ha="center", va="center", color=self.muted,
            )

        self.fig_bars.tight_layout(pad=2.0)
        self.canvas_bars.draw()

    # ------------------------------------------------------------------
    # Table Update
    # ------------------------------------------------------------------

    def _update_table(self):
        # Clear
        for item in self.tree.get_children():
            self.tree.delete(item)

        for i, t in enumerate(self.filtered_trades):
            pnl = t["pnl"]
            pnl_pct = t["pnl_pct"]
            tag = "profit" if pnl > 0 else "loss"

            # Format entry/exit time: strip the .SSSSSS+TZ suffix
            def _fmt_ts(ts):
                if not ts:
                    return ""
                # "2026-07-01T12:34:56.789012+00:00" -> "2026-07-01 12:34"
                return str(ts)[:16].replace("T", " ")

            values = (
                i + 1,
                t["source"],
                t["symbol"],
                _fmt_ts(t["entry_time"]),
                _fmt_ts(t["exit_time"]),
                f"{t['entry_price']:.5f}",
                f"{t['exit_price']:.5f}",
                f"{t['qty']:.4f}",
                f"{pnl:+.4f}",
                f"{pnl_pct:+.2f}%",
            )
            self.tree.insert("", "end", values=values, tags=(tag,))

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def _export_csv(self):
        if not self.filtered_trades:
            messagebox.showinfo("导出", "无数据可导出")
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"trades_{datetime.now().strftime('%Y%m%d')}.csv",
        )
        if not filepath:
            return

        df = pd.DataFrame(self.filtered_trades)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        messagebox.showinfo("导出", f"已导出 {len(df)} 条记录到:\n{filepath}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def main():
    root = tk.Tk()
    app = AnalyticsApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
