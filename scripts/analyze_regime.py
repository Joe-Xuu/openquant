"""
================================================================================
ANALYZE REGIME — First empirical read on the "trend vs ranging" premise
================================================================================

Loads a JSON of OHLCV bars and answers three questions with data:
  1. Kaufman Efficiency Ratio  — how directional is price movement?
  2. Hurst exponent (R/S)      — is there positive autocorrelation (trend)?
  3. RegimeDetector            — what does the ACTUAL project code label each
                                 window, and how does it align with 1 & 2?

This reuses the real strategy.regime_detector.RegimeDetector, not a toy —
so it also serves as a sanity check of the production classifier.

Usage:
    python scripts/analyze_regime.py data/experiment/DOGEUSDT_5m.json
================================================================================
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from strategy.regime_detector import MarketRegime, RegimeDetector  # noqa: E402


# ---------------------------------------------------------------------------
# Metrics (rolling)
# ---------------------------------------------------------------------------

def kaufman_er(closes: list[float], window: int) -> list[float]:
    """Rolling Kaufman Efficiency Ratio: |net move| / sum(|step moves|)."""
    out = []
    for t in range(window, len(closes)):
        net = abs(closes[t] - closes[t - window])
        path = sum(abs(closes[i] - closes[i - 1]) for i in range(t - window + 1, t + 1))
        out.append(net / path if path > 0 else 0.0)
    return out


def hurst_rs(series: list[float], window: int) -> list[float]:
    """Rolling Hurst exponent via rescaled range (R/S)."""
    out = []
    for t in range(window, len(series) + 1):
        seg = series[t - window:t]
        n = len(seg)
        mean = sum(seg) / n
        dev = [x - mean for x in seg]
        # cumulative deviations
        cum = []
        s = 0.0
        for d in dev:
            s += d
            cum.append(s)
        r = max(cum) - min(cum)
        # population std
        var = sum(d * d for d in dev) / n
        std = math.sqrt(var)
        if std <= 0 or r <= 0:
            out.append(0.5)
            continue
        out.append(math.log(r / std) / math.log(n))
    return out


def variance_ratio(returns: list[float], k: int, window: int) -> list[float]:
    """Rolling Lo-MacKinlay variance ratio: Var(k-step) / (k * Var(1-step))."""
    out = []
    for t in range(window + k, len(returns) + 1):
        r1 = returns[t - window:t]                       # 1-step returns
        var1 = sum((x - sum(r1) / window) ** 2 for x in r1) / window
        # k-step overlapping returns
        rk = [sum(returns[j:j + k]) for j in range(t - window, t - k + 1)]
        mk = len(rk)
        vark = sum((x - sum(rk) / mk) ** 2 for x in rk) / mk
        out.append((vark / (k * var1)) if var1 > 0 else 1.0)
    return out


def pct_above(vals: list[float], thr: float) -> float:
    if not vals:
        return float("nan")
    return sum(1 for v in vals if v > thr) / len(vals) * 100


def pct_below(vals: list[float], thr: float) -> float:
    if not vals:
        return float("nan")
    return sum(1 for v in vals if v < thr) / len(vals) * 100


def mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def _pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    a, b = a[:n], b[:n]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((a[i] - ma) ** 2 for i in range(n))
    vb = sum((b[i] - mb) ** 2 for i in range(n))
    if va <= 0 or vb <= 0:
        return float("nan")
    return cov / math.sqrt(va * vb)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Empirical regime analysis on OHLCV JSON.")
    ap.add_argument("path", help="JSON file of OHLCV bars (list of dicts with 'close').")
    ap.add_argument("--er-window", type=int, default=20)
    ap.add_argument("--hurst-window", type=int, default=128)
    ap.add_argument("--det-window", type=int, default=100, help="RegimeDetector lookback.")
    args = ap.parse_args()

    bars = json.loads(Path(args.path).read_text())
    closes = [b["close"] for b in bars]
    n = len(closes)
    print(f"Loaded {n} bars from {args.path}")
    print(f"  close range: {closes[0]} -> {closes[-1]} "
          f"({(closes[-1]/closes[0]-1)*100:+.2f}% over the sample)")
    print(f"  window lengths: ER={args.er_window}, Hurst={args.hurst_window}, "
          f"Detector={args.det_window}\n")

    # 1 & 2: ER + Hurst + VR (raw directional / persistence metrics)
    er = kaufman_er(closes, args.er_window)
    hurst = hurst_rs(closes, args.hurst_window)
    returns = [closes[i] / closes[i - 1] - 1 for i in range(1, n)]
    vr = variance_ratio(returns, k=5, window=64)

    print("=" * 66)
    print(" RAW DIRECTIONAL / PERSISTENCE METRICS")
    print("=" * 66)
    print(f"  Kaufman ER      : mean={mean(er):.3f}  median={sorted(er)[len(er)//2]:.3f}")
    print(f"                    >0.30 (directional): {pct_above(er, 0.30):5.1f}%   "
          f"<0.20 (choppy): {pct_below(er, 0.20):5.1f}%")
    print(f"  Hurst (R/S)     : mean={mean(hurst):.3f}  median={sorted(hurst)[len(hurst)//2]:.3f}")
    print(f"                    >0.55 (persistent): {pct_above(hurst, 0.55):5.1f}%   "
          f"<0.45 (mean-revert): {pct_below(hurst, 0.45):5.1f}%")
    print(f"  Variance ratio  : mean={mean(vr):.3f}  >1.10 (momentum): {pct_above(vr, 1.10):5.1f}%   "
          f"<0.90 (mean-revert): {pct_below(vr, 0.90):5.1f}%")
    print()

    # 3: the ACTUAL production classifier
    det = RegimeDetector()
    current = MarketRegime.RANGING
    trending_frac = 0.0
    score_hist = {"TRENDING": [], "RANGING": []}
    n_eval = 0
    for t in range(args.det_window, n):
        res = det.detect(
            bars[t - args.det_window:t],
            current_regime=current,
        )
        current = res.regime
        n_eval += 1
        if res.regime == MarketRegime.TRENDING:
            trending_frac += 1
        score_hist[res.regime.value].append(res.score)

    print("=" * 66)
    print(" PRODUCTION RegimeDetector (5-factor + hysteresis)")
    print("=" * 66)
    print(f"  windows evaluated : {n_eval}")
    print(f"  labelled TRENDING : {trending_frac/n_eval*100:5.1f}%")
    print(f"  labelled RANGING  : {(1 - trending_frac/n_eval)*100:5.1f}%")
    print(f"  score when TRENDING: mean={mean(score_hist['TRENDING']):.3f}  "
          f"n={len(score_hist['TRENDING'])}")
    print(f"  score when RANGING : mean={mean(score_hist['RANGING']):.3f}  "
          f"n={len(score_hist['RANGING'])}")
    print()

    # Alignment: does the production score track the raw directional metrics?
    # (compare at a common sampling grid)
    print("=" * 66)
    print(" ALIGNMENT: production score vs raw metrics (Pearson r)")
    print("=" * 66)
    # Re-run to collect scores aligned to bar index
    scores = [float("nan")] * n
    current = MarketRegime.RANGING
    for t in range(args.det_window, n):
        res = det.detect(bars[t - args.det_window:t], current_regime=current)
        current = res.regime
        scores[t] = res.score
    for name, metric in [("Kaufman ER", er), ("Hurst", hurst), ("VarRatio", vr)]:
        # align metric (starts later) to bar index
        m = metric
        off = 0
        # metrics have different offsets; align by truncating to last len(m) scores
        s_tail = [s for s in scores if not math.isnan(s)][-len(m):]
        r = _pearson(m, s_tail)
        print(f"  score ~ {name:<11}: r={r:+.3f}")

    print()
    print("NOTE: Hurst window=128 means it can only see ~128-bar (10.7h on 5m)")
    print("structure; ER window=20 sees ~100min. Interpret per-horizon.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
