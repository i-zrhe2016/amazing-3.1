from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
import argparse
import random

from amazing31_python import Amazing31, Config, OrderType
from backtest_usdchf import (
    SimBroker,
    calc_max_drawdown,
    download_usdchf_5m,
    load_bars_from_csv,
)

TUNABLE_PARAM_NAMES = (
    "lot",
    "k_lot",
    "max_lot",
)


def run_with_config(bars, cfg: Config) -> Dict[str, float]:
    broker = SimBroker(symbol=cfg.symbol, initial_balance=10_000.0, leverage=100, seed=20260226)
    strat = Amazing31(broker, cfg)

    for bar in bars:
        broker.on_bar(bar)
        broker.trigger_pending_from_bar()
        strat.on_tick(now_ts=bar.ts, current_bar_ts=bar.ts)
        broker.snapshot()

    for o in list(broker.get_orders()):
        if o.type in (OrderType.BUY, OrderType.SELL):
            broker.close_order(o.ticket)
        else:
            broker.delete_order(o.ticket)
    broker.snapshot()

    closed = broker.closed_pnls
    wins = [x for x in closed if x > 0]
    losses = [x for x in closed if x <= 0]
    max_dd = calc_max_drawdown(broker.equity_curve) * 100.0
    net = broker.balance() - 10_000.0
    pf = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 0.0

    return {
        "final_balance": broker.balance(),
        "net_profit": net,
        "closed_trades": float(len(closed)),
        "win_rate": (len(wins) / len(closed) * 100.0 if closed else 0.0),
        "profit_factor": pf,
        "max_drawdown": max_dd,
        "avg_spread": broker.avg_spread_pips(),
    }


def score_stats(stats: Dict[str, float]) -> float:
    ret_pct = stats["net_profit"] / 100.0
    dd = stats["max_drawdown"]
    pf = stats["profit_factor"]
    trades = stats["closed_trades"]

    score = ret_pct - 2.0 * dd + 15.0 * max(0.0, pf - 1.0)
    if dd > 35.0:
        score -= (dd - 35.0) * 4.0
    if trades < 200:
        score -= 20.0
    return score


def sample_params(rng: random.Random) -> Dict[str, Any]:
    return {
        "lot": round(rng.uniform(0.005, 0.02), 3),
        "k_lot": round(rng.uniform(1.05, 1.28), 3),
        "max_lot": round(rng.uniform(0.3, 2.0), 2),
    }


def build_cfg(params: Dict[str, Any]) -> Config:
    return Config(symbol="USDCHF", magic=9453, **params)


def bars_in_last_years(all_bars, years: int):
    end_ts = all_bars[-1].ts
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc).date()
    start_dt = end_dt - timedelta(days=365 * years)
    start_ts = int(datetime.combine(start_dt, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    return [b for b in all_bars if b.ts >= start_ts]


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize Amazing3.1 USDCHF params")
    parser.add_argument("--trials", type=int, default=30, help="Random-search trials on 1Y")
    parser.add_argument("--seed", type=int, default=20260226, help="Random seed")
    args = parser.parse_args()

    end_d = date.today()
    start_d = end_d - timedelta(days=365 * 5)
    csv_path = download_usdchf_5m(start_d, end_d)
    all_bars = load_bars_from_csv(csv_path)
    bars_1y = bars_in_last_years(all_bars, 1)
    bars_3y = bars_in_last_years(all_bars, 3)
    bars_5y = bars_in_last_years(all_bars, 5)

    if not bars_1y:
        raise RuntimeError("1Y bars is empty")

    baseline_cfg = Config(symbol="USDCHF", magic=9453)
    baseline_1y = run_with_config(bars_1y, baseline_cfg)

    rng = random.Random(args.seed)
    candidates: List[Tuple[float, Dict[str, Any], Dict[str, float]]] = []

    print(f"Tunable params ({len(TUNABLE_PARAM_NAMES)}): {', '.join(TUNABLE_PARAM_NAMES)}")
    print(f"Running random search: trials={args.trials}, bars_1y={len(bars_1y)}")
    for i in range(1, args.trials + 1):
        params = sample_params(rng)
        cfg = build_cfg(params)
        stats_1y = run_with_config(bars_1y, cfg)
        score_1y = score_stats(stats_1y)
        candidates.append((score_1y, params, stats_1y))
        if i % 5 == 0 or i == args.trials:
            best_now = max(candidates, key=lambda x: x[0])
            print(
                f"[{i}/{args.trials}] best_score={best_now[0]:.3f} "
                f"net={best_now[2]['net_profit']:.2f} dd={best_now[2]['max_drawdown']:.2f}% "
                f"pf={best_now[2]['profit_factor']:.3f}"
            )

    top5 = sorted(candidates, key=lambda x: x[0], reverse=True)[:5]
    ranked: List[Tuple[float, Dict[str, Any], Dict[str, float], Dict[str, float]]] = []
    print("\nEvaluating top-5 on 3Y...")
    for score_1y, params, s1 in top5:
        cfg = build_cfg(params)
        s3 = run_with_config(bars_3y, cfg)
        combined = 0.45 * score_1y + 0.55 * score_stats(s3)
        ranked.append((combined, params, s1, s3))
        print(
            f"candidate combined={combined:.3f} | "
            f"1Y net={s1['net_profit']:.2f} dd={s1['max_drawdown']:.2f}% pf={s1['profit_factor']:.3f} | "
            f"3Y net={s3['net_profit']:.2f} dd={s3['max_drawdown']:.2f}% pf={s3['profit_factor']:.3f}"
        )

    ranked.sort(key=lambda x: x[0], reverse=True)
    best_combined, best_params, best_1y, best_3y = ranked[0]
    best_cfg = build_cfg(best_params)
    best_5y = run_with_config(bars_5y, best_cfg)

    print("\n=== Baseline (1Y) ===")
    print(baseline_1y)
    print("\n=== Best Params ===")
    print(best_params)
    print("\n=== Best Stats ===")
    print({"combined_score": best_combined, "1Y": best_1y, "3Y": best_3y, "5Y": best_5y})


if __name__ == "__main__":
    main()
