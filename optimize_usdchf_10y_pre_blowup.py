from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import argparse
import json
import random
import re

from amazing31_python import Amazing31, Config, OpenMode, OrderType
from backtest_usdchf import Bar, SimBroker, calc_max_drawdown, download_usdchf_5m, load_bars_from_csv


@dataclass
class YearResult:
    year_idx: int
    start_utc: str
    end_utc: str
    bars: int
    net_profit: float
    final_balance: float
    max_drawdown_pct: float
    min_free_margin: float
    blew_up: bool
    blowup_time_utc: str


def split_into_year_windows(bars: List[Bar], years: int = 10) -> List[List[Bar]]:
    if not bars:
        return []
    windows: List[List[Bar]] = []
    idx = 0
    n = len(bars)
    start_ts = bars[0].ts
    sec_1y = 365 * 24 * 60 * 60
    for _ in range(years):
        end_ts = start_ts + sec_1y
        j = idx
        while j < n and bars[j].ts < end_ts:
            j += 1
        if j <= idx:
            break
        windows.append(bars[idx:j])
        idx = j
        start_ts = end_ts
    return windows


TUNABLE_PARAM_NAMES = (
    "lot",
    "k_lot",
    "max_lot",
)


def sample_params(rng: random.Random) -> Dict[str, Any]:
    return {
        "lot": round(rng.uniform(0.002, 0.020), 3),
        "k_lot": round(rng.uniform(1.01, 1.20), 3),
        "max_lot": round(rng.uniform(0.20, 1.50), 2),
    }


def seed_param_candidates() -> List[Dict[str, Any]]:
    return [
        {},
        {"lot": 0.003, "k_lot": 1.02, "max_lot": 0.25},
        {"lot": 0.005, "k_lot": 1.05, "max_lot": 0.40},
        {"lot": 0.008, "k_lot": 1.08, "max_lot": 0.80},
    ]


def run_one_year(year_idx: int, bars: List[Bar], cfg: Config) -> YearResult:
    broker = SimBroker(symbol="USDCHF", initial_balance=10_000.0, leverage=100, seed=20260226 + year_idx)
    strat = Amazing31(broker, cfg)

    blew_up = False
    blowup_ts = 0
    min_free_margin = float("inf")

    for bar in bars:
        broker.on_bar(bar)
        broker.trigger_pending_from_bar()
        strat.on_tick(now_ts=bar.ts, current_bar_ts=bar.ts)
        broker.snapshot()

        eq = broker.equity()
        fm = broker.free_margin()
        if fm < min_free_margin:
            min_free_margin = fm

        # "爆仓"近似定义: 账户净值<=0 或可用保证金<=0
        if eq <= 0.0 or fm <= 0.0:
            blew_up = True
            blowup_ts = bar.ts
            break

    for o in list(broker.get_orders()):
        if o.type in (OrderType.BUY, OrderType.SELL):
            broker.close_order(o.ticket)
        else:
            broker.delete_order(o.ticket)
    broker.snapshot()

    start_dt = datetime.fromtimestamp(bars[0].ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(bars[-1].ts, tz=timezone.utc)
    blowup_time = datetime.fromtimestamp(blowup_ts, tz=timezone.utc).isoformat() if blew_up else "-"

    final_balance = broker.balance()
    net_profit = final_balance - 10_000.0
    max_dd_pct = calc_max_drawdown(broker.equity_curve) * 100.0

    if min_free_margin == float("inf"):
        min_free_margin = broker.free_margin()

    return YearResult(
        year_idx=year_idx,
        start_utc=start_dt.isoformat(),
        end_utc=end_dt.isoformat(),
        bars=len(bars),
        net_profit=net_profit,
        final_balance=final_balance,
        max_drawdown_pct=max_dd_pct,
        min_free_margin=min_free_margin,
        blew_up=blew_up,
        blowup_time_utc=blowup_time,
    )


def evaluate_params(params: Dict[str, Any], yearly_bars: List[List[Bar]]) -> Tuple[float, List[YearResult], Dict[str, float]]:
    cfg = Config(symbol="USDCHF", magic=9453, **params)

    results: List[YearResult] = []
    for i, bars in enumerate(yearly_bars, start=1):
        year_result = run_one_year(i, bars, cfg)
        results.append(year_result)
        if year_result.blew_up:
            break

    nets = [r.net_profit for r in results]
    blowups = sum(1 for r in results if r.blew_up)
    sum_net = sum(nets) if nets else -10**12
    avg_net = sum_net / len(nets) if nets else -10**12
    min_net = min(nets) if nets else -10**12
    worst_dd = max((r.max_drawdown_pct for r in results), default=0.0)
    min_free_margin = min((r.min_free_margin for r in results), default=0.0)

    # 硬约束: 任一年爆仓即判为不可行解。
    if blowups > 0 or len(results) < len(yearly_bars):
        score = -10**12 - blowups * 10**9 + sum_net
    else:
        # 目标：在10年全不爆仓的前提下，最大化总净利润；
        # 同分时偏好最差年度更高、回撤更低。
        score = sum_net + 0.05 * min_net - 0.02 * worst_dd

    agg = {
        "sum_net_profit": sum_net,
        "avg_net_profit": avg_net,
        "min_year_net_profit": min_net,
        "blowup_years": float(blowups),
        "years_ran": float(len(results)),
        "worst_year_max_drawdown_pct": worst_dd,
        "min_free_margin": min_free_margin,
        "feasible_no_blowup_10y": float(1 if blowups == 0 and len(results) == len(yearly_bars) else 0),
    }
    return score, results, agg


def _find_local_merged_file(start_d: date, end_d: date) -> Path | None:
    exact = Path("download") / f"usdchf-m5-bid-{start_d.isoformat()}-{end_d.isoformat()}-merged.csv"
    if exact.exists() and exact.stat().st_size > 0:
        return exact

    pat = re.compile(r"usdchf-m5-bid-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})-merged\.csv$")
    candidates: List[Tuple[int, date, Path]] = []
    for p in Path("download").glob("usdchf-m5-bid-*-*-merged.csv"):
        m = pat.fullmatch(p.name)
        if not m:
            continue
        try:
            s = date.fromisoformat(m.group(1))
            e = date.fromisoformat(m.group(2))
        except ValueError:
            continue
        span_days = (e - s).days
        candidates.append((span_days, e, p))

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def load_or_download_10y_bars() -> Tuple[Path, List[Bar]]:
    end_d = date.today()
    start_d = end_d - timedelta(days=365 * 10)

    merged = _find_local_merged_file(start_d, end_d)
    if merged is None:
        merged = download_usdchf_5m(start_d, end_d)

    bars = load_bars_from_csv(merged)
    if not bars:
        raise RuntimeError(f"empty bars from {merged}")
    return merged, bars


def _params_for_json(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, OpenMode):
            out[k] = int(v)
        else:
            out[k] = v
    return out


def save_result_json(
    out_path: Path,
    data_path: Path,
    trials: int,
    seed: int,
    params: Dict[str, Any],
    agg: Dict[str, float],
    year_results: List[YearResult],
) -> None:
    payload = {
        "objective": "maximize net profit over 10 yearly windows with strict no-blowup constraint",
        "blowup_definition": "equity <= 0 OR free_margin <= 0",
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "data_file": str(data_path),
        "trials": trials,
        "seed": seed,
        "best_params": _params_for_json(params),
        "best_aggregate": agg,
        "yearly_results": [asdict(x) for x in year_results],
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize USDCHF params for max profit under strict no-blowup (10Y)")
    parser.add_argument("--trials", type=int, default=20, help="Random-search trials")
    parser.add_argument("--seed", type=int, default=20260226, help="Random seed")
    args = parser.parse_args()

    data_path, bars = load_or_download_10y_bars()
    yearly_bars = split_into_year_windows(bars, years=10)
    if len(yearly_bars) < 10:
        raise RuntimeError(f"need 10 year windows, got {len(yearly_bars)} from {data_path}")

    rng = random.Random(args.seed)
    best_any_score = -10**18
    best_any_params: Dict[str, Any] = {}
    best_any_year_results: List[YearResult] = []
    best_any_agg: Dict[str, float] = {}

    best_feasible_score = -10**18
    best_feasible_params: Dict[str, Any] = {}
    best_feasible_year_results: List[YearResult] = []
    best_feasible_agg: Dict[str, float] = {}
    feasible_count = 0

    print(f"data={data_path}")
    print(f"Tunable params ({len(TUNABLE_PARAM_NAMES)}): {', '.join(TUNABLE_PARAM_NAMES)}")
    print(f"bars={len(bars)}, years={len(yearly_bars)}, trials={args.trials}")
    for idx, params in enumerate(seed_param_candidates(), start=1):
        score, year_results, agg = evaluate_params(params, yearly_bars)
        if score > best_any_score:
            best_any_score = score
            best_any_params = params
            best_any_year_results = year_results
            best_any_agg = agg
        if agg["feasible_no_blowup_10y"] == 1.0:
            feasible_count += 1
            if score > best_feasible_score:
                best_feasible_score = score
                best_feasible_params = params
                best_feasible_year_results = year_results
                best_feasible_agg = agg
        print(
            f"[seed-{idx}] score={score:.2f} sum_net={agg['sum_net_profit']:.2f} "
            f"min_year_net={agg['min_year_net_profit']:.2f} blowups={int(agg['blowup_years'])} "
            f"years_ran={int(agg['years_ran'])}/10 feasible_count={feasible_count}"
        )

    for i in range(1, args.trials + 1):
        params = sample_params(rng)
        score, year_results, agg = evaluate_params(params, yearly_bars)

        if score > best_any_score:
            best_any_score = score
            best_any_params = params
            best_any_year_results = year_results
            best_any_agg = agg

        if agg["feasible_no_blowup_10y"] == 1.0:
            feasible_count += 1
            if score > best_feasible_score:
                best_feasible_score = score
                best_feasible_params = params
                best_feasible_year_results = year_results
                best_feasible_agg = agg

        best_shown = best_feasible_score if best_feasible_score > -10**18 else best_any_score
        print(
            f"[{i}/{args.trials}] score={score:.2f} sum_net={agg['sum_net_profit']:.2f} "
            f"min_year_net={agg['min_year_net_profit']:.2f} blowups={int(agg['blowup_years'])} "
            f"years_ran={int(agg['years_ran'])}/10 feasible_count={feasible_count} "
            f"| best={best_shown:.2f}"
        )

    if not best_feasible_params:
        print("\nWARNING: no 10Y no-blowup candidate found in this run.")
        print("Top candidate is infeasible:")
        print(best_any_params)
        print(best_any_agg)
        raise RuntimeError("no feasible no-blowup params found. increase --trials or tighten ranges.")

    out_json = Path("optimized_params_10y_no_blowup_max_profit.json")
    save_result_json(
        out_path=out_json,
        data_path=data_path,
        trials=args.trials,
        seed=args.seed,
        params=best_feasible_params,
        agg=best_feasible_agg,
        year_results=best_feasible_year_results,
    )

    print("\n=== BEST PARAMS (10Y STRICT NO-BLOWUP) ===")
    print(best_feasible_params)
    print("\n=== BEST AGG ===")
    print(best_feasible_agg)
    print("\n=== YEARLY RESULTS ===")
    for r in best_feasible_year_results:
        print(
            f"Y{r.year_idx}: {r.start_utc[:10]} -> {r.end_utc[:10]} | "
            f"net={r.net_profit:.2f} dd={r.max_drawdown_pct:.2f}% "
            f"min_fm={r.min_free_margin:.2f} blew_up={r.blew_up} "
            f"blowup_time={r.blowup_time_utc}"
        )
    print(f"\nSaved: {out_json}")


if __name__ == "__main__":
    main()
