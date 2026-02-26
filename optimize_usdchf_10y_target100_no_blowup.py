from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import argparse
import json
import random

from amazing31_python import Config, OpenMode
from optimize_usdchf_10y_pre_blowup import load_or_download_10y_bars, run_one_year, split_into_year_windows

YEARS = 10
INITIAL_BALANCE = 10_000.0
TARGET_YEARLY_RETURN_PCT = 100.0
TARGET_YEARLY_NET = INITIAL_BALANCE * (TARGET_YEARLY_RETURN_PCT / 100.0)


PRESERVED_PARAMS: Dict[str, Any] = {}

TUNABLE_PARAM_NAMES = (
    "lot",
    "k_lot",
    "max_lot",
)


def seed_param_candidates() -> List[Dict[str, Any]]:
    base = dict(PRESERVED_PARAMS)
    seeds = [base]

    # Baseline + progressively aggressive position-sizing presets.
    for lot, max_lot, k_lot in [
        (0.020, 0.80, 1.12),
        (0.032, 1.20, 1.18),
        (0.045, 2.50, 1.24),
        (0.060, 4.00, 1.30),
    ]:
        p = dict(base)
        p.update(
            {
                "lot": lot,
                "max_lot": max_lot,
                "k_lot": k_lot,
            }
        )
        seeds.append(p)
    return seeds


def sample_params(rng: random.Random) -> Dict[str, Any]:
    return {
        "lot": round(rng.uniform(0.020, 0.080), 3),
        "k_lot": round(rng.uniform(1.08, 1.40), 3),
        "max_lot": round(rng.uniform(0.8, 6.0), 2),
    }


def evaluate_params(
    params: Dict[str, Any], yearly_bars: List[List[Any]]
) -> Tuple[Tuple[float, ...], List[Dict[str, Any]], Dict[str, float]]:
    cfg = Config(symbol="USDCHF", magic=9453, **params)

    yearly: List[Dict[str, Any]] = []
    for i, bars in enumerate(yearly_bars, start=1):
        y = run_one_year(i, bars, cfg)
        y_dict = asdict(y)
        y_return_pct = (y.net_profit / INITIAL_BALANCE) * 100.0
        y_dict["return_pct"] = y_return_pct
        yearly.append(y_dict)
        if y.blew_up:
            break

    years_ran = len(yearly)
    blowup_years = float(sum(1 for y in yearly if y["blew_up"]))
    pass_target_years = float(sum(1 for y in yearly if y["net_profit"] >= TARGET_YEARLY_NET))
    min_return_pct = min((y["return_pct"] for y in yearly), default=-10**9)
    sum_net = sum((y["net_profit"] for y in yearly), 0.0)
    min_free_margin = min((y["min_free_margin"] for y in yearly), default=-10**9)
    worst_dd = max((y["max_drawdown_pct"] for y in yearly), default=0.0)

    ran_full_10y = years_ran == len(yearly_bars)
    feasible_no_blowup = ran_full_10y and blowup_years == 0.0
    feasible_target100 = feasible_no_blowup and pass_target_years == float(len(yearly_bars))

    # 分层目标（词典序）:
    # 1) 是否满足“10年均>=100%且不爆仓”
    # 2) 满足目标的年份数
    # 3) 爆仓年数更少
    # 4) 最差年份收益率更高
    # 5) 总净利润更高
    # 6) 最小可用保证金更高
    rank = (
        2.0 if feasible_target100 else (1.0 if feasible_no_blowup else 0.0),
        pass_target_years,
        -blowup_years,
        min_return_pct,
        sum_net,
        min_free_margin,
    )

    agg = {
        "target_yearly_return_pct": TARGET_YEARLY_RETURN_PCT,
        "target_yearly_net_profit": TARGET_YEARLY_NET,
        "sum_net_profit": sum_net,
        "avg_net_profit": sum_net / years_ran if years_ran > 0 else -10**9,
        "min_year_return_pct": min_return_pct,
        "pass_target_years": pass_target_years,
        "blowup_years": blowup_years,
        "years_ran": float(years_ran),
        "feasible_no_blowup_10y": 1.0 if feasible_no_blowup else 0.0,
        "feasible_target100_10y": 1.0 if feasible_target100 else 0.0,
        "worst_year_max_drawdown_pct": worst_dd,
        "min_free_margin": min_free_margin,
    }
    return rank, yearly, agg


def params_to_json(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in params.items():
        out[k] = int(v) if isinstance(v, OpenMode) else v
    return out


def save_json(
    out_path: Path,
    data_path: Path,
    trials: int,
    seed: int,
    best_params: Dict[str, Any],
    best_agg: Dict[str, float],
    best_yearly: List[Dict[str, Any]],
    best_rank: Tuple[float, ...],
    best_target_params: Dict[str, Any],
    best_target_agg: Dict[str, float],
    best_target_yearly: List[Dict[str, Any]],
    best_target_rank: Tuple[float, ...],
) -> None:
    payload = {
        "objective": "target >=100% yearly return, while minimizing blow-up risk as much as possible",
        "target_yearly_return_pct": TARGET_YEARLY_RETURN_PCT,
        "blowup_definition": "equity <= 0 OR free_margin <= 0",
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "data_file": str(data_path),
        "trials": trials,
        "seed": seed,
        "preserved_params": params_to_json(PRESERVED_PARAMS),
        "best_overall_rank": list(best_rank),
        "best_overall_params": params_to_json(best_params),
        "best_overall_aggregate": best_agg,
        "best_overall_yearly_results": best_yearly,
        "best_target100_rank": list(best_target_rank) if best_target_params else None,
        "best_target100_params": params_to_json(best_target_params) if best_target_params else None,
        "best_target100_aggregate": best_target_agg if best_target_params else None,
        "best_target100_yearly_results": best_target_yearly if best_target_params else None,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize USDCHF params for >=100%/year with low blow-up risk")
    parser.add_argument("--trials", type=int, default=20, help="Random-search trials")
    parser.add_argument("--seed", type=int, default=20260226, help="Random seed")
    parser.add_argument(
        "--out",
        type=str,
        default="optimized_params_10y_target100_no_blowup.json",
        help="Output json path",
    )
    args = parser.parse_args()

    data_path, bars = load_or_download_10y_bars()
    yearly_bars = split_into_year_windows(bars, years=YEARS)
    if len(yearly_bars) < YEARS:
        raise RuntimeError(f"need {YEARS} year windows, got {len(yearly_bars)} from {data_path}")

    rng = random.Random(args.seed)
    best_rank: Tuple[float, ...] = (-10**18, -10**18, -10**18, -10**18, -10**18, -10**18)
    best_params: Dict[str, Any] = {}
    best_yearly: List[Dict[str, Any]] = []
    best_agg: Dict[str, float] = {}

    best_target_rank: Tuple[float, ...] = (-10**18, -10**18, -10**18, -10**18, -10**18, -10**18)
    best_target_params: Dict[str, Any] = {}
    best_target_yearly: List[Dict[str, Any]] = []
    best_target_agg: Dict[str, float] = {}

    print(f"data={data_path}")
    print(f"Tunable params ({len(TUNABLE_PARAM_NAMES)}): {', '.join(TUNABLE_PARAM_NAMES)}")
    print(f"bars={len(bars)}, years={len(yearly_bars)}, trials={args.trials}")

    for i, p in enumerate(seed_param_candidates(), start=1):
        rank, yearly, agg = evaluate_params(p, yearly_bars)
        if rank > best_rank:
            best_rank, best_params, best_yearly, best_agg = rank, p, yearly, agg
        if agg["feasible_target100_10y"] == 1.0 and rank > best_target_rank:
            best_target_rank, best_target_params, best_target_yearly, best_target_agg = rank, p, yearly, agg
        print(
            f"[seed-{i}] rank0={rank[0]:.0f} pass={int(agg['pass_target_years'])}/10 "
            f"blowups={int(agg['blowup_years'])} min_ret={agg['min_year_return_pct']:.2f}% "
            f"sum_net={agg['sum_net_profit']:.2f}"
        )

    for i in range(1, args.trials + 1):
        p = sample_params(rng)
        rank, yearly, agg = evaluate_params(p, yearly_bars)
        if rank > best_rank:
            best_rank, best_params, best_yearly, best_agg = rank, p, yearly, agg
        if agg["feasible_target100_10y"] == 1.0 and rank > best_target_rank:
            best_target_rank, best_target_params, best_target_yearly, best_target_agg = rank, p, yearly, agg
        print(
            f"[{i}/{args.trials}] rank0={rank[0]:.0f} pass={int(agg['pass_target_years'])}/10 "
            f"blowups={int(agg['blowup_years'])} min_ret={agg['min_year_return_pct']:.2f}% "
            f"sum_net={agg['sum_net_profit']:.2f} | best_pass={int(best_agg.get('pass_target_years', 0))}/10"
        )

    out_path = Path(args.out)
    save_json(
        out_path=out_path,
        data_path=data_path,
        trials=args.trials,
        seed=args.seed,
        best_params=best_params,
        best_agg=best_agg,
        best_yearly=best_yearly,
        best_rank=best_rank,
        best_target_params=best_target_params,
        best_target_agg=best_target_agg,
        best_target_yearly=best_target_yearly,
        best_target_rank=best_target_rank,
    )

    print("\n=== PRESERVED PARAMS (DEFAULTS) ===")
    print(PRESERVED_PARAMS)
    print("\n=== BEST OVERALL (TARGET-ORIENTED) ===")
    print(best_params)
    print(best_agg)
    if best_target_params:
        print("\n=== BEST STRICT TARGET100 + NO-BLOWUP ===")
        print(best_target_params)
        print(best_target_agg)
    else:
        print("\n=== BEST STRICT TARGET100 + NO-BLOWUP ===")
        print("not found in this run")
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
