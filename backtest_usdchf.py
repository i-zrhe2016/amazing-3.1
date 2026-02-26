from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import argparse
import csv
import math
import random
import subprocess

from amazing31_python import Amazing31, Config, OpenMode, Order, OrderType


@dataclass
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float


class SimBroker:
    def __init__(
        self,
        symbol: str = "USDCHF",
        initial_balance: float = 10_000.0,
        leverage: int = 100,
        seed: int = 20260226,
    ):
        self.symbol = symbol
        self._balance = initial_balance
        self._equity = initial_balance
        self._leverage = leverage

        self._digits = 5
        self._point = 0.00001

        self._orders: List[Order] = []
        self._next_ticket = 1

        self.current_bar: Optional[Bar] = None
        self._bid = 0.0
        self._ask = 0.0
        self._spread_points = 0.0

        self.closed_pnls: List[float] = []
        self.equity_curve: List[float] = []
        self.balance_curve: List[float] = []
        self.spread_pips_curve: List[float] = []

        self._rand = random.Random(seed)

    # ---- Broker API used by strategy ----
    def get_orders(self) -> List[Order]:
        return self._orders

    def get_bid_ask(self) -> tuple[float, float]:
        return self._bid, self._ask

    def digits(self) -> int:
        return self._digits

    def point(self) -> float:
        return self._point

    def spread_points(self) -> float:
        return self._spread_points

    def account_leverage(self) -> int:
        return self._leverage

    def free_margin(self) -> float:
        return self._equity - self._used_margin()

    def margin_per_lot(self, symbol: str) -> float:
        if symbol != self.symbol:
            return 0.0
        return 100_000.0 / self._leverage

    def is_trade_allowed(self) -> bool:
        return True

    def is_expert_enabled(self) -> bool:
        return True

    def is_stopped(self) -> bool:
        return False

    def send_pending(self, order_type: OrderType, lots: float, price: float, comment: str) -> Optional[int]:
        ticket = self._next_ticket
        self._next_ticket += 1
        self._orders.append(
            Order(
                ticket=ticket,
                symbol=self.symbol,
                magic=9453,
                type=order_type,
                lots=lots,
                open_price=round(price, self._digits),
                profit=0.0,
                comment=comment,
                open_time=self.current_bar.ts if self.current_bar else 0,
            )
        )
        return ticket

    def modify_order(self, ticket: int, new_price: float) -> bool:
        o = self._find_order(ticket)
        if not o or o.type not in (OrderType.BUYSTOP, OrderType.SELLSTOP):
            return False
        o.open_price = round(new_price, self._digits)
        return True

    def close_order(self, ticket: int) -> bool:
        o = self._find_order(ticket)
        if not o:
            return False
        if o.type in (OrderType.BUYSTOP, OrderType.SELLSTOP):
            return self.delete_order(ticket)

        # Use market close price with slippage against trader.
        if o.type == OrderType.BUY:
            close_px = self._apply_slippage_price(self._bid, o.lots, side="sell")
            pnl = self._pnl_buy(o.lots, o.open_price, close_px)
        else:
            close_px = self._apply_slippage_price(self._ask, o.lots, side="buy")
            pnl = self._pnl_sell(o.lots, o.open_price, close_px)

        self._balance += pnl
        self.closed_pnls.append(pnl)
        self._orders = [x for x in self._orders if x.ticket != ticket]
        self._mark_to_market()
        return True

    def delete_order(self, ticket: int) -> bool:
        n0 = len(self._orders)
        self._orders = [x for x in self._orders if x.ticket != ticket]
        return len(self._orders) != n0

    # ---- Simulation helpers ----
    def on_bar(self, bar: Bar) -> None:
        self.current_bar = bar

        spread_pips = self._dynamic_spread_pips(bar)
        self._spread_points = spread_pips * 10.0

        half_spread = (self._spread_points * self._point) / 2.0
        mid = bar.close
        self._bid = round(mid - half_spread, self._digits)
        self._ask = round(mid + half_spread, self._digits)
        self.spread_pips_curve.append(spread_pips)

    def trigger_pending_from_bar(self) -> None:
        if not self.current_bar:
            return

        ohlc = self.current_bar

        for o in list(self._orders):
            if o.type == OrderType.BUYSTOP and ohlc.high >= o.open_price:
                base_fill = max(ohlc.open, o.open_price)
                fill = self._apply_slippage_price(base_fill, o.lots, side="buy")
                o.type = OrderType.BUY
                o.open_price = round(fill, self._digits)
                o.open_time = ohlc.ts

            elif o.type == OrderType.SELLSTOP and ohlc.low <= o.open_price:
                base_fill = min(ohlc.open, o.open_price)
                fill = self._apply_slippage_price(base_fill, o.lots, side="sell")
                o.type = OrderType.SELL
                o.open_price = round(fill, self._digits)
                o.open_time = ohlc.ts

        self._mark_to_market()

    def snapshot(self) -> None:
        self._mark_to_market()
        self.equity_curve.append(self._equity)
        self.balance_curve.append(self._balance)

    def balance(self) -> float:
        return self._balance

    def equity(self) -> float:
        return self._equity

    def avg_spread_pips(self) -> float:
        return sum(self.spread_pips_curve) / len(self.spread_pips_curve) if self.spread_pips_curve else 0.0

    def _find_order(self, ticket: int) -> Optional[Order]:
        for o in self._orders:
            if o.ticket == ticket:
                return o
        return None

    def _used_margin(self) -> float:
        used = 0.0
        for o in self._orders:
            if o.type in (OrderType.BUY, OrderType.SELL):
                used += o.lots * self.margin_per_lot(self.symbol)
        return used

    def _mark_to_market(self) -> None:
        floating = 0.0
        for o in self._orders:
            if o.type == OrderType.BUY:
                o.profit = self._pnl_buy(o.lots, o.open_price, self._bid)
            elif o.type == OrderType.SELL:
                o.profit = self._pnl_sell(o.lots, o.open_price, self._ask)
            else:
                o.profit = 0.0
            floating += o.total_profit
        self._equity = self._balance + floating

    def _dynamic_spread_pips(self, bar: Bar) -> float:
        # Finer spread model: base + volatility + session + small random microstructure noise.
        base = 0.55
        range_pips = max((bar.high - bar.low) / 0.0001, 0.0)
        vol_part = min(1.6, 0.018 * range_pips)

        hour = datetime.fromtimestamp(bar.ts, tz=timezone.utc).hour
        if 21 <= hour or hour <= 1:
            session = 0.45
        elif 6 <= hour <= 15:
            session = 0.0
        else:
            session = 0.15

        noise = self._rand.uniform(-0.08, 0.12)
        return min(3.0, max(0.25, base + vol_part + session + noise))

    def _apply_slippage_price(self, price: float, lots: float, side: str) -> float:
        # side='buy' => worse price is up, side='sell' => worse price is down.
        # Size-aware + volatility-aware slippage in pips.
        if not self.current_bar:
            return price

        range_pips = max((self.current_bar.high - self.current_bar.low) / 0.0001, 0.0)
        vol_component = min(1.2, 0.012 * range_pips)
        size_component = min(0.6, max(0.0, lots - 0.05) * 0.18)
        noise = abs(self._rand.gauss(0.0, 0.10))
        slip_pips = min(2.5, 0.08 + vol_component + size_component + noise)
        slip = slip_pips * 0.0001

        if side == "buy":
            return round(price + slip, self._digits)
        return round(price - slip, self._digits)

    @staticmethod
    def _pnl_buy(lots: float, open_price: float, close_bid: float) -> float:
        units = 100_000.0 * lots
        if close_bid <= 0:
            return 0.0
        return units * (close_bid - open_price) / close_bid

    @staticmethod
    def _pnl_sell(lots: float, open_price: float, close_ask: float) -> float:
        units = 100_000.0 * lots
        if close_ask <= 0:
            return 0.0
        return units * (open_price - close_ask) / close_ask


def download_usdchf_5m(from_d: date, to_d: date) -> Path:
    out_dir = Path(__file__).resolve().parent / "download"
    out_dir.mkdir(parents=True, exist_ok=True)
    merged_name = f"usdchf-m5-bid-{from_d.isoformat()}-{to_d.isoformat()}-merged.csv"
    merged_path = out_dir / merged_name
    if merged_path.exists() and merged_path.stat().st_size > 0:
        return merged_path

    parts: List[Path] = []
    cur = from_d
    while cur <= to_d:
        part_to = min(cur + timedelta(days=365), to_d)
        part_name = f"usdchf-m5-bid-{cur.isoformat()}-{part_to.isoformat()}.csv"
        part_path = out_dir / part_name

        if not part_path.exists() or part_path.stat().st_size == 0:
            cmd = [
                "npx",
                "-y",
                "dukascopy-node",
                "-i",
                "usdchf",
                "-from",
                cur.isoformat(),
                "-to",
                part_to.isoformat(),
                "-t",
                "m5",
                "-f",
                "csv",
            ]
            subprocess.run(cmd, cwd=str(Path(__file__).resolve().parent), check=True)

        if not part_path.exists():
            raise RuntimeError(f"数据下载完成但未找到文件: {part_path}")
        parts.append(part_path)
        cur = part_to + timedelta(days=1)

    rows_by_ts = {}
    for p in parts:
        with p.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                ts = r.get("timestamp")
                if not ts:
                    continue
                rows_by_ts[ts] = r

    sorted_ts = sorted(rows_by_ts.keys(), key=lambda x: int(x))
    with merged_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close"])
        for ts in sorted_ts:
            r = rows_by_ts[ts]
            w.writerow([ts, r["open"], r["high"], r["low"], r["close"]])

    return merged_path


def load_bars_from_csv(path: Path) -> List[Bar]:
    bars: List[Bar] = []
    with path.open("r", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        for r in rows:
            try:
                ts = int(r["timestamp"]) // 1000
                o = float(r["open"])
                h = float(r["high"])
                l = float(r["low"])
                c = float(r["close"])
            except Exception:
                continue
            bars.append(Bar(ts=ts, open=o, high=h, low=l, close=c))

    bars.sort(key=lambda x: x.ts)
    return bars


def calc_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def run_backtest(years: int = 1) -> None:
    end_d = date.today()
    start_d = end_d - timedelta(days=365 * years)

    csv_path = download_usdchf_5m(start_d, end_d)
    bars = load_bars_from_csv(csv_path)
    if not bars:
        raise RuntimeError("5m 数据为空")

    broker = SimBroker(symbol="USDCHF", initial_balance=10_000.0, leverage=100, seed=20260226)

    cfg = Config(
        symbol="USDCHF",
        magic=9453,
        open_mode=OpenMode.BAR,
        sleep_seconds=0,
    )

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
    total = sum(closed)
    wins = [x for x in closed if x > 0]
    losses = [x for x in closed if x <= 0]
    max_dd = calc_max_drawdown(broker.equity_curve) * 100.0

    first_dt = datetime.fromtimestamp(bars[0].ts, tz=timezone.utc)
    last_dt = datetime.fromtimestamp(bars[-1].ts, tz=timezone.utc)

    print(f"=== Amazing3.1 USDCHF Backtest (M5, Last {years}Y) ===")
    print(f"Period (UTC): {first_dt} -> {last_dt}")
    print(f"Bars: {len(bars)}")
    print("Data: dukascopy-node (bid m5)")
    print("Execution model: dynamic spread + size/volatility slippage")
    print(f"Initial balance: 10000.00")
    print(f"Final balance: {broker.balance():.2f}")
    print(f"Net profit: {total:.2f}")
    print(f"Closed trades: {len(closed)}")
    print(f"Win rate: {(len(wins) / len(closed) * 100.0 if closed else 0.0):.2f}%")
    print(f"Avg win: {(sum(wins) / len(wins) if wins else 0.0):.2f}")
    print(f"Avg loss: {(sum(losses) / len(losses) if losses else 0.0):.2f}")
    print(f"Profit factor: {(abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 0.0):.3f}")
    print(f"Max drawdown: {max_dd:.2f}%")
    print(f"Average spread: {broker.avg_spread_pips():.3f} pips")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest Amazing3.1 on USDCHF M5")
    parser.add_argument("--years", type=int, default=1, help="How many years back from today")
    args = parser.parse_args()
    run_backtest(years=max(1, args.years))
