from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import List, Optional, Protocol
import math
import time


class OrderType(IntEnum):
    BUY = 0
    SELL = 1
    BUYSTOP = 4
    SELLSTOP = 5


class OpenMode(IntEnum):
    BAR = 1
    SLEEP = 2
    ALWAYS = 3


@dataclass
class Order:
    ticket: int
    symbol: str
    magic: int
    type: OrderType
    lots: float
    open_price: float
    profit: float
    swap: float = 0.0
    commission: float = 0.0
    comment: str = ""
    open_time: int = 0

    @property
    def total_profit(self) -> float:
        return self.profit + self.swap + self.commission


class Broker(Protocol):
    def get_orders(self) -> List[Order]: ...
    def get_bid_ask(self) -> tuple[float, float]: ...
    def digits(self) -> int: ...
    def point(self) -> float: ...
    def spread_points(self) -> float: ...
    def account_leverage(self) -> int: ...
    def free_margin(self) -> float: ...
    def margin_per_lot(self, symbol: str) -> float: ...
    def is_trade_allowed(self) -> bool: ...
    def is_expert_enabled(self) -> bool: ...
    def is_stopped(self) -> bool: ...
    def send_pending(self, order_type: OrderType, lots: float, price: float, comment: str) -> Optional[int]: ...
    def modify_order(self, ticket: int, new_price: float) -> bool: ...
    def close_order(self, ticket: int) -> bool: ...
    def delete_order(self, ticket: int) -> bool: ...


@dataclass
class Config:
    symbol: str
    magic: int = 9453
    totals: int = 50
    max_spread: float = 32
    leverage_min: int = 100

    close_buy_sell: bool = True
    homeopathy_close_all: bool = True
    homeopathy: bool = False
    over: bool = False
    next_time: int = 0

    money: float = 0.0
    first_step: int = 30
    min_distance: int = 60
    two_min_distance: int = 60
    step_trail_orders: int = 5
    step: int = 100
    two_step: int = 100

    open_mode: OpenMode = OpenMode.ALWAYS
    sleep_seconds: int = 30

    max_loss: float = -100000.0
    max_loss_close_all: float = -50.0
    lot: float = 0.01
    max_lot: float = 10.0
    plus_lot: float = 0.0
    k_lot: float = 1.3
    digits_lot: int = 2

    close_all: float = 0.5
    profit_by_count: bool = True
    stop_profit: float = 2.0
    stop_loss: float = 0.0

    on_top_not_buy_first: float = 0.0
    on_under_not_sell_first: float = 0.0
    on_top_not_buy_add: float = 0.0
    on_under_not_sell_add: float = 0.0

    ea_start_time: str = "00:00"
    ea_stop_time: str = "24:00"
    limit_start_time: str = "00:00"
    limit_stop_time: str = "24:00"
    check_margin_for_add_orders: bool = False

    def __post_init__(self) -> None:
        # MT4 init() flips these extern values to negative internally.
        if self.max_loss_close_all > 0:
            self.max_loss_close_all = -self.max_loss_close_all
        if self.max_loss > 0:
            self.max_loss = -self.max_loss
        if self.stop_loss > 0:
            self.stop_loss = -self.stop_loss
        if self.money > 0:
            self.money = -self.money

        self.ea_start_time = self._clean_time(self.ea_start_time)
        self.ea_stop_time = self._clean_time(self.ea_stop_time)
        self.limit_start_time = self._clean_time(self.limit_start_time)
        self.limit_stop_time = self._clean_time(self.limit_stop_time)

    @staticmethod
    def _clean_time(value: str) -> str:
        s = value.strip().replace(" ", "")
        return "23:59:59" if s == "24:00" else s


@dataclass
class State:
    pause_until: int = 0
    last_bar_time: int = 0
    peak_buy_diff: float = 0.0
    peak_sell_diff: float = 0.0


class Amazing31:
    def __init__(self, broker: Broker, cfg: Config):
        self.broker = broker
        self.cfg = cfg
        self.state = State()

    def _n(self, value: float) -> float:
        return round(value, self.broker.digits())

    @staticmethod
    def lizong_8(timeframe: int, current_period: int) -> int:
        if timeframe > 43200:
            return 0
        if timeframe > 10080:
            return 43200
        if timeframe > 1440:
            return 10080
        if timeframe > 240:
            return 1440
        if timeframe > 60:
            return 240
        if timeframe > 30:
            return 60
        if timeframe > 15:
            return 30
        if timeframe > 5:
            return 15
        if timeframe > 1:
            return 5
        if timeframe == 1:
            return 1
        if timeframe == 0:
            return current_period
        return 0

    def _orders(self) -> List[Order]:
        return [
            o for o in self.broker.get_orders()
            if o.symbol == self.cfg.symbol and o.magic == self.cfg.magic
        ]

    def lizong_10(self, order_type: int, sign_mode: int, top_n: int) -> float:
        vals: List[float] = []
        for o in self._orders():
            if order_type != -100 and o.type != order_type:
                continue
            if sign_mode == 1 and o.profit >= 0:
                vals.append(o.profit)
            elif sign_mode == 2 and o.profit < 0:
                vals.append(-o.profit)
        vals.sort(reverse=True)
        return sum(vals[:top_n])

    def lizong_9(self, order_type: int, count: int, mode: int) -> None:
        while count > 0:
            pool = [o for o in self._orders() if o.type == order_type]
            if not pool:
                return
            pool.sort(key=lambda x: x.profit, reverse=(mode == 1))
            target = pool[0]
            if mode == 1 and target.profit >= 0 and self.broker.close_order(target.ticket):
                count -= 1
            elif mode == 1 and target.profit < 0:
                count -= 1
            elif mode == 2 and target.profit < 0 and self.broker.close_order(target.ticket):
                count -= 1
            elif mode == 2 and target.profit >= 0:
                count -= 1
            else:
                return

    def lizong_7(self, side: int) -> bool:
        for _ in range(10):
            remain = 0
            for o in self._orders():
                ok = True
                if o.type in (OrderType.BUY, OrderType.BUYSTOP) and side in (1, 0):
                    ok = self.broker.close_order(o.ticket) if o.type == OrderType.BUY else self.broker.delete_order(o.ticket)
                elif o.type in (OrderType.SELL, OrderType.SELLSTOP) and side in (-1, 0):
                    ok = self.broker.close_order(o.ticket) if o.type == OrderType.SELL else self.broker.delete_order(o.ticket)
                else:
                    continue
                if not ok:
                    remain += 1
            if remain == 0:
                return True
            time.sleep(1)
        return False

    @staticmethod
    def _time_to_seconds(value: str) -> int:
        parts = value.split(":")
        if len(parts) == 2:
            h, m = parts
            s = "0"
        elif len(parts) == 3:
            h, m, s = parts
        else:
            return 0
        try:
            hh = max(0, min(int(h), 23))
            mm = max(0, min(int(m), 59))
            ss = max(0, min(int(s), 59))
        except ValueError:
            return 0
        return hh * 3600 + mm * 60 + ss

    def _in_time_window(self, now_ts: int, start: str, stop: str) -> bool:
        now = datetime.utcfromtimestamp(now_ts)
        cur = now.hour * 3600 + now.minute * 60 + now.second
        start_s = self._time_to_seconds(start)
        stop_s = self._time_to_seconds(stop)
        if start_s <= stop_s:
            return start_s <= cur <= stop_s
        return cur >= start_s or cur <= stop_s

    @staticmethod
    def _count_ss(side_orders: List[Order]) -> int:
        return sum(1 for o in side_orders if o.comment == "SS")

    @staticmethod
    def _latest_open_time(side_orders: List[Order]) -> int:
        latest_ticket = -1
        latest_open = 0
        for o in side_orders:
            if o.ticket > latest_ticket:
                latest_ticket = o.ticket
                latest_open = o.open_time
        return latest_open

    def on_tick(self, now_ts: int, current_bar_ts: int) -> None:
        bid, ask = self.broker.get_bid_ask()
        pt = self.broker.point()

        orders = self._orders()
        buys = [o for o in orders if o.type == OrderType.BUY]
        sells = [o for o in orders if o.type == OrderType.SELL]
        buystops = [o for o in orders if o.type == OrderType.BUYSTOP]
        sellstops = [o for o in orders if o.type == OrderType.SELLSTOP]

        buy_profit = sum(o.total_profit for o in buys)
        sell_profit = sum(o.total_profit for o in sells)
        total_profit = buy_profit + sell_profit

        buy_lots = sum(o.lots for o in buys)
        sell_lots = sum(o.lots for o in sells)

        buy_high = max([o.open_price for o in buys + buystops], default=0.0)
        buy_low = min([o.open_price for o in buys], default=0.0)
        sell_low = min([o.open_price for o in sells + sellstops], default=0.0)
        sell_high = max([o.open_price for o in sells], default=0.0)
        buy_ss_count = self._count_ss(buys)
        sell_ss_count = self._count_ss(sells)
        sell_ss_when_no_buy_ss = sell_ss_count if buy_ss_count < 1 else 0

        can_buy = True
        can_sell = True

        if not self._in_time_window(now_ts, self.cfg.ea_start_time, self.cfg.ea_stop_time):
            can_buy = False
            can_sell = False

        if (
            self.broker.account_leverage() < self.cfg.leverage_min
            or not self.broker.is_trade_allowed()
            or not self.broker.is_expert_enabled()
            or self.broker.is_stopped()
            or len(buys) + len(sells) >= self.cfg.totals
            or self.broker.spread_points() > self.cfg.max_spread
        ):
            can_buy = False
            can_sell = False

        if now_ts < self.state.pause_until:
            can_buy = False
            can_sell = False

        if self.cfg.over and len(buys) == 0:
            can_buy = False
        if self.cfg.over and len(sells) == 0:
            can_sell = False

        if self.cfg.over and total_profit >= self.cfg.close_all:
            self.lizong_7(0)
            if self.cfg.next_time > 0:
                self.state.pause_until = now_ts + self.cfg.next_time
            return

        if not self.cfg.over:
            if (
                (sell_ss_when_no_buy_ss < 1 or not self.cfg.homeopathy_close_all)
                and buy_profit > self.cfg.max_loss_close_all
                and sell_profit > self.cfg.max_loss_close_all
            ):
                if (
                    (self.cfg.profit_by_count and buy_profit > self.cfg.stop_profit * len(buys))
                    or (not self.cfg.profit_by_count and buy_profit > self.cfg.stop_profit)
                ):
                    self.lizong_7(1)
                    return
                if (
                    (self.cfg.profit_by_count and sell_profit > self.cfg.stop_profit * len(sells))
                    or (not self.cfg.profit_by_count and sell_profit > self.cfg.stop_profit)
                ):
                    self.lizong_7(-1)
                    return

            if (
                self.cfg.homeopathy_close_all
                and (buy_ss_count > 0 or sell_ss_count > 0)
                and total_profit >= self.cfg.close_all
            ):
                self.lizong_7(0)
                if self.cfg.next_time > 0:
                    self.state.pause_until = now_ts + self.cfg.next_time
                return

            if (
                total_profit >= self.cfg.close_all
                and (buy_profit <= self.cfg.max_loss_close_all or sell_profit <= self.cfg.max_loss_close_all)
            ):
                self.lizong_7(0)
                if self.cfg.next_time > 0:
                    self.state.pause_until = now_ts + self.cfg.next_time
                return

        if self.cfg.stop_loss != 0.0 and total_profit <= self.cfg.stop_loss:
            self.lizong_7(0)
            if self.cfg.next_time > 0:
                self.state.pause_until = now_ts + self.cfg.next_time
            return

        # CloseBuySell: close strongest side partial + weakest opposite side partial.
        if self.cfg.close_buy_sell:
            buy_diff = self.lizong_10(OrderType.BUY, 1, 1) - self.lizong_10(OrderType.BUY, 2, 2)
            self.state.peak_buy_diff = max(self.state.peak_buy_diff, buy_diff)
            if self.state.peak_buy_diff > 0 and buy_diff > 0 and buy_lots > 0 and len(buys) > 3:
                best_buy = max((o.profit for o in buys), default=0.0)
                best_buy_lot = next((o.lots for o in buys if o.profit == best_buy), 0.0)
                if buy_lots > best_buy_lot * 3 + sell_lots:
                    self.lizong_9(OrderType.BUY, 1, 1)
                    self.lizong_9(OrderType.BUY, 2, 2)
                    self.state.peak_buy_diff = 0.0
                    self.state.peak_sell_diff = 0.0

            sell_diff = self.lizong_10(OrderType.SELL, 1, 1) - self.lizong_10(OrderType.SELL, 2, 2)
            self.state.peak_sell_diff = max(self.state.peak_sell_diff, sell_diff)
            if self.state.peak_sell_diff > 0 and sell_diff > 0 and sell_lots > 0 and len(sells) > 3:
                best_sell = max((o.profit for o in sells), default=0.0)
                best_sell_lot = next((o.lots for o in sells if o.profit == best_sell), 0.0)
                if sell_lots > best_sell_lot * 3 + buy_lots:
                    self.lizong_9(OrderType.SELL, 1, 1)
                    self.lizong_9(OrderType.SELL, 2, 2)
                    self.state.peak_buy_diff = 0.0
                    self.state.peak_sell_diff = 0.0

        aggressive_mode = (self.cfg.money == 0.0 or total_profit > self.cfg.money)

        open_gate = (
            (self.cfg.open_mode == OpenMode.BAR and self.state.last_bar_time != current_bar_ts)
            or self.cfg.open_mode in (OpenMode.SLEEP, OpenMode.ALWAYS)
        )

        if open_gate:
            buy_last_open = self._latest_open_time(buys)
            sell_last_open = self._latest_open_time(sells)
            limit_window = self._in_time_window(now_ts, self.cfg.limit_start_time, self.cfg.limit_stop_time)
            self._try_open_buy(
                now_ts,
                can_buy,
                ask,
                pt,
                buys,
                buystops,
                buy_profit,
                buy_lots,
                sell_lots,
                buy_high,
                buy_low,
                aggressive_mode,
                buy_last_open,
                limit_window,
            )
            self._try_open_sell(
                now_ts,
                can_sell,
                bid,
                pt,
                sells,
                sellstops,
                sell_profit,
                sell_lots,
                buy_lots,
                sell_low,
                sell_high,
                aggressive_mode,
                sell_last_open,
                limit_window,
            )
            self.state.last_bar_time = current_bar_ts

        self._trail_pending_buy(can_buy, ask, pt, len(buys), aggressive_mode, buy_high, buy_low, buystops)
        self._trail_pending_sell(can_sell, bid, pt, len(sells), aggressive_mode, sell_low, sell_high, sellstops)

    def _calc_lot(self, side_count: int) -> float:
        if side_count == 0:
            x = self.cfg.lot
        else:
            x = self.cfg.lot * math.pow(self.cfg.k_lot, side_count) + side_count * self.cfg.plus_lot
        return round(min(x, self.cfg.max_lot), self.cfg.digits_lot)

    def _can_afford(self, lots: float) -> bool:
        if not self.cfg.check_margin_for_add_orders:
            return True
        need = self.broker.margin_per_lot(self.cfg.symbol)
        if need <= 0:
            return True
        return lots * 2.0 < self.broker.free_margin() / need

    def _try_open_buy(
        self,
        now_ts: int,
        can_buy: bool,
        ask: float,
        pt: float,
        buys: List[Order],
        buystops: List[Order],
        buy_profit: float,
        buy_lots: float,
        sell_lots: float,
        buy_high: float,
        buy_low: float,
        aggressive_mode: bool,
        last_open_time: int,
        limit_window: bool,
    ) -> None:
        if buystops or buy_profit <= self.cfg.max_loss or not can_buy:
            return
        if self.cfg.open_mode == OpenMode.SLEEP and now_ts - last_open_time < self.cfg.sleep_seconds:
            return
        count = len(buys)
        if count == 0:
            px = self._n(ask + self.cfg.first_step * pt)
        else:
            base = self.cfg.min_distance if aggressive_mode else self.cfg.two_min_distance
            step = self.cfg.step if aggressive_mode else self.cfg.two_step
            px = self._n(ask + base * pt)
            if buy_low > 0 and px < self._n(buy_low - step * pt):
                px = self._n(ask + step * pt)

        cond = (
            count == 0
            or (buy_high > 0 and px >= self._n(buy_high + (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt)
                and sell_lots > buy_lots * 3 and sell_lots - buy_lots > 0.2)
            or (buy_low > 0 and px <= self._n(buy_low - (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt))
            or (self.cfg.homeopathy and buy_high > 0 and px >= self._n(buy_high + self.cfg.step * pt) and buy_lots == sell_lots)
        )
        if not cond:
            return

        lots = self._calc_lot(count)
        if count > 0 and not self._can_afford(lots):
            return

        if (
            count > 0
            and limit_window
            and self.cfg.on_top_not_buy_add != 0.0
            and px >= self.cfg.on_top_not_buy_add
        ):
            return

        ss_comment = (
            (buy_high > 0 and px >= self._n(buy_high + (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt)
             and sell_lots > buy_lots * 3 and sell_lots - buy_lots > 0.2)
            or (self.cfg.homeopathy and buy_high > 0 and px >= self._n(buy_high + self.cfg.step * pt) and buy_lots == sell_lots)
        )
        comment = "SS" if ss_comment else "NN"
        self.broker.send_pending(OrderType.BUYSTOP, lots, px, comment)

    def _try_open_sell(
        self,
        now_ts: int,
        can_sell: bool,
        bid: float,
        pt: float,
        sells: List[Order],
        sellstops: List[Order],
        sell_profit: float,
        sell_lots: float,
        buy_lots: float,
        sell_low: float,
        sell_high: float,
        aggressive_mode: bool,
        last_open_time: int,
        limit_window: bool,
    ) -> None:
        if sellstops or sell_profit <= self.cfg.max_loss or not can_sell:
            return
        if self.cfg.open_mode == OpenMode.SLEEP and now_ts - last_open_time < self.cfg.sleep_seconds:
            return
        count = len(sells)
        if count == 0:
            px = self._n(bid - self.cfg.first_step * pt)
        else:
            base = self.cfg.min_distance if aggressive_mode else self.cfg.two_min_distance
            step = self.cfg.step if aggressive_mode else self.cfg.two_step
            px = self._n(bid - base * pt)
            if sell_high > 0 and px < self._n(sell_high + step * pt):
                px = self._n(bid - step * pt)

        cond = (
            count == 0
            or (sell_low > 0 and px <= self._n(sell_low - (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt)
                and buy_lots > sell_lots * 3 and buy_lots - sell_lots > 0.2)
            or (sell_high > 0 and px >= self._n(sell_high + (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt))
            or (self.cfg.homeopathy and sell_low > 0 and px <= self._n(sell_low - self.cfg.step * pt) and buy_lots == sell_lots)
        )
        if not cond:
            return

        lots = self._calc_lot(count)
        if count > 0 and not self._can_afford(lots):
            return

        if (
            count > 0
            and limit_window
            and self.cfg.on_under_not_sell_add != 0.0
            and px <= self.cfg.on_under_not_sell_add
        ):
            return

        ss_comment = (
            (sell_low > 0 and px <= self._n(sell_low - (self.cfg.step if aggressive_mode else self.cfg.two_step) * pt)
             and buy_lots > sell_lots * 3 and buy_lots - sell_lots > 0.2)
            or (self.cfg.homeopathy and sell_low > 0 and px <= self._n(sell_low - self.cfg.step * pt) and buy_lots == sell_lots)
        )
        comment = "SS" if ss_comment else "NN"
        self.broker.send_pending(OrderType.SELLSTOP, lots, px, comment)

    def _trail_pending_buy(self, can_buy: bool, ask: float, pt: float, buy_count: int, aggressive_mode: bool,
                           buy_high: float, buy_low: float, buystops: List[Order]) -> None:
        if not can_buy or not buystops:
            return
        pending = max(buystops, key=lambda x: x.open_price)
        px = self._n(ask + (self.cfg.first_step if buy_count == 0 else (self.cfg.min_distance if aggressive_mode else self.cfg.two_min_distance)) * pt)
        if self._n(pending.open_price - self.cfg.step_trail_orders * pt) > px:
            step = self.cfg.step if aggressive_mode else self.cfg.two_step
            cond = (buy_low == 0.0 or px <= self._n(buy_low - step * pt) or px >= self._n(buy_high + step * pt))
            if cond:
                self.broker.modify_order(pending.ticket, px)

    def _trail_pending_sell(self, can_sell: bool, bid: float, pt: float, sell_count: int, aggressive_mode: bool,
                            sell_low: float, sell_high: float, sellstops: List[Order]) -> None:
        if not can_sell or not sellstops:
            return
        pending = min(sellstops, key=lambda x: x.open_price)
        px = self._n(bid - (self.cfg.first_step if sell_count == 0 else (self.cfg.min_distance if aggressive_mode else self.cfg.two_min_distance)) * pt)
        if self._n(pending.open_price + self.cfg.step_trail_orders * pt) < px:
            step = self.cfg.step if aggressive_mode else self.cfg.two_step
            cond = (sell_high == 0.0 or px >= self._n(sell_high + step * pt) or px <= self._n(sell_low - step * pt))
            if cond:
                self.broker.modify_order(pending.ticket, px)
