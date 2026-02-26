use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const INITIAL_BALANCE: f64 = 10_000.0;
const MAGIC: i32 = 9453;
const LEVERAGE: i32 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum OrderType {
    Buy = 0,
    Sell = 1,
    BuyStop = 4,
    SellStop = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum OpenMode {
    Bar = 1,
    Sleep = 2,
    Always = 3,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    ticket: i64,
    symbol: String,
    magic: i32,
    order_type: OrderType,
    lots: f64,
    open_price: f64,
    profit: f64,
    swap: f64,
    commission: f64,
    comment: String,
    open_time: i64,
}

impl Order {
    fn total_profit(&self) -> f64 {
        self.profit + self.swap + self.commission
    }
}

#[derive(Clone, Debug)]
struct Bar {
    ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct Config {
    symbol: String,
    magic: i32,
    totals: i32,
    max_spread: f64,
    leverage_min: i32,

    close_buy_sell: bool,
    homeopathy_close_all: bool,
    homeopathy: bool,
    over: bool,
    next_time: i64,

    money: f64,
    first_step: i32,
    min_distance: i32,
    two_min_distance: i32,
    step_trail_orders: i32,
    step: i32,
    two_step: i32,

    open_mode: OpenMode,
    sleep_seconds: i64,

    max_loss: f64,
    max_loss_close_all: f64,
    lot: f64,
    max_lot: f64,
    plus_lot: f64,
    k_lot: f64,
    digits_lot: i32,

    close_all: f64,
    profit_by_count: bool,
    stop_profit: f64,
    stop_loss: f64,

    on_top_not_buy_first: f64,
    on_under_not_sell_first: f64,
    on_top_not_buy_add: f64,
    on_under_not_sell_add: f64,

    ea_start_time: String,
    ea_stop_time: String,
    limit_start_time: String,
    limit_stop_time: String,

    check_margin_for_add_orders: bool,
}

impl Config {
    fn from_params(symbol: &str, params: &Map<String, Value>) -> Self {
        let mut max_loss = get_f64(params, "max_loss", 100_000.0);
        let mut max_loss_close_all = get_f64(params, "max_loss_close_all", 50.0);
        let mut stop_loss = get_f64(params, "stop_loss", 0.0);
        let mut money = get_f64(params, "money", 0.0);

        if max_loss > 0.0 {
            max_loss = -max_loss;
        }
        if max_loss_close_all > 0.0 {
            max_loss_close_all = -max_loss_close_all;
        }
        if stop_loss > 0.0 {
            stop_loss = -stop_loss;
        }
        if money > 0.0 {
            money = -money;
        }

        let open_mode_raw = get_i64(params, "open_mode", 1);
        let open_mode = match open_mode_raw {
            1 => OpenMode::Bar,
            2 => OpenMode::Sleep,
            3 => OpenMode::Always,
            _ => OpenMode::Bar,
        };

        Self {
            symbol: symbol.to_string(),
            magic: MAGIC,
            totals: get_i64(params, "totals", 50) as i32,
            max_spread: get_f64(params, "max_spread", 32.0),
            leverage_min: get_i64(params, "leverage_min", 100) as i32,

            close_buy_sell: get_bool(params, "close_buy_sell", true),
            homeopathy_close_all: get_bool(params, "homeopathy_close_all", true),
            homeopathy: get_bool(params, "homeopathy", false),
            over: get_bool(params, "over", false),
            next_time: get_i64(params, "next_time", 0),

            money,
            first_step: get_i64(params, "first_step", 30) as i32,
            min_distance: get_i64(params, "min_distance", 60) as i32,
            two_min_distance: get_i64(params, "two_min_distance", 60) as i32,
            step_trail_orders: get_i64(params, "step_trail_orders", 5) as i32,
            step: get_i64(params, "step", 100) as i32,
            two_step: get_i64(params, "two_step", 100) as i32,

            open_mode,
            sleep_seconds: get_i64(params, "sleep_seconds", 30),

            max_loss,
            max_loss_close_all,
            lot: get_f64(params, "lot", 0.01),
            max_lot: get_f64(params, "max_lot", 10.0),
            plus_lot: get_f64(params, "plus_lot", 0.0),
            k_lot: get_f64(params, "k_lot", 1.3),
            digits_lot: get_i64(params, "digits_lot", 3) as i32,

            close_all: get_f64(params, "close_all", 0.5),
            profit_by_count: get_bool(params, "profit_by_count", true),
            stop_profit: get_f64(params, "stop_profit", 2.0),
            stop_loss,

            on_top_not_buy_first: get_f64(params, "on_top_not_buy_first", 0.0),
            on_under_not_sell_first: get_f64(params, "on_under_not_sell_first", 0.0),
            on_top_not_buy_add: get_f64(params, "on_top_not_buy_add", 0.0),
            on_under_not_sell_add: get_f64(params, "on_under_not_sell_add", 0.0),

            ea_start_time: clean_time(get_string(params, "ea_start_time", "00:00")),
            ea_stop_time: clean_time(get_string(params, "ea_stop_time", "24:00")),
            limit_start_time: clean_time(get_string(params, "limit_start_time", "00:00")),
            limit_stop_time: clean_time(get_string(params, "limit_stop_time", "24:00")),

            check_margin_for_add_orders: get_bool(params, "check_margin_for_add_orders", false),
        }
    }
}

fn clean_time(s: String) -> String {
    let t = s.trim().replace(' ', "");
    if t == "24:00" {
        "23:59:59".to_string()
    } else {
        t
    }
}

#[derive(Clone, Debug)]
struct State {
    pause_until: i64,
    last_bar_time: i64,
    peak_buy_diff: f64,
    peak_sell_diff: f64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            pause_until: 0,
            last_bar_time: 0,
            peak_buy_diff: 0.0,
            peak_sell_diff: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
struct SimBroker {
    symbol: String,
    balance: f64,
    equity: f64,
    leverage: i32,

    digits: i32,
    point: f64,

    orders: Vec<Order>,
    next_ticket: i64,

    current_bar: Option<Bar>,
    bid: f64,
    ask: f64,
    spread_points: f64,

    closed_pnls: Vec<f64>,
    equity_curve: Vec<f64>,
    balance_curve: Vec<f64>,
    spread_pips_curve: Vec<f64>,

    rng: StdRng,
}

impl SimBroker {
    fn new(symbol: &str, initial_balance: f64, leverage: i32, seed: u64) -> Self {
        Self {
            symbol: symbol.to_string(),
            balance: initial_balance,
            equity: initial_balance,
            leverage,
            digits: 5,
            point: 0.00001,
            orders: Vec::new(),
            next_ticket: 1,
            current_bar: None,
            bid: 0.0,
            ask: 0.0,
            spread_points: 0.0,
            closed_pnls: Vec::new(),
            equity_curve: Vec::new(),
            balance_curve: Vec::new(),
            spread_pips_curve: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn get_orders(&self) -> &[Order] {
        &self.orders
    }

    fn get_bid_ask(&self) -> (f64, f64) {
        (self.bid, self.ask)
    }

    fn free_margin(&self) -> f64 {
        self.equity - self.used_margin()
    }

    fn margin_per_lot(&self, symbol: &str) -> f64 {
        if symbol != self.symbol {
            0.0
        } else {
            100_000.0 / self.leverage as f64
        }
    }

    fn is_trade_allowed(&self) -> bool {
        true
    }

    fn is_expert_enabled(&self) -> bool {
        true
    }

    fn is_stopped(&self) -> bool {
        false
    }

    fn send_pending(
        &mut self,
        order_type: OrderType,
        lots: f64,
        price: f64,
        comment: &str,
    ) -> Option<i64> {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.orders.push(Order {
            ticket,
            symbol: self.symbol.clone(),
            magic: MAGIC,
            order_type,
            lots,
            open_price: round_to(price, self.digits),
            profit: 0.0,
            swap: 0.0,
            commission: 0.0,
            comment: comment.to_string(),
            open_time: self.current_bar.as_ref().map(|b| b.ts).unwrap_or(0),
        });
        Some(ticket)
    }

    fn modify_order(&mut self, ticket: i64, new_price: f64) -> bool {
        if let Some(o) = self.orders.iter_mut().find(|o| o.ticket == ticket) {
            if matches!(o.order_type, OrderType::BuyStop | OrderType::SellStop) {
                o.open_price = round_to(new_price, self.digits);
                return true;
            }
        }
        false
    }

    fn close_order(&mut self, ticket: i64) -> bool {
        let Some(idx) = self.orders.iter().position(|o| o.ticket == ticket) else {
            return false;
        };
        let o = self.orders[idx].clone();

        if matches!(o.order_type, OrderType::BuyStop | OrderType::SellStop) {
            return self.delete_order(ticket);
        }

        let pnl = match o.order_type {
            OrderType::Buy => {
                let close_px = self.apply_slippage_price(self.bid, o.lots, true);
                self.pnl_buy(o.lots, o.open_price, close_px)
            }
            OrderType::Sell => {
                let close_px = self.apply_slippage_price(self.ask, o.lots, false);
                self.pnl_sell(o.lots, o.open_price, close_px)
            }
            _ => 0.0,
        };

        self.balance += pnl;
        self.closed_pnls.push(pnl);
        self.orders.remove(idx);
        self.mark_to_market();
        true
    }

    fn delete_order(&mut self, ticket: i64) -> bool {
        let n0 = self.orders.len();
        self.orders.retain(|o| o.ticket != ticket);
        self.orders.len() != n0
    }

    fn on_bar(&mut self, bar: &Bar) {
        self.current_bar = Some(bar.clone());

        let spread_pips = self.dynamic_spread_pips(bar);
        self.spread_points = spread_pips * 10.0;

        let half_spread = (self.spread_points * self.point) / 2.0;
        let mid = bar.close;
        self.bid = round_to(mid - half_spread, self.digits);
        self.ask = round_to(mid + half_spread, self.digits);
        self.spread_pips_curve.push(spread_pips);
    }

    fn trigger_pending_from_bar(&mut self) {
        let Some(ohlc) = self.current_bar.clone() else {
            return;
        };

        for idx in 0..self.orders.len() {
            let (order_type, open_price, lots) = {
                let o = &self.orders[idx];
                (o.order_type, o.open_price, o.lots)
            };

            if order_type == OrderType::BuyStop && ohlc.high >= open_price {
                let base_fill = ohlc.open.max(open_price);
                let fill = self.apply_slippage_price(base_fill, lots, true);
                let o = &mut self.orders[idx];
                o.order_type = OrderType::Buy;
                o.open_price = round_to(fill, self.digits);
                o.open_time = ohlc.ts;
            } else if order_type == OrderType::SellStop && ohlc.low <= open_price {
                let base_fill = ohlc.open.min(open_price);
                let fill = self.apply_slippage_price(base_fill, lots, false);
                let o = &mut self.orders[idx];
                o.order_type = OrderType::Sell;
                o.open_price = round_to(fill, self.digits);
                o.open_time = ohlc.ts;
            }
        }

        self.mark_to_market();
    }

    fn snapshot(&mut self) {
        self.mark_to_market();
        self.equity_curve.push(self.equity);
        self.balance_curve.push(self.balance);
    }

    fn used_margin(&self) -> f64 {
        self.orders
            .iter()
            .filter(|o| matches!(o.order_type, OrderType::Buy | OrderType::Sell))
            .map(|o| o.lots * self.margin_per_lot(&self.symbol))
            .sum()
    }

    fn mark_to_market(&mut self) {
        let bid = self.bid;
        let ask = self.ask;
        let mut floating = 0.0;
        for o in &mut self.orders {
            o.profit = match o.order_type {
                OrderType::Buy => Self::pnl_buy_calc(o.lots, o.open_price, bid),
                OrderType::Sell => Self::pnl_sell_calc(o.lots, o.open_price, ask),
                _ => 0.0,
            };
            floating += o.total_profit();
        }
        self.equity = self.balance + floating;
    }

    fn dynamic_spread_pips(&mut self, bar: &Bar) -> f64 {
        let base = 0.55;
        let range_pips = ((bar.high - bar.low) / 0.0001).max(0.0);
        let vol_part = (0.018 * range_pips).min(1.6);

        let hour = DateTime::from_timestamp(bar.ts, 0)
            .map(|dt| dt.hour() as i32)
            .unwrap_or(0);

        let session = if hour >= 21 || hour <= 1 {
            0.45
        } else if (6..=15).contains(&hour) {
            0.0
        } else {
            0.15
        };

        let noise = self.rng.random_range(-0.08..=0.12);
        clamp(base + vol_part + session + noise, 0.25, 3.0)
    }

    fn apply_slippage_price(&mut self, price: f64, lots: f64, is_buy: bool) -> f64 {
        let Some(bar) = self.current_bar.as_ref() else {
            return price;
        };

        let range_pips = ((bar.high - bar.low) / 0.0001).max(0.0);
        let vol_component = (0.012 * range_pips).min(1.2);
        let size_component = ((lots - 0.05).max(0.0) * 0.18).min(0.6);
        let noise = abs_gauss(&mut self.rng, 0.10);
        let slip_pips = (0.08 + vol_component + size_component + noise).min(2.5);
        let slip = slip_pips * 0.0001;

        if is_buy {
            round_to(price + slip, self.digits)
        } else {
            round_to(price - slip, self.digits)
        }
    }

    fn pnl_buy(&self, lots: f64, open_price: f64, close_bid: f64) -> f64 {
        Self::pnl_buy_calc(lots, open_price, close_bid)
    }

    fn pnl_buy_calc(lots: f64, open_price: f64, close_bid: f64) -> f64 {
        let units = 100_000.0 * lots;
        if close_bid <= 0.0 {
            return 0.0;
        }
        units * (close_bid - open_price) / close_bid
    }

    fn pnl_sell(&self, lots: f64, open_price: f64, close_ask: f64) -> f64 {
        Self::pnl_sell_calc(lots, open_price, close_ask)
    }

    fn pnl_sell_calc(lots: f64, open_price: f64, close_ask: f64) -> f64 {
        let units = 100_000.0 * lots;
        if close_ask <= 0.0 {
            return 0.0;
        }
        units * (open_price - close_ask) / close_ask
    }
}

use chrono::Timelike;

#[derive(Clone, Debug)]
struct Amazing31 {
    cfg: Config,
    state: State,
}

impl Amazing31 {
    fn new(cfg: Config) -> Self {
        Self {
            cfg,
            state: State::default(),
        }
    }

    fn n(&self, broker: &SimBroker, value: f64) -> f64 {
        round_to(value, broker.digits)
    }

    fn orders(&self, broker: &SimBroker) -> Vec<Order> {
        broker
            .get_orders()
            .iter()
            .filter(|o| o.symbol == self.cfg.symbol && o.magic == self.cfg.magic)
            .cloned()
            .collect()
    }

    fn lizong_10(&self, broker: &SimBroker, order_type: i32, sign_mode: i32, top_n: usize) -> f64 {
        let mut vals = Vec::new();
        for o in self.orders(broker) {
            if order_type != -100 && o.order_type as i32 != order_type {
                continue;
            }
            if sign_mode == 1 && o.profit >= 0.0 {
                vals.push(o.profit);
            } else if sign_mode == 2 && o.profit < 0.0 {
                vals.push(-o.profit);
            }
        }
        vals.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
        vals.into_iter().take(top_n).sum()
    }

    fn lizong_9(
        &mut self,
        broker: &mut SimBroker,
        order_type: OrderType,
        mut count: i32,
        mode: i32,
    ) {
        while count > 0 {
            let mut pool: Vec<Order> = self
                .orders(broker)
                .into_iter()
                .filter(|o| o.order_type == order_type)
                .collect();
            if pool.is_empty() {
                return;
            }
            pool.sort_by(|a, b| {
                if mode == 1 {
                    b.profit.partial_cmp(&a.profit).unwrap_or(Ordering::Equal)
                } else {
                    a.profit.partial_cmp(&b.profit).unwrap_or(Ordering::Equal)
                }
            });
            let target = pool[0].clone();
            if mode == 1 && target.profit >= 0.0 {
                if broker.close_order(target.ticket) {
                    count -= 1;
                }
            } else if mode == 1 && target.profit < 0.0 {
                count -= 1;
            } else if mode == 2 && target.profit < 0.0 {
                if broker.close_order(target.ticket) {
                    count -= 1;
                }
            } else if mode == 2 && target.profit >= 0.0 {
                count -= 1;
            } else {
                return;
            }
        }
    }

    fn lizong_7(&mut self, broker: &mut SimBroker, side: i32) -> bool {
        for _ in 0..10 {
            let mut remain = 0;
            let all = self.orders(broker);
            for o in all {
                let ok = if matches!(o.order_type, OrderType::Buy | OrderType::BuyStop)
                    && (side == 1 || side == 0)
                {
                    if o.order_type == OrderType::Buy {
                        broker.close_order(o.ticket)
                    } else {
                        broker.delete_order(o.ticket)
                    }
                } else if matches!(o.order_type, OrderType::Sell | OrderType::SellStop)
                    && (side == -1 || side == 0)
                {
                    if o.order_type == OrderType::Sell {
                        broker.close_order(o.ticket)
                    } else {
                        broker.delete_order(o.ticket)
                    }
                } else {
                    continue;
                };
                if !ok {
                    remain += 1;
                }
            }
            if remain == 0 {
                return true;
            }
        }
        false
    }

    fn time_to_seconds(value: &str) -> i64 {
        let parts: Vec<&str> = value.split(':').collect();
        let (h, m, s) = match parts.len() {
            2 => (parts[0], parts[1], "0"),
            3 => (parts[0], parts[1], parts[2]),
            _ => return 0,
        };

        let hh = h.parse::<i64>().unwrap_or(0).clamp(0, 23);
        let mm = m.parse::<i64>().unwrap_or(0).clamp(0, 59);
        let ss = s.parse::<i64>().unwrap_or(0).clamp(0, 59);
        hh * 3600 + mm * 60 + ss
    }

    fn in_time_window(&self, now_ts: i64, start: &str, stop: &str) -> bool {
        let Some(dt) = DateTime::from_timestamp(now_ts, 0) else {
            return true;
        };
        let cur = dt.hour() as i64 * 3600 + dt.minute() as i64 * 60 + dt.second() as i64;
        let start_s = Self::time_to_seconds(start);
        let stop_s = Self::time_to_seconds(stop);

        if start_s <= stop_s {
            start_s <= cur && cur <= stop_s
        } else {
            cur >= start_s || cur <= stop_s
        }
    }

    fn count_ss(side_orders: &[Order]) -> i32 {
        side_orders.iter().filter(|o| o.comment == "SS").count() as i32
    }

    fn latest_open_time(side_orders: &[Order]) -> i64 {
        let mut latest_ticket = -1_i64;
        let mut latest_open = 0_i64;
        for o in side_orders {
            if o.ticket > latest_ticket {
                latest_ticket = o.ticket;
                latest_open = o.open_time;
            }
        }
        latest_open
    }

    fn on_tick(&mut self, broker: &mut SimBroker, now_ts: i64, current_bar_ts: i64) {
        let (bid, ask) = broker.get_bid_ask();
        let pt = broker.point;

        let orders = self.orders(broker);
        let buys: Vec<Order> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::Buy)
            .cloned()
            .collect();
        let sells: Vec<Order> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::Sell)
            .cloned()
            .collect();
        let buystops: Vec<Order> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::BuyStop)
            .cloned()
            .collect();
        let sellstops: Vec<Order> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::SellStop)
            .cloned()
            .collect();

        let buy_profit: f64 = buys.iter().map(Order::total_profit).sum();
        let sell_profit: f64 = sells.iter().map(Order::total_profit).sum();
        let total_profit = buy_profit + sell_profit;

        let buy_lots: f64 = buys.iter().map(|o| o.lots).sum();
        let sell_lots: f64 = sells.iter().map(|o| o.lots).sum();

        let buy_high = buys
            .iter()
            .chain(buystops.iter())
            .map(|o| o.open_price)
            .fold(0.0_f64, f64::max);
        let buy_low = buys
            .iter()
            .map(|o| o.open_price)
            .fold(f64::INFINITY, f64::min);
        let buy_low = if buy_low.is_finite() { buy_low } else { 0.0 };

        let sell_low = sells
            .iter()
            .chain(sellstops.iter())
            .map(|o| o.open_price)
            .fold(f64::INFINITY, f64::min);
        let sell_low = if sell_low.is_finite() { sell_low } else { 0.0 };
        let sell_high = sells.iter().map(|o| o.open_price).fold(0.0_f64, f64::max);

        let buy_ss_count = Self::count_ss(&buys);
        let sell_ss_count = Self::count_ss(&sells);
        let sell_ss_when_no_buy_ss = if buy_ss_count < 1 { sell_ss_count } else { 0 };

        let mut can_buy = true;
        let mut can_sell = true;

        if !self.in_time_window(now_ts, &self.cfg.ea_start_time, &self.cfg.ea_stop_time) {
            can_buy = false;
            can_sell = false;
        }

        if broker.leverage < self.cfg.leverage_min
            || !broker.is_trade_allowed()
            || !broker.is_expert_enabled()
            || broker.is_stopped()
            || (buys.len() + sells.len()) as i32 >= self.cfg.totals
            || broker.spread_points > self.cfg.max_spread
        {
            can_buy = false;
            can_sell = false;
        }

        if now_ts < self.state.pause_until {
            can_buy = false;
            can_sell = false;
        }

        if self.cfg.over && buys.is_empty() {
            can_buy = false;
        }
        if self.cfg.over && sells.is_empty() {
            can_sell = false;
        }

        if self.cfg.over && total_profit >= self.cfg.close_all {
            self.lizong_7(broker, 0);
            if self.cfg.next_time > 0 {
                self.state.pause_until = now_ts + self.cfg.next_time;
            }
            return;
        }

        if !self.cfg.over {
            if (sell_ss_when_no_buy_ss < 1 || !self.cfg.homeopathy_close_all)
                && buy_profit > self.cfg.max_loss_close_all
                && sell_profit > self.cfg.max_loss_close_all
            {
                if (self.cfg.profit_by_count
                    && buy_profit > self.cfg.stop_profit * buys.len() as f64)
                    || (!self.cfg.profit_by_count && buy_profit > self.cfg.stop_profit)
                {
                    self.lizong_7(broker, 1);
                    return;
                }
                if (self.cfg.profit_by_count
                    && sell_profit > self.cfg.stop_profit * sells.len() as f64)
                    || (!self.cfg.profit_by_count && sell_profit > self.cfg.stop_profit)
                {
                    self.lizong_7(broker, -1);
                    return;
                }
            }

            if self.cfg.homeopathy_close_all
                && (buy_ss_count > 0 || sell_ss_count > 0)
                && total_profit >= self.cfg.close_all
            {
                self.lizong_7(broker, 0);
                if self.cfg.next_time > 0 {
                    self.state.pause_until = now_ts + self.cfg.next_time;
                }
                return;
            }

            if total_profit >= self.cfg.close_all
                && (buy_profit <= self.cfg.max_loss_close_all
                    || sell_profit <= self.cfg.max_loss_close_all)
            {
                self.lizong_7(broker, 0);
                if self.cfg.next_time > 0 {
                    self.state.pause_until = now_ts + self.cfg.next_time;
                }
                return;
            }
        }

        if self.cfg.stop_loss != 0.0 && total_profit <= self.cfg.stop_loss {
            self.lizong_7(broker, 0);
            if self.cfg.next_time > 0 {
                self.state.pause_until = now_ts + self.cfg.next_time;
            }
            return;
        }

        if self.cfg.close_buy_sell {
            let buy_diff = self.lizong_10(broker, OrderType::Buy as i32, 1, 1)
                - self.lizong_10(broker, OrderType::Buy as i32, 2, 2);
            self.state.peak_buy_diff = self.state.peak_buy_diff.max(buy_diff);
            if self.state.peak_buy_diff > 0.0 && buy_diff > 0.0 && buy_lots > 0.0 && buys.len() > 3
            {
                let best_buy = buys.iter().map(|o| o.profit).fold(0.0_f64, f64::max);
                let best_buy_lot = buys
                    .iter()
                    .find(|o| (o.profit - best_buy).abs() < 1e-12)
                    .map(|o| o.lots)
                    .unwrap_or(0.0);
                if buy_lots > best_buy_lot * 3.0 + sell_lots {
                    self.lizong_9(broker, OrderType::Buy, 1, 1);
                    self.lizong_9(broker, OrderType::Buy, 2, 2);
                    self.state.peak_buy_diff = 0.0;
                    self.state.peak_sell_diff = 0.0;
                }
            }

            let sell_diff = self.lizong_10(broker, OrderType::Sell as i32, 1, 1)
                - self.lizong_10(broker, OrderType::Sell as i32, 2, 2);
            self.state.peak_sell_diff = self.state.peak_sell_diff.max(sell_diff);
            if self.state.peak_sell_diff > 0.0
                && sell_diff > 0.0
                && sell_lots > 0.0
                && sells.len() > 3
            {
                let best_sell = sells.iter().map(|o| o.profit).fold(0.0_f64, f64::max);
                let best_sell_lot = sells
                    .iter()
                    .find(|o| (o.profit - best_sell).abs() < 1e-12)
                    .map(|o| o.lots)
                    .unwrap_or(0.0);
                if sell_lots > best_sell_lot * 3.0 + buy_lots {
                    self.lizong_9(broker, OrderType::Sell, 1, 1);
                    self.lizong_9(broker, OrderType::Sell, 2, 2);
                    self.state.peak_buy_diff = 0.0;
                    self.state.peak_sell_diff = 0.0;
                }
            }
        }

        let aggressive_mode = self.cfg.money == 0.0 || total_profit > self.cfg.money;

        let open_gate = (self.cfg.open_mode == OpenMode::Bar
            && self.state.last_bar_time != current_bar_ts)
            || matches!(self.cfg.open_mode, OpenMode::Sleep | OpenMode::Always);

        if open_gate {
            let buy_last_open = Self::latest_open_time(&buys);
            let sell_last_open = Self::latest_open_time(&sells);
            let limit_window = self.in_time_window(
                now_ts,
                &self.cfg.limit_start_time,
                &self.cfg.limit_stop_time,
            );

            self.try_open_buy(
                broker,
                now_ts,
                can_buy,
                ask,
                pt,
                &buys,
                &buystops,
                buy_profit,
                buy_lots,
                sell_lots,
                buy_high,
                buy_low,
                aggressive_mode,
                buy_last_open,
                limit_window,
            );
            self.try_open_sell(
                broker,
                now_ts,
                can_sell,
                bid,
                pt,
                &sells,
                &sellstops,
                sell_profit,
                sell_lots,
                buy_lots,
                sell_low,
                sell_high,
                aggressive_mode,
                sell_last_open,
                limit_window,
            );

            self.state.last_bar_time = current_bar_ts;
        }

        self.trail_pending_buy(
            broker,
            can_buy,
            ask,
            pt,
            buys.len() as i32,
            aggressive_mode,
            buy_high,
            buy_low,
            &buystops,
        );
        self.trail_pending_sell(
            broker,
            can_sell,
            bid,
            pt,
            sells.len() as i32,
            aggressive_mode,
            sell_low,
            sell_high,
            &sellstops,
        );
    }

    fn calc_lot(&self, side_count: i32) -> f64 {
        let x = if side_count == 0 {
            self.cfg.lot
        } else {
            self.cfg.lot * self.cfg.k_lot.powi(side_count) + side_count as f64 * self.cfg.plus_lot
        };
        round_to(x.min(self.cfg.max_lot), self.cfg.digits_lot)
    }

    fn can_afford(&self, broker: &SimBroker, lots: f64) -> bool {
        if !self.cfg.check_margin_for_add_orders {
            return true;
        }
        let need = broker.margin_per_lot(&self.cfg.symbol);
        if need <= 0.0 {
            return true;
        }
        lots * 2.0 < broker.free_margin() / need
    }

    #[allow(clippy::too_many_arguments)]
    fn try_open_buy(
        &mut self,
        broker: &mut SimBroker,
        now_ts: i64,
        can_buy: bool,
        ask: f64,
        pt: f64,
        buys: &[Order],
        buystops: &[Order],
        buy_profit: f64,
        buy_lots: f64,
        sell_lots: f64,
        buy_high: f64,
        buy_low: f64,
        aggressive_mode: bool,
        last_open_time: i64,
        limit_window: bool,
    ) {
        if !buystops.is_empty() || buy_profit <= self.cfg.max_loss || !can_buy {
            return;
        }
        if self.cfg.open_mode == OpenMode::Sleep && now_ts - last_open_time < self.cfg.sleep_seconds
        {
            return;
        }

        let count = buys.len() as i32;
        let px = if count == 0 {
            self.n(broker, ask + self.cfg.first_step as f64 * pt)
        } else {
            let base = if aggressive_mode {
                self.cfg.min_distance
            } else {
                self.cfg.two_min_distance
            };
            let step = if aggressive_mode {
                self.cfg.step
            } else {
                self.cfg.two_step
            };
            let mut px = self.n(broker, ask + base as f64 * pt);
            if buy_low > 0.0 && px < self.n(broker, buy_low - step as f64 * pt) {
                px = self.n(broker, ask + step as f64 * pt);
            }
            px
        };

        let step_now = if aggressive_mode {
            self.cfg.step
        } else {
            self.cfg.two_step
        };

        let cond = count == 0
            || (buy_high > 0.0
                && px >= self.n(broker, buy_high + step_now as f64 * pt)
                && sell_lots > buy_lots * 3.0
                && sell_lots - buy_lots > 0.2)
            || (buy_low > 0.0 && px <= self.n(broker, buy_low - step_now as f64 * pt))
            || (self.cfg.homeopathy
                && buy_high > 0.0
                && px >= self.n(broker, buy_high + self.cfg.step as f64 * pt)
                && (buy_lots - sell_lots).abs() < 1e-12);

        if !cond {
            return;
        }

        let lots = self.calc_lot(count);
        if count > 0 && !self.can_afford(broker, lots) {
            return;
        }

        if count > 0
            && limit_window
            && self.cfg.on_top_not_buy_add != 0.0
            && px >= self.cfg.on_top_not_buy_add
        {
            return;
        }

        let ss_comment = (buy_high > 0.0
            && px >= self.n(broker, buy_high + step_now as f64 * pt)
            && sell_lots > buy_lots * 3.0
            && sell_lots - buy_lots > 0.2)
            || (self.cfg.homeopathy
                && buy_high > 0.0
                && px >= self.n(broker, buy_high + self.cfg.step as f64 * pt)
                && (buy_lots - sell_lots).abs() < 1e-12);

        let comment = if ss_comment { "SS" } else { "NN" };
        broker.send_pending(OrderType::BuyStop, lots, px, comment);
    }

    #[allow(clippy::too_many_arguments)]
    fn try_open_sell(
        &mut self,
        broker: &mut SimBroker,
        now_ts: i64,
        can_sell: bool,
        bid: f64,
        pt: f64,
        sells: &[Order],
        sellstops: &[Order],
        sell_profit: f64,
        sell_lots: f64,
        buy_lots: f64,
        sell_low: f64,
        sell_high: f64,
        aggressive_mode: bool,
        last_open_time: i64,
        limit_window: bool,
    ) {
        if !sellstops.is_empty() || sell_profit <= self.cfg.max_loss || !can_sell {
            return;
        }
        if self.cfg.open_mode == OpenMode::Sleep && now_ts - last_open_time < self.cfg.sleep_seconds
        {
            return;
        }

        let count = sells.len() as i32;
        let px = if count == 0 {
            self.n(broker, bid - self.cfg.first_step as f64 * pt)
        } else {
            let base = if aggressive_mode {
                self.cfg.min_distance
            } else {
                self.cfg.two_min_distance
            };
            let step = if aggressive_mode {
                self.cfg.step
            } else {
                self.cfg.two_step
            };
            let mut px = self.n(broker, bid - base as f64 * pt);
            if sell_high > 0.0 && px < self.n(broker, sell_high + step as f64 * pt) {
                px = self.n(broker, bid - step as f64 * pt);
            }
            px
        };

        let step_now = if aggressive_mode {
            self.cfg.step
        } else {
            self.cfg.two_step
        };

        let cond = count == 0
            || (sell_low > 0.0
                && px <= self.n(broker, sell_low - step_now as f64 * pt)
                && buy_lots > sell_lots * 3.0
                && buy_lots - sell_lots > 0.2)
            || (sell_high > 0.0 && px >= self.n(broker, sell_high + step_now as f64 * pt))
            || (self.cfg.homeopathy
                && sell_low > 0.0
                && px <= self.n(broker, sell_low - self.cfg.step as f64 * pt)
                && (buy_lots - sell_lots).abs() < 1e-12);

        if !cond {
            return;
        }

        let lots = self.calc_lot(count);
        if count > 0 && !self.can_afford(broker, lots) {
            return;
        }

        if count > 0
            && limit_window
            && self.cfg.on_under_not_sell_add != 0.0
            && px <= self.cfg.on_under_not_sell_add
        {
            return;
        }

        let ss_comment = (sell_low > 0.0
            && px <= self.n(broker, sell_low - step_now as f64 * pt)
            && buy_lots > sell_lots * 3.0
            && buy_lots - sell_lots > 0.2)
            || (self.cfg.homeopathy
                && sell_low > 0.0
                && px <= self.n(broker, sell_low - self.cfg.step as f64 * pt)
                && (buy_lots - sell_lots).abs() < 1e-12);

        let comment = if ss_comment { "SS" } else { "NN" };
        broker.send_pending(OrderType::SellStop, lots, px, comment);
    }

    #[allow(clippy::too_many_arguments)]
    fn trail_pending_buy(
        &mut self,
        broker: &mut SimBroker,
        can_buy: bool,
        ask: f64,
        pt: f64,
        buy_count: i32,
        aggressive_mode: bool,
        buy_high: f64,
        buy_low: f64,
        buystops: &[Order],
    ) {
        if !can_buy || buystops.is_empty() {
            return;
        }

        let pending = buystops
            .iter()
            .max_by(|a, b| {
                a.open_price
                    .partial_cmp(&b.open_price)
                    .unwrap_or(Ordering::Equal)
            })
            .unwrap();

        let base = if buy_count == 0 {
            self.cfg.first_step
        } else if aggressive_mode {
            self.cfg.min_distance
        } else {
            self.cfg.two_min_distance
        };
        let px = self.n(broker, ask + base as f64 * pt);

        if self.n(
            broker,
            pending.open_price - self.cfg.step_trail_orders as f64 * pt,
        ) > px
        {
            let step = if aggressive_mode {
                self.cfg.step
            } else {
                self.cfg.two_step
            };
            let cond = buy_low == 0.0
                || px <= self.n(broker, buy_low - step as f64 * pt)
                || px >= self.n(broker, buy_high + step as f64 * pt);
            if cond {
                broker.modify_order(pending.ticket, px);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn trail_pending_sell(
        &mut self,
        broker: &mut SimBroker,
        can_sell: bool,
        bid: f64,
        pt: f64,
        sell_count: i32,
        aggressive_mode: bool,
        sell_low: f64,
        sell_high: f64,
        sellstops: &[Order],
    ) {
        if !can_sell || sellstops.is_empty() {
            return;
        }

        let pending = sellstops
            .iter()
            .min_by(|a, b| {
                a.open_price
                    .partial_cmp(&b.open_price)
                    .unwrap_or(Ordering::Equal)
            })
            .unwrap();

        let base = if sell_count == 0 {
            self.cfg.first_step
        } else if aggressive_mode {
            self.cfg.min_distance
        } else {
            self.cfg.two_min_distance
        };
        let px = self.n(broker, bid - base as f64 * pt);

        if self.n(
            broker,
            pending.open_price + self.cfg.step_trail_orders as f64 * pt,
        ) < px
        {
            let step = if aggressive_mode {
                self.cfg.step
            } else {
                self.cfg.two_step
            };
            let cond = sell_high == 0.0
                || px >= self.n(broker, sell_high + step as f64 * pt)
                || px <= self.n(broker, sell_low - step as f64 * pt);
            if cond {
                broker.modify_order(pending.ticket, px);
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct YearResult {
    year_idx: usize,
    start_utc: String,
    end_utc: String,
    bars: usize,
    net_profit: f64,
    final_balance: f64,
    max_drawdown_pct: f64,
    min_free_margin: f64,
    blew_up: bool,
    dd_limit_hit: bool,
    stop_time_utc: String,
}

fn run_one_year(
    year_idx: usize,
    bars: &[Bar],
    cfg: &Config,
    drawdown_limit: f64,
    seed: u64,
) -> YearResult {
    let mut broker = SimBroker::new(
        &cfg.symbol,
        INITIAL_BALANCE,
        LEVERAGE,
        seed + year_idx as u64,
    );
    let mut strat = Amazing31::new(cfg.clone());

    let mut blew_up = false;
    let mut dd_limit_hit = false;
    let mut stop_ts = 0_i64;
    let mut min_free_margin = f64::INFINITY;

    for bar in bars {
        broker.on_bar(bar);
        broker.trigger_pending_from_bar();
        strat.on_tick(&mut broker, bar.ts, bar.ts);
        broker.snapshot();

        let eq = broker.equity;
        let fm = broker.free_margin();
        min_free_margin = min_free_margin.min(fm);

        if eq <= 0.0 || fm <= 0.0 {
            blew_up = true;
            stop_ts = bar.ts;
            break;
        }

        let max_dd = calc_max_drawdown(&broker.equity_curve) * 100.0;
        if max_dd >= drawdown_limit {
            dd_limit_hit = true;
            stop_ts = bar.ts;
            break;
        }
    }

    let close_list: Vec<(i64, OrderType)> = broker
        .get_orders()
        .iter()
        .map(|o| (o.ticket, o.order_type))
        .collect();
    for (ticket, typ) in close_list {
        match typ {
            OrderType::Buy | OrderType::Sell => {
                broker.close_order(ticket);
            }
            OrderType::BuyStop | OrderType::SellStop => {
                broker.delete_order(ticket);
            }
        }
    }
    broker.snapshot();

    let start_utc = ts_to_utc(bars.first().map(|x| x.ts).unwrap_or(0));
    let end_utc = ts_to_utc(bars.last().map(|x| x.ts).unwrap_or(0));
    let stop_time_utc = if stop_ts > 0 {
        ts_to_utc(stop_ts)
    } else {
        "-".to_string()
    };

    let final_balance = broker.balance;
    let net_profit = final_balance - INITIAL_BALANCE;
    let max_dd_pct = calc_max_drawdown(&broker.equity_curve) * 100.0;

    if !min_free_margin.is_finite() {
        min_free_margin = broker.free_margin();
    }

    YearResult {
        year_idx,
        start_utc,
        end_utc,
        bars: bars.len(),
        net_profit,
        final_balance,
        max_drawdown_pct: max_dd_pct,
        min_free_margin,
        blew_up,
        dd_limit_hit,
        stop_time_utc,
    }
}

fn evaluate_params(
    params: &Map<String, Value>,
    yearly_bars: &[Vec<Bar>],
    symbol: &str,
    drawdown_limit: f64,
) -> (f64, Vec<YearResult>, Value) {
    let cfg = Config::from_params(symbol, params);

    let mut results = Vec::new();
    for (i, bars) in yearly_bars.iter().enumerate() {
        let r = run_one_year(i + 1, bars, &cfg, drawdown_limit, 20260226);
        let stop = r.blew_up || r.dd_limit_hit;
        results.push(r);
        if stop {
            break;
        }
    }

    let nets: Vec<f64> = results.iter().map(|r| r.net_profit).collect();
    let sum_net: f64 = nets.iter().sum();
    let avg_net = if nets.is_empty() {
        -1e12
    } else {
        sum_net / nets.len() as f64
    };
    let min_net = if nets.is_empty() {
        -1e12
    } else {
        nets.iter().copied().fold(f64::INFINITY, f64::min)
    };
    let worst_dd = results
        .iter()
        .map(|r| r.max_drawdown_pct)
        .fold(0.0_f64, f64::max);
    let min_free_margin = results
        .iter()
        .map(|r| r.min_free_margin)
        .fold(f64::INFINITY, f64::min);

    let blowups = results.iter().filter(|r| r.blew_up).count() as f64;
    let dd_hits = results.iter().filter(|r| r.dd_limit_hit).count() as f64;
    let years_ran = results.len() as f64;

    let feasible = years_ran == yearly_bars.len() as f64
        && blowups == 0.0
        && dd_hits == 0.0
        && worst_dd < drawdown_limit;

    let score = if feasible {
        sum_net + 0.03 * min_net - 0.03 * worst_dd
    } else {
        let missing = (yearly_bars.len() as f64 - years_ran).max(0.0);
        let dd_excess = (worst_dd - drawdown_limit).max(0.0);
        let penalty = missing * 5_000_000.0
            + blowups * 3_000_000.0
            + dd_hits * 1_500_000.0
            + dd_excess * 50_000.0;
        -1_000_000_000.0 - penalty + sum_net
    };

    let agg = json!({
        "sum_net_profit": sum_net,
        "avg_net_profit": avg_net,
        "min_year_net_profit": min_net,
        "blowup_years": blowups,
        "dd_limit_hit_years": dd_hits,
        "years_ran": years_ran,
        "worst_year_max_drawdown_pct": worst_dd,
        "min_free_margin": if min_free_margin.is_finite() { min_free_margin } else { 0.0 },
        "drawdown_limit_pct": drawdown_limit,
        "feasible_drawdown_limit": if feasible { 1.0 } else { 0.0 },
    });

    (score, results, agg)
}

fn calc_max_drawdown(equity_curve: &[f64]) -> f64 {
    if equity_curve.is_empty() {
        return 0.0;
    }
    let mut peak = equity_curve[0];
    let mut max_dd = 0.0;
    for &e in equity_curve {
        if e > peak {
            peak = e;
        }
        if peak > 0.0 {
            let dd = (peak - e) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

fn load_bars_from_csv(path: &Path) -> Result<Vec<Bar>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to open csv: {}", path.display()))?;

    let mut bars = Vec::new();
    for rec in rdr.records() {
        let r = match rec {
            Ok(x) => x,
            Err(_) => continue,
        };
        let ts = r
            .get(0)
            .and_then(|x| x.parse::<i64>().ok())
            .map(|x| x / 1000);
        let o = r.get(1).and_then(|x| x.parse::<f64>().ok());
        let h = r.get(2).and_then(|x| x.parse::<f64>().ok());
        let l = r.get(3).and_then(|x| x.parse::<f64>().ok());
        let c = r.get(4).and_then(|x| x.parse::<f64>().ok());
        if let (Some(ts), Some(open), Some(high), Some(low), Some(close)) = (ts, o, h, l, c) {
            bars.push(Bar {
                ts,
                open,
                high,
                low,
                close,
            });
        }
    }
    bars.sort_by_key(|b| b.ts);
    Ok(bars)
}

fn split_into_year_windows(bars: &[Bar], years: usize) -> Vec<Vec<Bar>> {
    if bars.is_empty() {
        return Vec::new();
    }
    let mut windows = Vec::new();
    let mut idx = 0_usize;
    let mut start_ts = bars[0].ts;
    let sec_1y = 365_i64 * 24 * 60 * 60;

    for _ in 0..years {
        let end_ts = start_ts + sec_1y;
        let mut j = idx;
        while j < bars.len() && bars[j].ts < end_ts {
            j += 1;
        }
        if j <= idx {
            break;
        }
        windows.push(bars[idx..j].to_vec());
        idx = j;
        start_ts = end_ts;
    }

    windows
}

fn load_or_select_data(
    symbol: &str,
    years: usize,
    data_file: &Option<PathBuf>,
) -> Result<(PathBuf, Vec<Bar>)> {
    let end_d = Utc::now().date_naive();
    let start_d = end_d - Duration::days(365 * years as i64);

    let selected = if let Some(p) = data_file {
        p.clone()
    } else {
        select_merged_file(symbol, start_d, end_d)?
    };

    let all_bars = load_bars_from_csv(&selected)?;
    if all_bars.is_empty() {
        bail!("data is empty: {}", selected.display());
    }

    let start_ts = start_d
        .and_hms_opt(0, 0, 0)
        .expect("valid start hms")
        .and_utc()
        .timestamp();
    let end_ts = (end_d + Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .expect("valid end hms")
        .and_utc()
        .timestamp();

    let bars: Vec<Bar> = all_bars
        .into_iter()
        .filter(|b| b.ts >= start_ts && b.ts < end_ts)
        .collect();

    if bars.is_empty() {
        bail!(
            "filtered bars is empty for {} from {} to {}",
            selected.display(),
            start_d,
            end_d
        );
    }

    Ok((selected, bars))
}

fn select_merged_file(symbol: &str, start_d: NaiveDate, end_d: NaiveDate) -> Result<PathBuf> {
    let sym = symbol.to_lowercase();
    let prefix = format!("{}-m5-bid-", sym);
    let suffix = "-merged.csv";
    let exact_name = format!("{}{}-{}{}", prefix, start_d, end_d, suffix);
    let exact_path = Path::new("download").join(exact_name);
    if exact_path.exists() {
        return Ok(exact_path);
    }

    let mut cover: Vec<(i64, NaiveDate, PathBuf)> = Vec::new();
    let mut fallback: Vec<(i64, NaiveDate, PathBuf)> = Vec::new();

    for entry in fs::read_dir("download").context("failed to read download directory")? {
        let e = match entry {
            Ok(x) => x,
            Err(_) => continue,
        };
        let path = e.path();
        let Some(name) = path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix) || !name.ends_with(suffix) {
            continue;
        }

        let mid = &name[prefix.len()..name.len() - suffix.len()];
        if mid.len() < 21 {
            continue;
        }

        let s = &mid[0..10];
        let e = &mid[mid.len() - 10..];
        let Ok(s_d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") else {
            continue;
        };
        let Ok(e_d) = NaiveDate::parse_from_str(e, "%Y-%m-%d") else {
            continue;
        };
        let span = (e_d - s_d).num_days();
        if s_d <= start_d && e_d >= end_d {
            cover.push((span, e_d, path.clone()));
        }
        fallback.push((span, e_d, path.clone()));
    }

    if !cover.is_empty() {
        cover.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        return Ok(cover[0].2.clone());
    }

    if !fallback.is_empty() {
        fallback.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        return Ok(fallback[0].2.clone());
    }

    bail!("no merged file found in download/ for symbol {symbol}")
}

#[derive(Clone, Copy, Debug)]
enum ParamKind {
    Int {
        low: i64,
        high: i64,
        step: i64,
    },
    Float {
        low: f64,
        high: f64,
        step: f64,
        precision: u32,
    },
    Bool {
        p_true: f64,
    },
}

#[derive(Clone, Copy, Debug)]
struct ParamSpec {
    name: &'static str,
    kind: ParamKind,
}

#[derive(Clone, Debug)]
struct CandidateEval {
    score: f64,
    params: Map<String, Value>,
    agg: Value,
    years: Vec<YearResult>,
}

fn param_specs() -> Vec<ParamSpec> {
    vec![
        ParamSpec {
            name: "totals",
            kind: ParamKind::Int {
                low: 20,
                high: 80,
                step: 5,
            },
        },
        ParamSpec {
            name: "max_spread",
            kind: ParamKind::Int {
                low: 20,
                high: 50,
                step: 2,
            },
        },
        ParamSpec {
            name: "close_buy_sell",
            kind: ParamKind::Bool { p_true: 0.5 },
        },
        ParamSpec {
            name: "homeopathy_close_all",
            kind: ParamKind::Bool { p_true: 0.35 },
        },
        ParamSpec {
            name: "homeopathy",
            kind: ParamKind::Bool { p_true: 0.25 },
        },
        ParamSpec {
            name: "money",
            kind: ParamKind::Float {
                low: 0.0,
                high: 250.0,
                step: 0.1,
                precision: 1,
            },
        },
        ParamSpec {
            name: "first_step",
            kind: ParamKind::Int {
                low: 20,
                high: 140,
                step: 5,
            },
        },
        ParamSpec {
            name: "min_distance",
            kind: ParamKind::Int {
                low: 30,
                high: 240,
                step: 5,
            },
        },
        ParamSpec {
            name: "two_min_distance",
            kind: ParamKind::Int {
                low: 40,
                high: 280,
                step: 5,
            },
        },
        ParamSpec {
            name: "step_trail_orders",
            kind: ParamKind::Int {
                low: 2,
                high: 18,
                step: 1,
            },
        },
        ParamSpec {
            name: "step",
            kind: ParamKind::Int {
                low: 70,
                high: 340,
                step: 5,
            },
        },
        ParamSpec {
            name: "two_step",
            kind: ParamKind::Int {
                low: 80,
                high: 360,
                step: 5,
            },
        },
        ParamSpec {
            name: "lot",
            kind: ParamKind::Float {
                low: 0.005,
                high: 0.10,
                step: 0.001,
                precision: 3,
            },
        },
        ParamSpec {
            name: "max_lot",
            kind: ParamKind::Float {
                low: 0.40,
                high: 6.0,
                step: 0.01,
                precision: 2,
            },
        },
        ParamSpec {
            name: "plus_lot",
            kind: ParamKind::Float {
                low: 0.0,
                high: 0.030,
                step: 0.001,
                precision: 3,
            },
        },
        ParamSpec {
            name: "k_lot",
            kind: ParamKind::Float {
                low: 1.05,
                high: 1.45,
                step: 0.001,
                precision: 3,
            },
        },
        ParamSpec {
            name: "close_all",
            kind: ParamKind::Float {
                low: 0.3,
                high: 4.0,
                step: 0.01,
                precision: 2,
            },
        },
        ParamSpec {
            name: "profit_by_count",
            kind: ParamKind::Bool { p_true: 0.5 },
        },
        ParamSpec {
            name: "stop_profit",
            kind: ParamKind::Float {
                low: 1.0,
                high: 14.0,
                step: 0.01,
                precision: 2,
            },
        },
        ParamSpec {
            name: "max_loss",
            kind: ParamKind::Float {
                low: 40_000.0,
                high: 350_000.0,
                step: 10.0,
                precision: 1,
            },
        },
        ParamSpec {
            name: "max_loss_close_all",
            kind: ParamKind::Float {
                low: 20.0,
                high: 350.0,
                step: 0.1,
                precision: 1,
            },
        },
        ParamSpec {
            name: "check_margin_for_add_orders",
            kind: ParamKind::Bool { p_true: 0.75 },
        },
    ]
}

fn base_numeric_bounds(specs: &[ParamSpec]) -> HashMap<&'static str, (f64, f64)> {
    let mut out = HashMap::new();
    for s in specs {
        match s.kind {
            ParamKind::Int { low, high, .. } => {
                out.insert(s.name, (low as f64, high as f64));
            }
            ParamKind::Float { low, high, .. } => {
                out.insert(s.name, (low, high));
            }
            ParamKind::Bool { .. } => {}
        }
    }
    out
}

fn base_bool_probs(specs: &[ParamSpec]) -> HashMap<&'static str, f64> {
    let mut out = HashMap::new();
    for s in specs {
        if let ParamKind::Bool { p_true } = s.kind {
            out.insert(s.name, p_true);
        }
    }
    out
}

fn seed_candidates() -> Vec<Map<String, Value>> {
    let mut base = Map::new();
    base.insert("totals".to_string(), Value::from(60));
    base.insert("max_spread".to_string(), Value::from(40));
    base.insert("close_buy_sell".to_string(), Value::from(false));
    base.insert("homeopathy_close_all".to_string(), Value::from(true));
    base.insert("homeopathy".to_string(), Value::from(false));
    base.insert("money".to_string(), Value::from(0.0));
    base.insert("first_step".to_string(), Value::from(35));
    base.insert("min_distance".to_string(), Value::from(155));
    base.insert("two_min_distance".to_string(), Value::from(95));
    base.insert("step_trail_orders".to_string(), Value::from(15));
    base.insert("step".to_string(), Value::from(160));
    base.insert("two_step".to_string(), Value::from(265));
    base.insert("lot".to_string(), Value::from(0.027));
    base.insert("max_lot".to_string(), Value::from(0.51));
    base.insert("plus_lot".to_string(), Value::from(0.003));
    base.insert("k_lot".to_string(), Value::from(1.085));
    base.insert("close_all".to_string(), Value::from(2.74));
    base.insert("profit_by_count".to_string(), Value::from(false));
    base.insert("stop_profit".to_string(), Value::from(4.49));
    base.insert("max_loss".to_string(), Value::from(91089.5));
    base.insert("max_loss_close_all".to_string(), Value::from(49.4));
    base.insert(
        "check_margin_for_add_orders".to_string(),
        Value::from(false),
    );

    let mut s1 = base.clone();
    s1.insert("lot".to_string(), Value::from(0.040));
    s1.insert("max_lot".to_string(), Value::from(1.20));
    s1.insert("plus_lot".to_string(), Value::from(0.006));
    s1.insert("k_lot".to_string(), Value::from(1.140));
    s1.insert("check_margin_for_add_orders".to_string(), Value::from(true));

    let mut s2 = base.clone();
    s2.insert("lot".to_string(), Value::from(0.055));
    s2.insert("max_lot".to_string(), Value::from(2.20));
    s2.insert("plus_lot".to_string(), Value::from(0.008));
    s2.insert("k_lot".to_string(), Value::from(1.180));
    s2.insert("close_all".to_string(), Value::from(2.20));

    let mut s3 = base.clone();
    s3.insert("totals".to_string(), Value::from(45));
    s3.insert("first_step".to_string(), Value::from(55));
    s3.insert("min_distance".to_string(), Value::from(120));
    s3.insert("two_min_distance".to_string(), Value::from(170));
    s3.insert("step".to_string(), Value::from(210));
    s3.insert("two_step".to_string(), Value::from(250));
    s3.insert("lot".to_string(), Value::from(0.022));
    s3.insert("max_lot".to_string(), Value::from(0.95));

    vec![base, s1, s2, s3]
}

fn sample_candidate(
    specs: &[ParamSpec],
    rng: &mut StdRng,
    num_bounds: &HashMap<&'static str, (f64, f64)>,
    bool_probs: &HashMap<&'static str, f64>,
) -> Map<String, Value> {
    let mut p = Map::new();

    for s in specs {
        match s.kind {
            ParamKind::Bool { p_true } => {
                let prob = *bool_probs.get(s.name).unwrap_or(&p_true);
                p.insert(
                    s.name.to_string(),
                    Value::from(rng.random_bool(prob.clamp(0.02, 0.98))),
                );
            }
            ParamKind::Int { low, high, step } => {
                let (l, h) = num_bounds
                    .get(s.name)
                    .copied()
                    .unwrap_or((low as f64, high as f64));
                let li = quantize_i64(l.round() as i64, low, high, step);
                let hi = quantize_i64(h.round() as i64, low, high, step).max(li);
                let steps = ((hi - li) / step).max(0);
                let k = rng.random_range(0..=steps);
                p.insert(s.name.to_string(), Value::from(li + k * step));
            }
            ParamKind::Float {
                low,
                high,
                step,
                precision,
            } => {
                let (l, h) = num_bounds.get(s.name).copied().unwrap_or((low, high));
                let mut v = if s.name == "money" {
                    if rng.random_bool(0.85) {
                        0.0
                    } else {
                        rng.random_range(l..=h)
                    }
                } else {
                    rng.random_range(l..=h)
                };
                v = quantize_f64(v, low, high, step, precision);
                p.insert(s.name.to_string(), Value::from(v));
            }
        }
    }

    add_fixed_params(&mut p);
    repair_candidate(&mut p, specs);
    p
}

fn mutate_candidate(
    base: &Map<String, Value>,
    specs: &[ParamSpec],
    rng: &mut StdRng,
    num_bounds: &HashMap<&'static str, (f64, f64)>,
    bool_probs: &HashMap<&'static str, f64>,
    scale: f64,
) -> Map<String, Value> {
    let mut p = base.clone();
    let mut changed = false;

    for s in specs {
        let p_mut = (0.10 + 0.20 * scale).min(0.85);
        if !rng.random_bool(p_mut) {
            continue;
        }

        match s.kind {
            ParamKind::Bool { p_true } => {
                let cur = get_bool(&p, s.name, false);
                let prob = *bool_probs.get(s.name).unwrap_or(&p_true);
                let v = if rng.random_bool(0.5) {
                    !cur
                } else {
                    rng.random_bool(prob.clamp(0.02, 0.98))
                };
                p.insert(s.name.to_string(), Value::from(v));
                changed = true;
            }
            ParamKind::Int { low, high, step } => {
                let (l, h) = num_bounds
                    .get(s.name)
                    .copied()
                    .unwrap_or((low as f64, high as f64));
                let li = quantize_i64(l.round() as i64, low, high, step);
                let hi = quantize_i64(h.round() as i64, low, high, step).max(li);
                let cur = get_i64(&p, s.name, li);
                if rng.random_bool((0.05 * scale).min(0.35)) {
                    let steps = ((hi - li) / step).max(0);
                    let k = rng.random_range(0..=steps);
                    p.insert(s.name.to_string(), Value::from(li + k * step));
                } else {
                    let step_count = ((hi - li) / step).max(1) as f64;
                    let max_jump = (step_count * 0.20 * scale).round().max(1.0) as i64;
                    let delta_steps = rng.random_range(-max_jump..=max_jump);
                    let v = quantize_i64(cur + delta_steps * step, li, hi, step);
                    p.insert(s.name.to_string(), Value::from(v));
                }
                changed = true;
            }
            ParamKind::Float {
                low,
                high,
                step,
                precision,
            } => {
                let (l, h) = num_bounds.get(s.name).copied().unwrap_or((low, high));
                let cur = get_f64(&p, s.name, l);
                let v = if rng.random_bool((0.06 * scale).min(0.30)) {
                    rng.random_range(l..=h)
                } else {
                    let span = (h - l).max(step);
                    let delta = rng.random_range(-span * 0.18 * scale..=span * 0.18 * scale);
                    cur + delta
                };
                p.insert(
                    s.name.to_string(),
                    Value::from(quantize_f64(v, low, high, step, precision)),
                );
                changed = true;
            }
        }
    }

    if !changed {
        let idx = rng.random_range(0..specs.len());
        let s = specs[idx];
        match s.kind {
            ParamKind::Bool { p_true } => {
                let prob = *bool_probs.get(s.name).unwrap_or(&p_true);
                p.insert(s.name.to_string(), Value::from(rng.random_bool(prob)));
            }
            ParamKind::Int { low, high, step } => {
                let (l, h) = num_bounds
                    .get(s.name)
                    .copied()
                    .unwrap_or((low as f64, high as f64));
                let li = quantize_i64(l.round() as i64, low, high, step);
                let hi = quantize_i64(h.round() as i64, low, high, step).max(li);
                let steps = ((hi - li) / step).max(0);
                let k = rng.random_range(0..=steps);
                p.insert(s.name.to_string(), Value::from(li + k * step));
            }
            ParamKind::Float {
                low,
                high,
                step,
                precision,
            } => {
                let (l, h) = num_bounds.get(s.name).copied().unwrap_or((low, high));
                let v = quantize_f64(rng.random_range(l..=h), low, high, step, precision);
                p.insert(s.name.to_string(), Value::from(v));
            }
        }
    }

    add_fixed_params(&mut p);
    repair_candidate(&mut p, specs);
    p
}

fn crossover_candidate(
    a: &Map<String, Value>,
    b: &Map<String, Value>,
    specs: &[ParamSpec],
    rng: &mut StdRng,
) -> Map<String, Value> {
    let mut out = Map::new();
    for s in specs {
        let from_a = rng.random_bool(0.5);
        let val = if from_a {
            a.get(s.name).cloned().unwrap_or(Value::Null)
        } else {
            b.get(s.name).cloned().unwrap_or(Value::Null)
        };
        out.insert(s.name.to_string(), val);
    }
    add_fixed_params(&mut out);
    repair_candidate(&mut out, specs);
    out
}

fn derive_refined_bounds(
    specs: &[ParamSpec],
    base_num: &HashMap<&'static str, (f64, f64)>,
    base_bool: &HashMap<&'static str, f64>,
    source: &[CandidateEval],
) -> (
    HashMap<&'static str, (f64, f64)>,
    HashMap<&'static str, f64>,
) {
    if source.is_empty() {
        return (base_num.clone(), base_bool.clone());
    }

    let top_n = source.len().min(10);
    let top = &source[..top_n];

    let mut num = base_num.clone();
    let mut bp = base_bool.clone();

    for s in specs {
        match s.kind {
            ParamKind::Bool { .. } => {
                let mut c_true = 0.0;
                for cand in top {
                    if get_bool(&cand.params, s.name, false) {
                        c_true += 1.0;
                    }
                }
                let p = (c_true / top_n as f64).clamp(0.1, 0.9);
                bp.insert(s.name, p);
            }
            ParamKind::Int { low, high, step } => {
                let vals: Vec<f64> = top
                    .iter()
                    .map(|cand| get_i64(&cand.params, s.name, low) as f64)
                    .collect();
                let vmin = vals.iter().copied().fold(f64::INFINITY, f64::min);
                let vmax = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);

                let (base_l, base_h) = base_num
                    .get(s.name)
                    .copied()
                    .unwrap_or((low as f64, high as f64));
                let base_w = (base_h - base_l).max(1.0);
                let w = (vmax - vmin).max(step as f64);
                let pad = (w * 0.25).max(base_w * 0.08);
                let mut nl = clamp(vmin - pad, base_l, base_h);
                let mut nh = clamp(vmax + pad, base_l, base_h);
                if nh - nl < (step * 2) as f64 {
                    let mid = (nl + nh) / 2.0;
                    nl = clamp(mid - step as f64, base_l, base_h);
                    nh = clamp(mid + step as f64, base_l, base_h);
                }
                num.insert(s.name, (nl, nh));
            }
            ParamKind::Float {
                low, high, step, ..
            } => {
                let vals: Vec<f64> = top
                    .iter()
                    .map(|cand| get_f64(&cand.params, s.name, low))
                    .collect();
                let vmin = vals.iter().copied().fold(f64::INFINITY, f64::min);
                let vmax = vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);

                let (base_l, base_h) = base_num.get(s.name).copied().unwrap_or((low, high));
                let base_w = (base_h - base_l).max(step);
                let w = (vmax - vmin).max(step);
                let pad = (w * 0.25).max(base_w * 0.08);
                let mut nl = clamp(vmin - pad, base_l, base_h);
                let mut nh = clamp(vmax + pad, base_l, base_h);
                if nh - nl < step * 2.0 {
                    let mid = (nl + nh) / 2.0;
                    nl = clamp(mid - step, base_l, base_h);
                    nh = clamp(mid + step, base_l, base_h);
                }
                num.insert(s.name, (nl, nh));
            }
        }
    }

    (num, bp)
}

fn optimize_params(
    specs: &[ParamSpec],
    symbol: &str,
    yearly_bars: &[Vec<Bar>],
    drawdown_limit: f64,
    trials: usize,
    seed: u64,
) -> (CandidateEval, Option<CandidateEval>, Value) {
    let mut rng = StdRng::seed_from_u64(seed);
    let base_num = base_numeric_bounds(specs);
    let base_bool = base_bool_probs(specs);

    let global_trials = (trials / 3).max(10).min(trials);
    let local_trials = trials.saturating_sub(global_trials);

    let mut top_all: Vec<CandidateEval> = Vec::new();
    let mut top_feasible: Vec<CandidateEval> = Vec::new();
    let mut best_any: Option<CandidateEval> = None;
    let mut best_feasible: Option<CandidateEval> = None;
    let mut visited: HashSet<String> = HashSet::new();

    let mut eval_count = 0_usize;
    let mut feasible_count = 0_usize;

    let mut seeds: VecDeque<Map<String, Value>> = seed_candidates().into();

    while eval_count < global_trials {
        let mut cand = if let Some(seed_p) = seeds.pop_front() {
            seed_p
        } else {
            sample_candidate(specs, &mut rng, &base_num, &base_bool)
        };
        add_fixed_params(&mut cand);
        repair_candidate(&mut cand, specs);

        let fp = fingerprint_params(&cand);
        if visited.contains(&fp) {
            continue;
        }
        visited.insert(fp);

        let (score, years, agg) = evaluate_params(&cand, yearly_bars, symbol, drawdown_limit);
        eval_count += 1;

        let ce = CandidateEval {
            score,
            params: cand,
            agg,
            years,
        };

        if best_any
            .as_ref()
            .map(|x| ce.score > x.score)
            .unwrap_or(true)
        {
            best_any = Some(ce.clone());
        }

        if is_feasible(&ce.agg) {
            feasible_count += 1;
            if best_feasible
                .as_ref()
                .map(|x| ce.score > x.score)
                .unwrap_or(true)
            {
                best_feasible = Some(ce.clone());
            }
            push_topk(&mut top_feasible, ce.clone(), 14);
        }

        push_topk(&mut top_all, ce.clone(), 20);

        println!(
            "[global {}/{}] score={:.2} sum_net={:.2} worst_dd={:.2}% feasible={} years={:.0}/{}",
            eval_count,
            global_trials,
            ce.score,
            agg_num(&ce.agg, "sum_net_profit"),
            agg_num(&ce.agg, "worst_year_max_drawdown_pct"),
            if is_feasible(&ce.agg) { 1 } else { 0 },
            agg_num(&ce.agg, "years_ran"),
            yearly_bars.len()
        );
    }

    let bound_source = if !top_feasible.is_empty() {
        top_feasible.clone()
    } else {
        top_all.clone()
    };
    let (ref_num, ref_bool) = derive_refined_bounds(specs, &base_num, &base_bool, &bound_source);

    println!("\nRefined bounds generated from top candidates.");

    let mut sigma = 1.0_f64;
    let mut stagnation = 0_i32;

    for i in 1..=local_trials {
        let mut generated = None;
        for _ in 0..50 {
            let parent_pool = if !top_feasible.is_empty() && rng.random_bool(0.75) {
                &top_feasible
            } else {
                &top_all
            };

            let mut cand = if parent_pool.is_empty() || rng.random_bool(0.24) {
                sample_candidate(specs, &mut rng, &ref_num, &ref_bool)
            } else {
                let p1 = select_parent(parent_pool, &mut rng).expect("parent exists");
                if parent_pool.len() >= 2 && rng.random_bool(0.30) {
                    let p2 = select_parent(parent_pool, &mut rng).expect("parent exists");
                    let cross = crossover_candidate(&p1.params, &p2.params, specs, &mut rng);
                    mutate_candidate(&cross, specs, &mut rng, &ref_num, &ref_bool, sigma)
                } else {
                    mutate_candidate(&p1.params, specs, &mut rng, &ref_num, &ref_bool, sigma)
                }
            };

            if rng.random_bool(0.08) {
                cand = mutate_candidate(
                    &cand,
                    specs,
                    &mut rng,
                    &ref_num,
                    &ref_bool,
                    (sigma * 1.4).min(2.5),
                );
            }

            let fp = fingerprint_params(&cand);
            if visited.contains(&fp) {
                continue;
            }
            visited.insert(fp);
            generated = Some(cand);
            break;
        }

        let Some(cand) = generated else {
            continue;
        };

        let (score, years, agg) = evaluate_params(&cand, yearly_bars, symbol, drawdown_limit);
        eval_count += 1;

        let ce = CandidateEval {
            score,
            params: cand,
            agg,
            years,
        };

        let mut improved = false;

        if best_any
            .as_ref()
            .map(|x| ce.score > x.score)
            .unwrap_or(true)
        {
            best_any = Some(ce.clone());
            improved = true;
        }

        if is_feasible(&ce.agg) {
            feasible_count += 1;
            if best_feasible
                .as_ref()
                .map(|x| ce.score > x.score)
                .unwrap_or(true)
            {
                best_feasible = Some(ce.clone());
                improved = true;
            }
            push_topk(&mut top_feasible, ce.clone(), 14);
        }

        push_topk(&mut top_all, ce.clone(), 20);

        if improved {
            sigma = (sigma * 0.90).max(0.25);
            stagnation = 0;
        } else {
            stagnation += 1;
            if stagnation % 12 == 0 {
                sigma = (sigma * 1.20).min(2.5);
            }
            if stagnation % 40 == 0 {
                sigma = (sigma * 1.30).min(2.5);
            }
        }

        println!(
            "[local {}/{}] score={:.2} sum_net={:.2} worst_dd={:.2}% feasible={} best_feasible={} sigma={:.2}",
            i,
            local_trials,
            ce.score,
            agg_num(&ce.agg, "sum_net_profit"),
            agg_num(&ce.agg, "worst_year_max_drawdown_pct"),
            if is_feasible(&ce.agg) { 1 } else { 0 },
            feasible_count,
            sigma,
        );
    }

    let best_any = best_any.expect("at least one candidate evaluated");

    let bounds_json = json!({
        "numeric": ref_num.into_iter().map(|(k, (l, h))| (k.to_string(), json!([l, h]))).collect::<BTreeMap<_, _>>(),
        "bool_probs": ref_bool.into_iter().map(|(k, p)| (k.to_string(), p)).collect::<BTreeMap<_, _>>(),
        "global_trials": global_trials,
        "local_trials": local_trials,
        "evaluated": eval_count,
    });

    (best_any, best_feasible, bounds_json)
}

fn add_fixed_params(p: &mut Map<String, Value>) {
    p.insert("digits_lot".to_string(), Value::from(3));
    p.insert("open_mode".to_string(), Value::from(1));
    p.insert("sleep_seconds".to_string(), Value::from(30));
    p.insert("stop_loss".to_string(), Value::from(0.0));
}

fn repair_candidate(p: &mut Map<String, Value>, specs: &[ParamSpec]) {
    for s in specs {
        match s.kind {
            ParamKind::Bool { p_true } => {
                let v = get_bool(p, s.name, p_true >= 0.5);
                p.insert(s.name.to_string(), Value::from(v));
            }
            ParamKind::Int { low, high, step } => {
                let default_v = quantize_i64((low + high) / 2, low, high, step);
                let v = quantize_i64(get_i64(p, s.name, default_v), low, high, step);
                p.insert(s.name.to_string(), Value::from(v));
            }
            ParamKind::Float {
                low,
                high,
                step,
                precision,
            } => {
                let v = quantize_f64(get_f64(p, s.name, low), low, high, step, precision);
                p.insert(s.name.to_string(), Value::from(v));
            }
        }
    }

    let min_distance = get_i64(p, "min_distance", 60);
    let two_min_distance = get_i64(p, "two_min_distance", min_distance);
    if two_min_distance < min_distance {
        p.insert("two_min_distance".to_string(), Value::from(min_distance));
    }

    let step = get_i64(p, "step", 100);
    let two_step = get_i64(p, "two_step", step);
    if two_step < step {
        p.insert("two_step".to_string(), Value::from(step));
    }

    let lot = get_f64(p, "lot", 0.01);
    let max_lot = get_f64(p, "max_lot", lot);
    if max_lot < lot + 0.01 {
        p.insert("max_lot".to_string(), Value::from(round_to(lot + 0.01, 2)));
    }

    let max_loss_close_all = get_f64(p, "max_loss_close_all", 50.0);
    let max_loss = get_f64(p, "max_loss", 100_000.0);
    if max_loss < max_loss_close_all + 5_000.0 {
        p.insert(
            "max_loss".to_string(),
            Value::from(round_to(max_loss_close_all + 5_000.0, 1)),
        );
    }

    add_fixed_params(p);
}

fn push_topk(buf: &mut Vec<CandidateEval>, cand: CandidateEval, k: usize) {
    buf.push(cand);
    buf.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    if buf.len() > k {
        buf.truncate(k);
    }
}

fn select_parent<'a>(pool: &'a [CandidateEval], rng: &mut StdRng) -> Option<&'a CandidateEval> {
    if pool.is_empty() {
        return None;
    }
    let u: f64 = rng.random();
    let idx = ((u * u) * pool.len() as f64).floor() as usize;
    pool.get(idx.min(pool.len() - 1))
}

fn is_feasible(agg: &Value) -> bool {
    agg_num(agg, "feasible_drawdown_limit") >= 0.5
}

fn agg_num(agg: &Value, key: &str) -> f64 {
    agg.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn fingerprint_params(p: &Map<String, Value>) -> String {
    let mut b = BTreeMap::new();
    for (k, v) in p {
        b.insert(k.clone(), v.clone());
    }
    serde_json::to_string(&b).unwrap_or_else(|_| "{}".to_string())
}

fn quantize_i64(v: i64, low: i64, high: i64, step: i64) -> i64 {
    let c = v.clamp(low, high);
    let n = ((c - low) as f64 / step as f64).round() as i64;
    (low + n * step).clamp(low, high)
}

fn quantize_f64(v: f64, low: f64, high: f64, step: f64, precision: u32) -> f64 {
    let c = clamp(v, low, high);
    let n = ((c - low) / step).round();
    let q = low + n * step;
    round_to(clamp(q, low, high), precision as i32)
}

fn get_i64(map: &Map<String, Value>, key: &str, default: i64) -> i64 {
    map.get(key)
        .and_then(|v| {
            if let Some(i) = v.as_i64() {
                Some(i)
            } else {
                v.as_f64().map(|x| x.round() as i64)
            }
        })
        .unwrap_or(default)
}

fn get_f64(map: &Map<String, Value>, key: &str, default: f64) -> f64 {
    map.get(key)
        .and_then(|v| {
            if let Some(f) = v.as_f64() {
                Some(f)
            } else {
                v.as_i64().map(|x| x as f64)
            }
        })
        .unwrap_or(default)
}

fn get_bool(map: &Map<String, Value>, key: &str, default: bool) -> bool {
    map.get(key)
        .and_then(|v| {
            if let Some(b) = v.as_bool() {
                Some(b)
            } else if let Some(i) = v.as_i64() {
                Some(i != 0)
            } else {
                None
            }
        })
        .unwrap_or(default)
}

fn get_string(map: &Map<String, Value>, key: &str, default: &str) -> String {
    map.get(key)
        .and_then(|v| v.as_str().map(ToString::to_string))
        .unwrap_or_else(|| default.to_string())
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

fn round_to(v: f64, digits: i32) -> f64 {
    let f = 10_f64.powi(digits);
    (v * f).round() / f
}

fn abs_gauss(rng: &mut StdRng, sigma: f64) -> f64 {
    let u1 = rng.random::<f64>().clamp(1e-12, 1.0);
    let u2 = rng.random::<f64>();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    (z * sigma).abs()
}

fn ts_to_utc(ts: i64) -> String {
    DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01T00:00:00+00:00".to_string())
}

#[derive(Parser, Debug)]
#[command(author, version, about = "AUDNZD parameter optimizer in pure Rust")]
struct Args {
    #[arg(long, default_value = "AUDNZD")]
    symbol: String,
    #[arg(long, default_value_t = 10)]
    years: usize,
    #[arg(long, default_value_t = 120)]
    trials: usize,
    #[arg(long, default_value_t = 20260226)]
    seed: u64,
    #[arg(long, default_value_t = 80.0)]
    drawdown_limit: f64,
    #[arg(long, default_value = "optimized_params_audnzd_10y_dd80_rust.json")]
    out: PathBuf,
    #[arg(long)]
    data_file: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.trials == 0 {
        bail!("--trials must be >= 1");
    }

    let (data_file, bars) = load_or_select_data(&args.symbol, args.years, &args.data_file)?;
    let yearly_bars = split_into_year_windows(&bars, args.years);
    if yearly_bars.len() < args.years {
        bail!(
            "need {} yearly windows, got {} from {}",
            args.years,
            yearly_bars.len(),
            data_file.display()
        );
    }

    println!("data={}", data_file.display());
    println!(
        "symbol={} bars={} years={} trials={} drawdown_limit={:.2}%",
        args.symbol,
        bars.len(),
        yearly_bars.len(),
        args.trials,
        args.drawdown_limit
    );

    let specs = param_specs();
    let (best_any, best_feasible, bounds_json) = optimize_params(
        &specs,
        &args.symbol,
        &yearly_bars,
        args.drawdown_limit,
        args.trials,
        args.seed,
    );

    let chosen = best_feasible.clone().unwrap_or_else(|| best_any.clone());
    let feasible_found = best_feasible.is_some();

    let payload = json!({
        "objective": format!("maximize profit on {} with worst drawdown < {:.2}%", args.symbol, args.drawdown_limit),
        "symbol": args.symbol,
        "drawdown_limit_pct": args.drawdown_limit,
        "years": args.years,
        "generated_at_utc": Utc::now().to_rfc3339(),
        "data_file": data_file,
        "trials": args.trials,
        "seed": args.seed,
        "algorithm": "adaptive elite search + boundary refinement",
        "chosen_boundaries": bounds_json,
        "feasible_found": feasible_found,
        "best_feasible": best_feasible.as_ref().map(|x| json!({
            "score": x.score,
            "params": x.params,
            "aggregate": x.agg,
            "yearly_results": x.years,
        })),
        "best_any": {
            "score": best_any.score,
            "params": best_any.params,
            "aggregate": best_any.agg,
            "yearly_results": best_any.years,
        },
        "selected_result": {
            "score": chosen.score,
            "params": chosen.params,
            "aggregate": chosen.agg,
            "yearly_results": chosen.years,
        }
    });

    fs::write(&args.out, serde_json::to_string_pretty(&payload)?)
        .with_context(|| format!("failed to write {}", args.out.display()))?;

    println!("\nSaved result: {}", args.out.display());
    println!(
        "Selected score={:.2} sum_net={:.2} worst_dd={:.2}% feasible={}",
        chosen.score,
        agg_num(&chosen.agg, "sum_net_profit"),
        agg_num(&chosen.agg, "worst_year_max_drawdown_pct"),
        if is_feasible(&chosen.agg) { 1 } else { 0 }
    );

    if !feasible_found {
        println!("WARNING: no feasible candidate found within the given trials.");
    }

    Ok(())
}
