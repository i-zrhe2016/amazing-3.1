use std::cmp::Ordering;

use chrono::{DateTime, Timelike};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Buy = 0,
    Sell = 1,
    BuyStop = 4,
    SellStop = 5,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub ticket: i64,
    pub symbol: String,
    pub magic: i32,
    pub order_type: OrderType,
    pub lots: f64,
    pub open_price: f64,
    pub profit: f64,
    pub swap: f64,
    pub commission: f64,
    pub comment: String,
    pub open_time: i64,
}

impl OrderSnapshot {
    fn total_profit(&self) -> f64 {
        self.profit + self.swap + self.commission
    }
}

#[allow(dead_code)]
pub trait BrokerApi {
    fn symbol(&self) -> &str;
    fn digits(&self) -> i32;
    fn point(&self) -> f64;
    fn leverage(&self) -> i32;
    fn spread_points(&self) -> f64;
    fn bid_ask(&self) -> (f64, f64);
    fn free_margin(&self) -> f64;
    fn margin_per_lot(&self, symbol: &str) -> f64;
    fn is_trade_allowed(&self) -> bool;
    fn is_expert_enabled(&self) -> bool;
    fn is_stopped(&self) -> bool;

    fn orders(&self) -> Vec<OrderSnapshot>;
    fn send_pending(
        &mut self,
        order_type: OrderType,
        lots: f64,
        price: f64,
        comment: &str,
    ) -> Option<i64>;
    fn modify_order(&mut self, ticket: i64, new_price: f64) -> bool;
    fn close_order(&mut self, ticket: i64) -> bool;
    fn delete_order(&mut self, ticket: i64) -> bool;
}

#[allow(dead_code, non_snake_case)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Amazing31Params {
    pub On_top_of_this_price_not_Buy_first_order: f64,
    pub On_under_of_this_price_not_Sell_first_order: f64,
    pub On_top_of_this_price_not_Buy_order: f64,
    pub On_under_of_this_price_not_Sell_order: f64,
    pub Limit_StartTime: String,
    pub Limit_StopTime: String,
    pub CloseBuySell: bool,
    pub HomeopathyCloseAll: bool,
    pub Homeopathy: bool,
    pub Over: bool,
    pub NextTime: i64,
    pub Money: f64,
    pub FirstStep: i32,
    pub MinDistance: i32,
    pub TwoMinDistance: i32,
    pub StepTrallOrders: i32,
    pub Step: i32,
    pub TwoStep: i32,
    pub OpenMode: i32,
    pub TimeZone: i32,
    pub sleep: i64,
    pub MaxLoss: f64,
    pub MaxLossCloseAll: f64,
    pub lot: f64,
    pub Maxlot: f64,
    pub PlusLot: f64,
    pub K_Lot: f64,
    pub DigitsLot: i32,
    pub CloseAll: f64,
    pub Profit: bool,
    pub StopProfit: f64,
    pub StopLoss: f64,
    pub Magic: i32,
    pub Totals: i32,
    pub MaxSpread: f64,
    pub Leverage: i32,
    pub EA_StartTime: String,
    pub EA_StopTime: String,
}

impl Default for Amazing31Params {
    fn default() -> Self {
        Self {
            On_top_of_this_price_not_Buy_first_order: 0.0,
            On_under_of_this_price_not_Sell_first_order: 0.0,
            On_top_of_this_price_not_Buy_order: 0.0,
            On_under_of_this_price_not_Sell_order: 0.0,
            Limit_StartTime: "00:00".to_string(),
            Limit_StopTime: "24:00".to_string(),
            CloseBuySell: true,
            HomeopathyCloseAll: true,
            Homeopathy: false,
            Over: false,
            NextTime: 0,
            Money: 0.0,
            FirstStep: 30,
            MinDistance: 60,
            TwoMinDistance: 60,
            StepTrallOrders: 5,
            Step: 100,
            TwoStep: 100,
            OpenMode: 3,
            TimeZone: 1,
            sleep: 30,
            MaxLoss: 100000.0,
            MaxLossCloseAll: 50.0,
            lot: 0.01,
            Maxlot: 10.0,
            PlusLot: 0.0,
            K_Lot: 1.3,
            DigitsLot: 2,
            CloseAll: 0.5,
            Profit: true,
            StopProfit: 2.0,
            StopLoss: 0.0,
            Magic: 9453,
            Totals: 50,
            MaxSpread: 32.0,
            Leverage: 100,
            EA_StartTime: "00:00".to_string(),
            EA_StopTime: "24:00".to_string(),
        }
    }
}

impl Amazing31Params {
    pub fn normalize_for_init(&mut self) {
        self.EA_StartTime = clean_time(self.EA_StartTime.clone());
        self.EA_StopTime = clean_time(self.EA_StopTime.clone());
        self.Limit_StartTime = clean_time(self.Limit_StartTime.clone());
        self.Limit_StopTime = clean_time(self.Limit_StopTime.clone());

        if self.MaxLossCloseAll > 0.0 {
            self.MaxLossCloseAll = -self.MaxLossCloseAll;
        }
        if self.MaxLoss > 0.0 {
            self.MaxLoss = -self.MaxLoss;
        }
        if self.StopLoss > 0.0 {
            self.StopLoss = -self.StopLoss;
        }
        if self.Money > 0.0 {
            self.Money = -self.Money;
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RuntimeState {
    pause_until: i64,
    last_bar_time: i64,
    peak_buy_diff: f64,
    peak_sell_diff: f64,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Amazing31Mt4 {
    pub params: Amazing31Params,
    state: RuntimeState,
}

#[allow(dead_code)]
impl Amazing31Mt4 {
    pub fn new(mut params: Amazing31Params) -> Self {
        params.normalize_for_init();
        Self {
            params,
            state: RuntimeState::default(),
        }
    }

    pub fn deinit(&mut self) {}

    pub fn lizong_8(period: i32) -> i32 {
        if period > 43200 {
            0
        } else if period > 10080 {
            43200
        } else if period > 1440 {
            10080
        } else if period > 240 {
            1440
        } else if period > 60 {
            240
        } else if period > 30 {
            60
        } else if period > 15 {
            30
        } else if period > 5 {
            15
        } else if period > 1 {
            5
        } else if period == 1 {
            1
        } else {
            0
        }
    }

    pub fn start(&mut self, broker: &mut dyn BrokerApi, now_ts: i64, frame_bar_ts: i64) {
        let (bid, ask) = broker.bid_ask();
        let pt = broker.point();
        let digits = broker.digits();

        let orders = self.my_orders(broker);
        let buys: Vec<OrderSnapshot> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::Buy)
            .cloned()
            .collect();
        let sells: Vec<OrderSnapshot> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::Sell)
            .cloned()
            .collect();
        let buystops: Vec<OrderSnapshot> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::BuyStop)
            .cloned()
            .collect();
        let sellstops: Vec<OrderSnapshot> = orders
            .iter()
            .filter(|o| o.order_type == OrderType::SellStop)
            .cloned()
            .collect();

        let buy_profit: f64 = buys.iter().map(OrderSnapshot::total_profit).sum();
        let sell_profit: f64 = sells.iter().map(OrderSnapshot::total_profit).sum();
        let total_profit = buy_profit + sell_profit;

        let buy_lots: f64 = buys.iter().map(|o| o.lots).sum();
        let sell_lots: f64 = sells.iter().map(|o| o.lots).sum();

        let buy_high = buys
            .iter()
            .chain(buystops.iter())
            .map(|o| o.open_price)
            .fold(0.0_f64, f64::max);
        let buy_low = {
            let v = buys
                .iter()
                .map(|o| o.open_price)
                .fold(f64::INFINITY, f64::min);
            if v.is_finite() { v } else { 0.0 }
        };

        let sell_low = {
            let v = sells
                .iter()
                .chain(sellstops.iter())
                .map(|o| o.open_price)
                .fold(f64::INFINITY, f64::min);
            if v.is_finite() { v } else { 0.0 }
        };
        let sell_high = sells.iter().map(|o| o.open_price).fold(0.0_f64, f64::max);

        let overweight_sell = buy_lots > 0.0 && sell_lots / buy_lots > 3.0 && sell_lots - buy_lots > 0.2;
        let overweight_buy = sell_lots > 0.0 && buy_lots / sell_lots > 3.0 && buy_lots - sell_lots > 0.2;

        let buy_ss_count = Self::count_ss(&buys);
        let sell_ss_count = Self::count_ss(&sells);
        let sell_ss_when_no_buy_ss = if buy_ss_count < 1 { sell_ss_count } else { 0 };

        let mut can_buy = true;
        let mut can_sell = true;

        if !self.in_time_window(now_ts, &self.params.EA_StartTime, &self.params.EA_StopTime) {
            can_buy = false;
            can_sell = false;
        }

        if broker.leverage() < self.params.Leverage
            || !broker.is_trade_allowed()
            || !broker.is_expert_enabled()
            || broker.is_stopped()
            || (buys.len() + sells.len()) as i32 >= self.params.Totals
            || broker.spread_points() > self.params.MaxSpread
        {
            can_buy = false;
            can_sell = false;
        }

        if now_ts < self.state.pause_until {
            can_buy = false;
            can_sell = false;
        }

        if self.params.Over && buys.is_empty() {
            can_buy = false;
        }
        if self.params.Over && sells.is_empty() {
            can_sell = false;
        }

        if self.params.Over && total_profit >= self.params.CloseAll {
            self.lizong_7(broker, 0);
            if self.params.NextTime > 0 {
                self.state.pause_until = now_ts + self.params.NextTime;
            }
            return;
        }

        if !self.params.Over {
            if (sell_ss_when_no_buy_ss < 1 || !self.params.HomeopathyCloseAll)
                && buy_profit > self.params.MaxLossCloseAll
                && sell_profit > self.params.MaxLossCloseAll
            {
                if (self.params.Profit && buy_profit > self.params.StopProfit * buys.len() as f64)
                    || (!self.params.Profit && buy_profit > self.params.StopProfit)
                {
                    self.lizong_7(broker, 1);
                    return;
                }

                if (self.params.Profit && sell_profit > self.params.StopProfit * sells.len() as f64)
                    || (!self.params.Profit && sell_profit > self.params.StopProfit)
                {
                    self.lizong_7(broker, -1);
                    return;
                }
            }

            if self.params.HomeopathyCloseAll
                && (buy_ss_count > 0 || sell_ss_count > 0)
                && total_profit >= self.params.CloseAll
            {
                self.lizong_7(broker, 0);
                if self.params.NextTime > 0 {
                    self.state.pause_until = now_ts + self.params.NextTime;
                }
                return;
            }

            if total_profit >= self.params.CloseAll
                && (buy_profit <= self.params.MaxLossCloseAll
                    || sell_profit <= self.params.MaxLossCloseAll)
            {
                self.lizong_7(broker, 0);
                if self.params.NextTime > 0 {
                    self.state.pause_until = now_ts + self.params.NextTime;
                }
                return;
            }
        }

        if self.params.StopLoss != 0.0 && total_profit <= self.params.StopLoss {
            self.lizong_7(broker, 0);
            if self.params.NextTime > 0 {
                self.state.pause_until = now_ts + self.params.NextTime;
            }
            return;
        }

        if self.params.CloseBuySell {
            let buy_diff = self.lizong_10(broker, OrderType::Buy as i32, 1, 1)
                - self.lizong_10(broker, OrderType::Buy as i32, 2, 2);
            self.state.peak_buy_diff = self.state.peak_buy_diff.max(buy_diff);
            if self.state.peak_buy_diff > 0.0 && buy_diff > 0.0 && buys.len() > 3 && buy_lots > 0.0 {
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
            if self.state.peak_sell_diff > 0.0 && sell_diff > 0.0 && sells.len() > 3 && sell_lots > 0.0 {
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

        let aggressive_mode = self.params.Money == 0.0 || total_profit > self.params.Money;
        let open_gate = (self.params.OpenMode == 1 && self.state.last_bar_time != frame_bar_ts)
            || self.params.OpenMode == 2
            || self.params.OpenMode == 3;

        if open_gate {
            let buy_last_open = Self::latest_open_time(&buys);
            let sell_last_open = Self::latest_open_time(&sells);
            let limit_window =
                self.in_time_window(now_ts, &self.params.Limit_StartTime, &self.params.Limit_StopTime);

            self.try_open_buy(
                broker,
                now_ts,
                can_buy,
                ask,
                pt,
                digits,
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
                digits,
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

            self.state.last_bar_time = frame_bar_ts;
        }

        self.trail_pending_buy(
            broker,
            can_buy,
            ask,
            pt,
            digits,
            buys.len() as i32,
            aggressive_mode,
            overweight_sell,
            buy_high,
            buy_low,
            &buystops,
        );

        self.trail_pending_sell(
            broker,
            can_sell,
            bid,
            pt,
            digits,
            sells.len() as i32,
            aggressive_mode,
            overweight_buy,
            sell_low,
            sell_high,
            &sellstops,
        );
    }

    pub fn lizong_7(&mut self, broker: &mut dyn BrokerApi, side: i32) -> bool {
        for _ in 0..10 {
            let mut remain = 0;
            for o in self.my_orders(broker) {
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
                    true
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

    fn lizong_9(
        &mut self,
        broker: &mut dyn BrokerApi,
        order_type: OrderType,
        mut count: i32,
        mode: i32,
    ) {
        while count > 0 {
            let mut pool: Vec<OrderSnapshot> = self
                .my_orders(broker)
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

    fn lizong_10(
        &self,
        broker: &dyn BrokerApi,
        order_type: i32,
        sign_mode: i32,
        top_n: usize,
    ) -> f64 {
        let mut vals = Vec::new();
        for o in self.my_orders(broker) {
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

    fn my_orders(&self, broker: &dyn BrokerApi) -> Vec<OrderSnapshot> {
        broker
            .orders()
            .into_iter()
            .filter(|o| o.symbol == broker.symbol() && o.magic == self.params.Magic)
            .collect()
    }

    fn calc_lot(&self, side_count: i32) -> f64 {
        let lots = if side_count == 0 {
            self.params.lot
        } else {
            self.params.lot * self.params.K_Lot.powi(side_count) + side_count as f64 * self.params.PlusLot
        };
        round_to(lots.min(self.params.Maxlot), self.params.DigitsLot)
    }

    fn can_afford(&self, _broker: &dyn BrokerApi, _lots: f64, _count: i32) -> bool {
        // 原始 MQ4 中此保护开关常量为 true，始终放行。
        true
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

    fn count_ss(side_orders: &[OrderSnapshot]) -> i32 {
        side_orders.iter().filter(|o| o.comment == "SS").count() as i32
    }

    fn latest_open_time(side_orders: &[OrderSnapshot]) -> i64 {
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

    #[allow(clippy::too_many_arguments)]
    fn try_open_buy(
        &mut self,
        broker: &mut dyn BrokerApi,
        now_ts: i64,
        can_buy: bool,
        ask: f64,
        pt: f64,
        digits: i32,
        buys: &[OrderSnapshot],
        buystops: &[OrderSnapshot],
        buy_profit: f64,
        buy_lots: f64,
        sell_lots: f64,
        buy_high: f64,
        buy_low: f64,
        aggressive_mode: bool,
        last_open_time: i64,
        limit_window: bool,
    ) {
        if !buystops.is_empty() || buy_profit <= self.params.MaxLoss || !can_buy {
            return;
        }

        if self.params.OpenMode == 2 && now_ts - last_open_time < self.params.sleep {
            return;
        }

        let count = buys.len() as i32;
        let px = if count == 0 {
            round_to(ask + self.params.FirstStep as f64 * pt, digits)
        } else {
            let base = if aggressive_mode {
                self.params.MinDistance
            } else {
                self.params.TwoMinDistance
            };
            let step = if aggressive_mode {
                self.params.Step
            } else {
                self.params.TwoStep
            };
            let mut px = round_to(ask + base as f64 * pt, digits);
            if buy_low > 0.0 && px < round_to(buy_low - step as f64 * pt, digits) {
                px = round_to(ask + step as f64 * pt, digits);
            }
            px
        };

        let step_now = if aggressive_mode {
            self.params.Step
        } else {
            self.params.TwoStep
        };

        let cond = count == 0
            || (buy_high > 0.0
                && px >= round_to(buy_high + step_now as f64 * pt, digits)
                && sell_lots > buy_lots * 3.0
                && sell_lots - buy_lots > 0.2)
            || (buy_low > 0.0 && px <= round_to(buy_low - step_now as f64 * pt, digits))
            || (self.params.Homeopathy
                && buy_high > 0.0
                && px >= round_to(buy_high + self.params.Step as f64 * pt, digits)
                && (buy_lots - sell_lots).abs() < 1e-12);

        if !cond {
            return;
        }

        let lots = self.calc_lot(count);
        if count > 0 && !self.can_afford(broker, lots, count) {
            return;
        }

        if count > 0
            && limit_window
            && self.params.On_top_of_this_price_not_Buy_order != 0.0
            && px >= self.params.On_top_of_this_price_not_Buy_order
        {
            return;
        }

        let ss_comment = (buy_high > 0.0
            && px >= round_to(buy_high + step_now as f64 * pt, digits)
            && sell_lots > buy_lots * 3.0
            && sell_lots - buy_lots > 0.2)
            || (self.params.Homeopathy
                && buy_high > 0.0
                && px >= round_to(buy_high + self.params.Step as f64 * pt, digits)
                && (buy_lots - sell_lots).abs() < 1e-12);

        let comment = if ss_comment { "SS" } else { "NN" };
        let _ = broker.send_pending(OrderType::BuyStop, lots, px, comment);
    }

    #[allow(clippy::too_many_arguments)]
    fn try_open_sell(
        &mut self,
        broker: &mut dyn BrokerApi,
        now_ts: i64,
        can_sell: bool,
        bid: f64,
        pt: f64,
        digits: i32,
        sells: &[OrderSnapshot],
        sellstops: &[OrderSnapshot],
        sell_profit: f64,
        sell_lots: f64,
        buy_lots: f64,
        sell_low: f64,
        sell_high: f64,
        aggressive_mode: bool,
        last_open_time: i64,
        limit_window: bool,
    ) {
        if !sellstops.is_empty() || sell_profit <= self.params.MaxLoss || !can_sell {
            return;
        }

        if self.params.OpenMode == 2 && now_ts - last_open_time < self.params.sleep {
            return;
        }

        let count = sells.len() as i32;
        let px = if count == 0 {
            round_to(bid - self.params.FirstStep as f64 * pt, digits)
        } else {
            let base = if aggressive_mode {
                self.params.MinDistance
            } else {
                self.params.TwoMinDistance
            };
            let step = if aggressive_mode {
                self.params.Step
            } else {
                self.params.TwoStep
            };
            let mut px = round_to(bid - base as f64 * pt, digits);
            if sell_high > 0.0 && px < round_to(sell_high + step as f64 * pt, digits) {
                px = round_to(bid - step as f64 * pt, digits);
            }
            px
        };

        let step_now = if aggressive_mode {
            self.params.Step
        } else {
            self.params.TwoStep
        };

        let cond = count == 0
            || (sell_low > 0.0
                && px <= round_to(sell_low - step_now as f64 * pt, digits)
                && buy_lots > sell_lots * 3.0
                && buy_lots - sell_lots > 0.2)
            || (sell_high > 0.0 && px >= round_to(sell_high + step_now as f64 * pt, digits))
            || (self.params.Homeopathy
                && sell_low > 0.0
                && px <= round_to(sell_low - self.params.Step as f64 * pt, digits)
                && (buy_lots - sell_lots).abs() < 1e-12);

        if !cond {
            return;
        }

        let lots = self.calc_lot(count);
        if count > 0 && !self.can_afford(broker, lots, count) {
            return;
        }

        if count > 0
            && limit_window
            && self.params.On_under_of_this_price_not_Sell_order != 0.0
            && px <= self.params.On_under_of_this_price_not_Sell_order
        {
            return;
        }

        let ss_comment = (sell_low > 0.0
            && px <= round_to(sell_low - step_now as f64 * pt, digits)
            && buy_lots > sell_lots * 3.0
            && buy_lots - sell_lots > 0.2)
            || (self.params.Homeopathy
                && sell_low > 0.0
                && px <= round_to(sell_low - self.params.Step as f64 * pt, digits)
                && (buy_lots - sell_lots).abs() < 1e-12);

        let comment = if ss_comment { "SS" } else { "NN" };
        let _ = broker.send_pending(OrderType::SellStop, lots, px, comment);
    }

    #[allow(clippy::too_many_arguments)]
    fn trail_pending_buy(
        &mut self,
        broker: &mut dyn BrokerApi,
        can_buy: bool,
        ask: f64,
        pt: f64,
        digits: i32,
        buy_count: i32,
        aggressive_mode: bool,
        overweight_sell: bool,
        buy_high: f64,
        buy_low: f64,
        buystops: &[OrderSnapshot],
    ) {
        if !can_buy || buystops.is_empty() {
            return;
        }

        let pending = buystops
            .iter()
            .max_by(|a, b| a.open_price.partial_cmp(&b.open_price).unwrap_or(Ordering::Equal))
            .expect("buystops is not empty");

        let base = if buy_count == 0 {
            self.params.FirstStep
        } else if aggressive_mode {
            self.params.MinDistance
        } else {
            self.params.TwoMinDistance
        };
        let px = round_to(ask + base as f64 * pt, digits);

        if round_to(
            pending.open_price - self.params.StepTrallOrders as f64 * pt,
            digits,
        ) > px
        {
            let cond = if aggressive_mode {
                buy_low == 0.0
                    || px <= round_to(buy_low - self.params.Step as f64 * pt, digits)
                    || px >= round_to(buy_high + self.params.Step as f64 * pt, digits)
                    || (overweight_sell && buy_count == 0)
            } else if self.params.Money != 0.0 {
                buy_low == 0.0
                    || px <= round_to(buy_low - self.params.TwoStep as f64 * pt, digits)
                    || px >= round_to(buy_high + self.params.TwoStep as f64 * pt, digits)
                    || (overweight_sell && buy_count == 0)
            } else {
                false
            };

            if cond {
                let _ = broker.modify_order(pending.ticket, px);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn trail_pending_sell(
        &mut self,
        broker: &mut dyn BrokerApi,
        can_sell: bool,
        bid: f64,
        pt: f64,
        digits: i32,
        sell_count: i32,
        aggressive_mode: bool,
        overweight_buy: bool,
        sell_low: f64,
        sell_high: f64,
        sellstops: &[OrderSnapshot],
    ) {
        if !can_sell || sellstops.is_empty() {
            return;
        }

        let pending = sellstops
            .iter()
            .min_by(|a, b| a.open_price.partial_cmp(&b.open_price).unwrap_or(Ordering::Equal))
            .expect("sellstops is not empty");

        let base = if sell_count == 0 {
            self.params.FirstStep
        } else if aggressive_mode {
            self.params.MinDistance
        } else {
            self.params.TwoMinDistance
        };
        let px = round_to(bid - base as f64 * pt, digits);

        if round_to(
            pending.open_price + self.params.StepTrallOrders as f64 * pt,
            digits,
        ) < px
        {
            let cond = if aggressive_mode {
                sell_high == 0.0
                    || px >= round_to(sell_high + self.params.Step as f64 * pt, digits)
                    || px <= round_to(sell_low - self.params.Step as f64 * pt, digits)
                    || (overweight_buy && sell_count == 0)
            } else if self.params.Money != 0.0 {
                sell_high == 0.0
                    || px >= round_to(sell_high + self.params.TwoStep as f64 * pt, digits)
                    || px <= round_to(sell_low - self.params.TwoStep as f64 * pt, digits)
                    || (overweight_buy && sell_count == 0)
            } else {
                false
            };

            if cond {
                let _ = broker.modify_order(pending.ticket, px);
            }
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

fn round_to(value: f64, digits: i32) -> f64 {
    let scale = 10_f64.powi(digits.clamp(0, 15));
    (value * scale).round() / scale
}
