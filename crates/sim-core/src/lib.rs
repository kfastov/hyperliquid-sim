//! Deterministic single-owner exchange domain core.
//!
//! The engine owns all mutation and receives logical time explicitly through
//! [`Command`]. It performs no I/O and reads neither wall-clock time nor system
//! entropy.

use hl_wire::{AssetId, PriceTicks, QtyLots, SimUserId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;

pub type OrderId = u64;
pub type TradeId = u64;
pub type EventSequence = u64;
pub type LogicalTimestamp = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Alo,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrder {
    pub asset: AssetId,
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: QtyLots,
    pub time_in_force: TimeInForce,
    pub client_order_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CancelOrder {
    pub asset: AssetId,
    pub order_id: OrderId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    PlaceBatch { user: SimUserId, timestamp: LogicalTimestamp, orders: Vec<PlaceOrder> },
    CancelBatch { user: SimUserId, timestamp: LogicalTimestamp, cancels: Vec<CancelOrder> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    Open,
    Filled,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub id: OrderId,
    pub user: SimUserId,
    pub asset: AssetId,
    pub side: Side,
    pub price: PriceTicks,
    pub quantity: QtyLots,
    pub remaining_lots: u64,
    pub time_in_force: TimeInForce,
    pub client_order_id: Option<String>,
    pub accepted_sequence: u64,
    pub state: OrderState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementDisposition {
    Resting,
    Filled,
    IocRemainderCancelled,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementReject {
    #[error("ALO order would immediately match")]
    MarketableAlo,
    #[error("order would trade against the same user")]
    SelfTrade,
    #[error("account arithmetic overflow")]
    ArithmeticOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PlacementStatus {
    Accepted {
        order_id: OrderId,
        filled_lots: u64,
        remaining_lots: u64,
        disposition: PlacementDisposition,
    },
    Rejected {
        reason: PlacementReject,
    },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReject {
    #[error("unknown order")]
    UnknownOrder,
    #[error("order belongs to another user")]
    NotOrderOwner,
    #[error("order belongs to another asset")]
    WrongAsset,
    #[error("order is already terminal")]
    TerminalOrder,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CancelStatus {
    Cancelled { order_id: OrderId, cancelled_lots: u64 },
    Rejected { order_id: OrderId, reason: CancelReject },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandResult {
    Placement { statuses: Vec<PlacementStatus> },
    Cancellation { statuses: Vec<CancelStatus> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApplyResult {
    pub result: CommandResult,
    /// Events emitted by this command only, in total sequence order.
    pub events: Vec<EventRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub trade_id: TradeId,
    pub asset: AssetId,
    pub price: PriceTicks,
    pub quantity_lots: u64,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker: SimUserId,
    pub taker: SimUserId,
    pub taker_side: Side,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    OrderAccepted {
        order: OrderSnapshot,
    },
    Fill {
        fill: Fill,
    },
    OrderUpdated {
        order_id: OrderId,
        user: SimUserId,
        asset: AssetId,
        remaining_lots: u64,
        state: OrderState,
    },
    OrderCancelled {
        order_id: OrderId,
        user: SimUserId,
        asset: AssetId,
        cancelled_lots: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub sequence: EventSequence,
    pub timestamp: LogicalTimestamp,
    pub event: Event,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub user: Option<SimUserId>,
    /// Signed synthetic position, in lots, keyed by fixed asset ID.
    pub positions: BTreeMap<AssetId, i128>,
    /// Synthetic cash delta in exact tick-lots. No collateral semantics are implied.
    pub cash_tick_lots: i128,
    pub open_orders: Vec<OrderId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: PriceTicks,
    pub quantity_lots: u128,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub asset: AssetId,
    /// Descending price order.
    pub bids: Vec<BookLevel>,
    /// Ascending price order.
    pub asks: Vec<BookLevel>,
    pub sequence: EventSequence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EngineSnapshot {
    pub seed: u64,
    pub sequence: EventSequence,
    pub books: Vec<BookSnapshot>,
    pub accounts: Vec<AccountSnapshot>,
    pub orders: Vec<OrderSnapshot>,
}

#[derive(Clone, Debug, Default)]
struct Account {
    positions: BTreeMap<AssetId, i128>,
    cash_tick_lots: i128,
}

#[derive(Clone, Debug)]
struct Order {
    id: OrderId,
    user: SimUserId,
    asset: AssetId,
    side: Side,
    price: PriceTicks,
    quantity: QtyLots,
    remaining_lots: u64,
    time_in_force: TimeInForce,
    client_order_id: Option<String>,
    accepted_sequence: u64,
    state: OrderState,
}

impl Order {
    fn snapshot(&self) -> OrderSnapshot {
        OrderSnapshot {
            id: self.id,
            user: self.user.clone(),
            asset: self.asset,
            side: self.side,
            price: self.price,
            quantity: self.quantity,
            remaining_lots: self.remaining_lots,
            time_in_force: self.time_in_force,
            client_order_id: self.client_order_id.clone(),
            accepted_sequence: self.accepted_sequence,
            state: self.state,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Book {
    bids: BTreeMap<PriceTicks, VecDeque<OrderId>>,
    asks: BTreeMap<PriceTicks, VecDeque<OrderId>>,
}

impl Book {
    fn would_cross(&self, side: Side, limit: PriceTicks) -> bool {
        match side {
            Side::Bid => self.asks.first_key_value().is_some_and(|(price, _)| *price <= limit),
            Side::Ask => self.bids.last_key_value().is_some_and(|(price, _)| *price >= limit),
        }
    }

    fn best_crossing(&self, side: Side, limit: PriceTicks) -> Option<OrderId> {
        let (price, queue) = match side {
            Side::Bid => self.asks.first_key_value()?,
            Side::Ask => self.bids.last_key_value()?,
        };
        let crosses = match side {
            Side::Bid => *price <= limit,
            Side::Ask => *price >= limit,
        };
        crosses.then(|| queue.front().copied()).flatten()
    }

    fn rest(&mut self, order: &Order) {
        let side = match order.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        side.entry(order.price).or_default().push_back(order.id);
    }

    fn remove(&mut self, side: Side, price: PriceTicks, order_id: OrderId) {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let remove_level = if let Some(queue) = levels.get_mut(&price) {
            queue.retain(|candidate| *candidate != order_id);
            queue.is_empty()
        } else {
            false
        };
        if remove_level {
            levels.remove(&price);
        }
    }
}

/// Single-owner deterministic exchange state machine.
#[derive(Clone, Debug)]
pub struct Engine {
    seed: u64,
    books: BTreeMap<AssetId, Book>,
    accounts: BTreeMap<SimUserId, Account>,
    orders: BTreeMap<OrderId, Order>,
    events: Vec<EventRecord>,
    next_order_id: OrderId,
    next_trade_id: TradeId,
    next_event_sequence: EventSequence,
    next_accepted_sequence: u64,
}

impl Engine {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let books = AssetId::ALL.into_iter().map(|asset| (asset, Book::default())).collect();
        Self {
            seed,
            books,
            accounts: BTreeMap::new(),
            orders: BTreeMap::new(),
            events: Vec::new(),
            next_order_id: 1,
            next_trade_id: 1,
            next_event_sequence: 1,
            next_accepted_sequence: 1,
        }
    }

    /// Applies one batch atomically as an engine turn. Entries are evaluated in
    /// request order; an entry rejection does not roll back earlier entries.
    pub fn apply(&mut self, command: Command) -> ApplyResult {
        let event_start = self.events.len();
        let result = match command {
            Command::PlaceBatch { user, timestamp, orders } => {
                let statuses = orders
                    .into_iter()
                    .map(|request| self.place_one(&user, timestamp, request))
                    .collect();
                CommandResult::Placement { statuses }
            }
            Command::CancelBatch { user, timestamp, cancels } => {
                let statuses = cancels
                    .into_iter()
                    .map(|request| self.cancel_one(&user, timestamp, request))
                    .collect();
                CommandResult::Cancellation { statuses }
            }
        };
        ApplyResult { result, events: self.events[event_start..].to_vec() }
    }

    #[must_use]
    pub fn events(&self) -> &[EventRecord] {
        &self.events
    }

    #[must_use]
    pub fn events_after(&self, sequence: EventSequence) -> Vec<EventRecord> {
        self.events.iter().filter(|event| event.sequence > sequence).cloned().collect()
    }

    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<OrderSnapshot> {
        self.orders.get(&order_id).map(Order::snapshot)
    }

    #[must_use]
    pub fn book_snapshot(&self, asset: AssetId) -> BookSnapshot {
        let book = self.books.get(&asset).expect("all fixed books exist");
        let aggregate = |levels: &BTreeMap<PriceTicks, VecDeque<OrderId>>, descending: bool| {
            let mut result: Vec<_> = levels
                .iter()
                .map(|(price, queue)| BookLevel {
                    price: *price,
                    quantity_lots: queue
                        .iter()
                        .filter_map(|id| self.orders.get(id))
                        .map(|order| u128::from(order.remaining_lots))
                        .sum(),
                })
                .filter(|level| level.quantity_lots > 0)
                .collect();
            if descending {
                result.reverse();
            }
            result
        };
        BookSnapshot {
            asset,
            bids: aggregate(&book.bids, true),
            asks: aggregate(&book.asks, false),
            sequence: self.next_event_sequence - 1,
        }
    }

    #[must_use]
    pub fn account_snapshot(&self, user: &SimUserId) -> AccountSnapshot {
        let account = self.accounts.get(user).cloned().unwrap_or_default();
        AccountSnapshot {
            user: Some(user.clone()),
            positions: account.positions,
            cash_tick_lots: account.cash_tick_lots,
            open_orders: self
                .orders
                .values()
                .filter(|order| order.user == *user && order.state == OrderState::Open)
                .map(|order| order.id)
                .collect(),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            seed: self.seed,
            sequence: self.next_event_sequence - 1,
            books: AssetId::ALL.into_iter().map(|asset| self.book_snapshot(asset)).collect(),
            accounts: self.accounts.keys().map(|user| self.account_snapshot(user)).collect(),
            orders: self.orders.values().map(Order::snapshot).collect(),
        }
    }

    fn place_one(
        &mut self,
        user: &SimUserId,
        timestamp: LogicalTimestamp,
        request: PlaceOrder,
    ) -> PlacementStatus {
        // Entry-level checkpoint makes arithmetic failure, ALO rejection, and
        // self-trade rejection mutation-free, while preserving prior successful
        // batch entries.
        let checkpoint = self.clone();
        match self.place_one_inner(user, timestamp, request) {
            Ok(status) => status,
            Err(reason) => {
                *self = checkpoint;
                PlacementStatus::Rejected { reason }
            }
        }
    }

    fn place_one_inner(
        &mut self,
        user: &SimUserId,
        timestamp: LogicalTimestamp,
        request: PlaceOrder,
    ) -> Result<PlacementStatus, PlacementReject> {
        let book = self.books.get(&request.asset).expect("all fixed books exist");
        if request.time_in_force == TimeInForce::Alo
            && book.would_cross(request.side, request.price)
        {
            return Err(PlacementReject::MarketableAlo);
        }

        let order_id = self.allocate_order_id();
        let accepted_sequence = self.allocate_accepted_sequence();
        let mut order = Order {
            id: order_id,
            user: user.clone(),
            asset: request.asset,
            side: request.side,
            price: request.price,
            quantity: request.quantity,
            remaining_lots: request.quantity.value(),
            time_in_force: request.time_in_force,
            client_order_id: request.client_order_id,
            accepted_sequence,
            state: OrderState::Open,
        };
        self.accounts.entry(user.clone()).or_default();
        self.orders.insert(order_id, order.clone());
        self.emit(timestamp, Event::OrderAccepted { order: order.snapshot() });

        while order.remaining_lots > 0 {
            let maker_id = match self
                .books
                .get(&order.asset)
                .expect("all fixed books exist")
                .best_crossing(order.side, order.price)
            {
                Some(id) => id,
                None => break,
            };
            let maker = self.orders.get(&maker_id).expect("book references live order").clone();
            let fill_lots = order.remaining_lots.min(maker.remaining_lots);
            self.apply_account_fill(&order, &maker, fill_lots)?;

            order.remaining_lots -= fill_lots;
            let maker_remaining = maker.remaining_lots - fill_lots;
            let maker_state =
                if maker_remaining == 0 { OrderState::Filled } else { OrderState::Open };
            {
                let stored_maker = self.orders.get_mut(&maker_id).expect("maker exists");
                stored_maker.remaining_lots = maker_remaining;
                stored_maker.state = maker_state;
            }
            if maker_state == OrderState::Filled {
                self.books.get_mut(&maker.asset).expect("all fixed books exist").remove(
                    maker.side,
                    maker.price,
                    maker.id,
                );
            }

            let trade_id = self.allocate_trade_id();
            self.emit(
                timestamp,
                Event::Fill {
                    fill: Fill {
                        trade_id,
                        asset: order.asset,
                        price: maker.price,
                        quantity_lots: fill_lots,
                        maker_order_id: maker.id,
                        taker_order_id: order.id,
                        maker: maker.user.clone(),
                        taker: order.user.clone(),
                        taker_side: order.side,
                    },
                },
            );
            self.emit(
                timestamp,
                Event::OrderUpdated {
                    order_id: maker.id,
                    user: maker.user,
                    asset: maker.asset,
                    remaining_lots: maker_remaining,
                    state: maker_state,
                },
            );
        }

        let filled_lots = order.quantity.value() - order.remaining_lots;
        let disposition = if order.remaining_lots == 0 {
            order.state = OrderState::Filled;
            PlacementDisposition::Filled
        } else if order.time_in_force == TimeInForce::Ioc {
            order.state = OrderState::Cancelled;
            PlacementDisposition::IocRemainderCancelled
        } else {
            self.books.get_mut(&order.asset).expect("all fixed books exist").rest(&order);
            PlacementDisposition::Resting
        };
        self.orders.insert(order.id, order.clone());
        self.emit(
            timestamp,
            Event::OrderUpdated {
                order_id: order.id,
                user: order.user,
                asset: order.asset,
                remaining_lots: order.remaining_lots,
                state: order.state,
            },
        );
        Ok(PlacementStatus::Accepted {
            order_id,
            filled_lots,
            remaining_lots: order.remaining_lots,
            disposition,
        })
    }

    fn cancel_one(
        &mut self,
        user: &SimUserId,
        timestamp: LogicalTimestamp,
        request: CancelOrder,
    ) -> CancelStatus {
        let Some(order) = self.orders.get(&request.order_id).cloned() else {
            return CancelStatus::Rejected {
                order_id: request.order_id,
                reason: CancelReject::UnknownOrder,
            };
        };
        let rejection = if order.user != *user {
            Some(CancelReject::NotOrderOwner)
        } else if order.asset != request.asset {
            Some(CancelReject::WrongAsset)
        } else if order.state != OrderState::Open {
            Some(CancelReject::TerminalOrder)
        } else {
            None
        };
        if let Some(reason) = rejection {
            return CancelStatus::Rejected { order_id: request.order_id, reason };
        }

        self.books.get_mut(&order.asset).expect("all fixed books exist").remove(
            order.side,
            order.price,
            order.id,
        );
        let cancelled_lots = order.remaining_lots;
        let stored = self.orders.get_mut(&order.id).expect("order exists");
        stored.state = OrderState::Cancelled;
        self.emit(
            timestamp,
            Event::OrderCancelled {
                order_id: order.id,
                user: order.user,
                asset: order.asset,
                cancelled_lots,
            },
        );
        CancelStatus::Cancelled { order_id: order.id, cancelled_lots }
    }

    fn apply_account_fill(
        &mut self,
        taker: &Order,
        maker: &Order,
        quantity_lots: u64,
    ) -> Result<(), PlacementReject> {
        if taker.user == maker.user {
            return Err(PlacementReject::SelfTrade);
        }
        let quantity = i128::from(quantity_lots);
        let notional = i128::from(maker.price.value())
            .checked_mul(quantity)
            .ok_or(PlacementReject::ArithmeticOverflow)?;
        let (buyer, seller) = match taker.side {
            Side::Bid => (&taker.user, &maker.user),
            Side::Ask => (&maker.user, &taker.user),
        };
        let buyer_account = self.accounts.get(buyer).cloned().unwrap_or_default();
        let seller_account = self.accounts.get(seller).cloned().unwrap_or_default();
        let buyer_position = buyer_account.positions.get(&taker.asset).copied().unwrap_or(0);
        let seller_position = seller_account.positions.get(&taker.asset).copied().unwrap_or(0);
        let next_buyer_position =
            buyer_position.checked_add(quantity).ok_or(PlacementReject::ArithmeticOverflow)?;
        let next_seller_position =
            seller_position.checked_sub(quantity).ok_or(PlacementReject::ArithmeticOverflow)?;
        let next_buyer_cash = buyer_account
            .cash_tick_lots
            .checked_sub(notional)
            .ok_or(PlacementReject::ArithmeticOverflow)?;
        let next_seller_cash = seller_account
            .cash_tick_lots
            .checked_add(notional)
            .ok_or(PlacementReject::ArithmeticOverflow)?;

        let buyer_account = self.accounts.entry(buyer.clone()).or_default();
        buyer_account.positions.insert(taker.asset, next_buyer_position);
        buyer_account.cash_tick_lots = next_buyer_cash;
        let seller_account = self.accounts.entry(seller.clone()).or_default();
        seller_account.positions.insert(taker.asset, next_seller_position);
        seller_account.cash_tick_lots = next_seller_cash;
        Ok(())
    }

    fn emit(&mut self, timestamp: LogicalTimestamp, event: Event) {
        let sequence = self.next_event_sequence;
        self.next_event_sequence =
            self.next_event_sequence.checked_add(1).expect("event ID exhausted");
        self.events.push(EventRecord { sequence, timestamp, event });
    }

    fn allocate_order_id(&mut self) -> OrderId {
        let id = self.next_order_id;
        self.next_order_id = self.next_order_id.checked_add(1).expect("order ID exhausted");
        id
    }

    fn allocate_trade_id(&mut self) -> TradeId {
        let id = self.next_trade_id;
        self.next_trade_id = self.next_trade_id.checked_add(1).expect("trade ID exhausted");
        id
    }

    fn allocate_accepted_sequence(&mut self) -> u64 {
        let sequence = self.next_accepted_sequence;
        self.next_accepted_sequence =
            self.next_accepted_sequence.checked_add(1).expect("accepted sequence exhausted");
        sequence
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(0)
    }
}
