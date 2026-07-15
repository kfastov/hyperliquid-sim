use hl_wire::{AssetId, PriceTicks, QtyLots, SimUserId};
use sim_core::{
    CancelOrder, CancelReject, CancelStatus, Command, CommandResult, Engine, Event, PlaceOrder,
    PlacementDisposition, PlacementReject, PlacementStatus, Side, TimeInForce,
};

fn user(byte: char) -> SimUserId {
    SimUserId::parse(&format!("0x{}", byte.to_string().repeat(40))).unwrap()
}

fn order(asset: AssetId, side: Side, price: i64, quantity: u64, tif: TimeInForce) -> PlaceOrder {
    PlaceOrder {
        asset,
        side,
        price: PriceTicks::new(price).unwrap(),
        quantity: QtyLots::new(quantity).unwrap(),
        time_in_force: tif,
        client_order_id: None,
    }
}

fn place(
    engine: &mut Engine,
    owner: &SimUserId,
    timestamp: u64,
    orders: Vec<PlaceOrder>,
) -> Vec<PlacementStatus> {
    match engine.apply(Command::PlaceBatch { user: owner.clone(), timestamp, orders }).result {
        CommandResult::Placement { statuses } => statuses,
        CommandResult::Cancellation { .. } => panic!("unexpected cancellation result"),
    }
}

fn accepted_id(status: &PlacementStatus) -> u64 {
    match status {
        PlacementStatus::Accepted { order_id, .. } => *order_id,
        PlacementStatus::Rejected { reason } => panic!("unexpected rejection: {reason}"),
    }
}

fn cancel(
    engine: &mut Engine,
    owner: &SimUserId,
    timestamp: u64,
    cancels: Vec<CancelOrder>,
) -> Vec<CancelStatus> {
    match engine.apply(Command::CancelBatch { user: owner.clone(), timestamp, cancels }).result {
        CommandResult::Cancellation { statuses } => statuses,
        CommandResult::Placement { .. } => panic!("unexpected placement result"),
    }
}

#[test]
fn resting_crossing_and_partial_fill_use_maker_price() {
    let mut engine = Engine::new(7);
    let maker = user('a');
    let taker = user('b');
    let maker_id = accepted_id(
        &place(
            &mut engine,
            &maker,
            10,
            vec![order(AssetId::BTC, Side::Ask, 100, 10, TimeInForce::Gtc)],
        )[0],
    );
    let taker_status = place(
        &mut engine,
        &taker,
        11,
        vec![order(AssetId::BTC, Side::Bid, 105, 4, TimeInForce::Gtc)],
    );

    assert!(matches!(
        taker_status[0],
        PlacementStatus::Accepted {
            filled_lots: 4,
            remaining_lots: 0,
            disposition: PlacementDisposition::Filled,
            ..
        }
    ));
    assert_eq!(engine.order(maker_id).unwrap().remaining_lots, 6);
    assert_eq!(engine.book_snapshot(AssetId::BTC).asks[0].quantity_lots, 6);
    let fill = engine
        .events()
        .iter()
        .find_map(|record| match &record.event {
            Event::Fill { fill } => Some(fill),
            _ => None,
        })
        .unwrap();
    assert_eq!(fill.price.value(), 100);
    assert_eq!(fill.quantity_lots, 4);
    assert_eq!(engine.account_snapshot(&taker).positions[&AssetId::BTC], 4);
    assert_eq!(engine.account_snapshot(&maker).positions[&AssetId::BTC], -4);
}

#[test]
fn matching_obeys_price_then_time_fifo() {
    let mut engine = Engine::new(1);
    let first = user('a');
    let second = user('b');
    let better = user('c');
    let taker = user('d');
    let first_id = accepted_id(
        &place(
            &mut engine,
            &first,
            1,
            vec![order(AssetId::ETH, Side::Ask, 101, 2, TimeInForce::Gtc)],
        )[0],
    );
    let second_id = accepted_id(
        &place(
            &mut engine,
            &second,
            2,
            vec![order(AssetId::ETH, Side::Ask, 101, 2, TimeInForce::Gtc)],
        )[0],
    );
    let better_id = accepted_id(
        &place(
            &mut engine,
            &better,
            3,
            vec![order(AssetId::ETH, Side::Ask, 100, 2, TimeInForce::Gtc)],
        )[0],
    );
    place(&mut engine, &taker, 4, vec![order(AssetId::ETH, Side::Bid, 101, 5, TimeInForce::Ioc)]);

    let makers: Vec<_> = engine
        .events()
        .iter()
        .filter_map(|record| match &record.event {
            Event::Fill { fill } => Some(fill.maker_order_id),
            _ => None,
        })
        .collect();
    assert_eq!(makers, vec![better_id, first_id, second_id]);
    assert_eq!(engine.order(second_id).unwrap().remaining_lots, 1);
}

#[test]
fn ioc_cancels_remainder_and_never_rests() {
    let mut engine = Engine::new(2);
    let status = place(
        &mut engine,
        &user('a'),
        1,
        vec![order(AssetId::SOL, Side::Bid, 50, 9, TimeInForce::Ioc)],
    );
    assert!(matches!(
        status[0],
        PlacementStatus::Accepted {
            filled_lots: 0,
            remaining_lots: 9,
            disposition: PlacementDisposition::IocRemainderCancelled,
            ..
        }
    ));
    assert!(engine.book_snapshot(AssetId::SOL).bids.is_empty());
    let id = accepted_id(&status[0]);
    assert_eq!(engine.order(id).unwrap().state, sim_core::OrderState::Cancelled);
}

#[test]
fn marketable_alo_is_atomic_but_non_marketable_alo_rests() {
    let mut engine = Engine::new(3);
    let maker = user('a');
    let taker = user('b');
    place(&mut engine, &maker, 1, vec![order(AssetId::BTC, Side::Ask, 100, 3, TimeInForce::Gtc)]);
    let before = engine.snapshot();
    let rejected = place(
        &mut engine,
        &taker,
        2,
        vec![order(AssetId::BTC, Side::Bid, 100, 2, TimeInForce::Alo)],
    );
    assert_eq!(
        rejected,
        vec![PlacementStatus::Rejected { reason: PlacementReject::MarketableAlo }]
    );
    assert_eq!(engine.snapshot(), before);

    let accepted = place(
        &mut engine,
        &taker,
        3,
        vec![order(AssetId::BTC, Side::Bid, 99, 2, TimeInForce::Alo)],
    );
    assert!(matches!(
        accepted[0],
        PlacementStatus::Accepted { disposition: PlacementDisposition::Resting, .. }
    ));
}

#[test]
fn batch_is_ordered_and_allows_partial_success() {
    let mut engine = Engine::new(4);
    let maker = user('a');
    let batch_user = user('b');
    place(&mut engine, &maker, 1, vec![order(AssetId::BTC, Side::Ask, 100, 1, TimeInForce::Gtc)]);
    let statuses = place(
        &mut engine,
        &batch_user,
        2,
        vec![
            order(AssetId::BTC, Side::Bid, 100, 1, TimeInForce::Alo),
            order(AssetId::BTC, Side::Bid, 99, 1, TimeInForce::Gtc),
        ],
    );
    assert!(matches!(statuses[0], PlacementStatus::Rejected { .. }));
    assert!(matches!(statuses[1], PlacementStatus::Accepted { order_id: 2, .. }));
}

#[test]
fn cancellation_is_owner_scoped_and_terminal_errors_are_stable() {
    let mut engine = Engine::new(5);
    let owner = user('a');
    let stranger = user('b');
    let id = accepted_id(
        &place(
            &mut engine,
            &owner,
            1,
            vec![order(AssetId::SOL, Side::Ask, 25, 7, TimeInForce::Gtc)],
        )[0],
    );

    assert_eq!(
        cancel(&mut engine, &stranger, 2, vec![CancelOrder { asset: AssetId::SOL, order_id: id }],),
        vec![CancelStatus::Rejected { order_id: id, reason: CancelReject::NotOrderOwner }]
    );
    assert_eq!(
        cancel(&mut engine, &owner, 3, vec![CancelOrder { asset: AssetId::BTC, order_id: id }],),
        vec![CancelStatus::Rejected { order_id: id, reason: CancelReject::WrongAsset }]
    );
    assert_eq!(
        cancel(&mut engine, &owner, 4, vec![CancelOrder { asset: AssetId::SOL, order_id: id }],),
        vec![CancelStatus::Cancelled { order_id: id, cancelled_lots: 7 }]
    );
    assert_eq!(
        cancel(&mut engine, &owner, 5, vec![CancelOrder { asset: AssetId::SOL, order_id: id }],),
        vec![CancelStatus::Rejected { order_id: id, reason: CancelReject::TerminalOrder }]
    );
    assert_eq!(
        cancel(&mut engine, &owner, 6, vec![CancelOrder { asset: AssetId::SOL, order_id: 999 }],),
        vec![CancelStatus::Rejected { order_id: 999, reason: CancelReject::UnknownOrder }]
    );
    assert!(engine.book_snapshot(AssetId::SOL).asks.is_empty());
}

#[test]
fn fixed_asset_books_are_independent() {
    let mut engine = Engine::new(6);
    let owner = user('a');
    place(
        &mut engine,
        &owner,
        1,
        vec![
            order(AssetId::BTC, Side::Bid, 10, 1, TimeInForce::Gtc),
            order(AssetId::ETH, Side::Ask, 20, 2, TimeInForce::Gtc),
        ],
    );
    let eth_before = engine.book_snapshot(AssetId::ETH);
    place(
        &mut engine,
        &user('b'),
        2,
        vec![order(AssetId::BTC, Side::Ask, 10, 1, TimeInForce::Gtc)],
    );
    let eth_after = engine.book_snapshot(AssetId::ETH);
    assert_eq!(eth_after.asks, eth_before.asks);
    assert_eq!(eth_after.bids, eth_before.bids);
    assert!(engine.book_snapshot(AssetId::SOL).asks.is_empty());
    assert!(engine.book_snapshot(AssetId::SOL).bids.is_empty());
}

#[test]
fn replay_is_identical_and_ids_and_events_are_monotonic() {
    let commands = [
        Command::PlaceBatch {
            user: user('a'),
            timestamp: 10,
            orders: vec![order(AssetId::BTC, Side::Ask, 100, 5, TimeInForce::Gtc)],
        },
        Command::PlaceBatch {
            user: user('b'),
            timestamp: 11,
            orders: vec![order(AssetId::BTC, Side::Bid, 100, 3, TimeInForce::Gtc)],
        },
        Command::PlaceBatch {
            user: user('c'),
            timestamp: 12,
            orders: vec![order(AssetId::SOL, Side::Bid, 50, 2, TimeInForce::Gtc)],
        },
    ];
    let replay = |seed| {
        let mut engine = Engine::new(seed);
        let results: Vec<_> =
            commands.iter().cloned().map(|command| engine.apply(command)).collect();
        (results, engine.snapshot(), engine.events().to_vec())
    };
    let first = replay(42);
    let second = replay(42);
    assert_eq!(first, second);

    let order_ids: Vec<_> = first.1.orders.iter().map(|order| order.id).collect();
    assert_eq!(order_ids, vec![1, 2, 3]);
    let event_sequences: Vec<_> = first.2.iter().map(|event| event.sequence).collect();
    assert_eq!(event_sequences, (1..=event_sequences.len() as u64).collect::<Vec<_>>());
    let trade_ids: Vec<_> = first
        .2
        .iter()
        .filter_map(|event| match &event.event {
            Event::Fill { fill } => Some(fill.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(trade_ids, vec![1]);
}
