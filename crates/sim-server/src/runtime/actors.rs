//! Deterministic local liquidity/trade actors.
//!
//! Actors own only seeded decision state. Oracle observations and generated
//! commands are submitted through [`RuntimePort`], never to `sim_core::Engine`.

use super::{RuntimeError, RuntimePort, RuntimeReply, RuntimeRequest};
use hl_wire::{PriceTicks, QtyLots, SimUserId};
use oracle_hyperliquid::OracleObservation;
use sim_core::{ApplyResult, Command, PlaceOrder, Side, TimeInForce};

/// One generated command paired with the owner's deterministic result/events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorSubmission {
    pub command: Command,
    pub result: ApplyResult,
}

/// Small deterministic actor set with fixed synthetic identities.
#[derive(Clone, Debug)]
pub struct SeededLocalActors {
    random_state: u64,
    maker: SimUserId,
    taker: SimUserId,
}

impl SeededLocalActors {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { random_state: seed, maker: fixed_user('a'), taker: fixed_user('b') }
    }

    /// Applies the explicit oracle input, then places and immediately trades one
    /// deterministic quote per observation at the supplied logical time.
    pub async fn run_step(
        &mut self,
        port: &dyn RuntimePort,
        logical_time: u64,
        observations: &[OracleObservation],
    ) -> Result<Vec<ActorSubmission>, RuntimeError> {
        for observation in observations {
            let reply = request(port, RuntimeRequest::ObserveOracle(*observation)).await?;
            if !matches!(reply, RuntimeReply::OracleObserved(Ok(()))) {
                return Err(RuntimeError::OracleStale(observation.asset));
            }
        }

        let mut submissions = Vec::with_capacity(observations.len() * 2);
        for observation in observations {
            let maker_side = if self.next_u64() & 1 == 0 { Side::Bid } else { Side::Ask };
            let offset = i64::try_from(self.next_u64() % 7 + 1).expect("bounded offset");
            let quote_ticks = match maker_side {
                Side::Bid => observation.price.value().saturating_sub(offset).max(1),
                Side::Ask => observation.price.value().saturating_add(offset),
            };
            let quantity = self.next_u64() % 5 + 1;
            let quote_price = PriceTicks::new(quote_ticks).expect("positive actor price");

            let maker = Command::PlaceBatch {
                user: self.maker.clone(),
                timestamp: logical_time,
                orders: vec![PlaceOrder {
                    asset: observation.asset,
                    side: maker_side,
                    price: quote_price,
                    quantity: QtyLots::new(quantity).expect("positive actor quantity"),
                    time_in_force: TimeInForce::Gtc,
                    client_order_id: Some(format!(
                        "actor-maker-{}-{logical_time}-{}",
                        observation.asset.symbol(),
                        self.random_state
                    )),
                }],
            };
            submissions.push(submit(port, maker).await?);

            let taker = Command::PlaceBatch {
                user: self.taker.clone(),
                timestamp: logical_time,
                orders: vec![PlaceOrder {
                    asset: observation.asset,
                    side: opposite(maker_side),
                    price: quote_price,
                    quantity: QtyLots::new(quantity).expect("positive actor quantity"),
                    time_in_force: TimeInForce::Ioc,
                    client_order_id: Some(format!(
                        "actor-taker-{}-{logical_time}-{}",
                        observation.asset.symbol(),
                        self.random_state
                    )),
                }],
            };
            submissions.push(submit(port, taker).await?);
        }
        Ok(submissions)
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64: fixed integer operations give a stable stream on all targets.
        self.random_state = self.random_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.random_state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

async fn request(
    port: &dyn RuntimePort,
    request: RuntimeRequest,
) -> Result<RuntimeReply, RuntimeError> {
    port.try_request(request)?.receive().await
}

async fn submit(port: &dyn RuntimePort, command: Command) -> Result<ActorSubmission, RuntimeError> {
    let RuntimeReply::Applied(result) =
        request(port, RuntimeRequest::Apply(command.clone())).await?
    else {
        return Err(RuntimeError::ReplyDropped);
    };
    Ok(ActorSubmission { command, result })
}

fn fixed_user(hex: char) -> SimUserId {
    SimUserId::parse(&format!("0x{}", hex.to_string().repeat(40)))
        .expect("fixed actor identity is valid")
}

const fn opposite(side: Side) -> Side {
    match side {
        Side::Bid => Side::Ask,
        Side::Ask => Side::Bid,
    }
}
