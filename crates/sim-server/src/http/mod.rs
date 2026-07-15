//! Bounded Axum adapter for the `sim-header-v1` HTTP profile.

use crate::{RuntimeError, RuntimePort, RuntimeReply, RuntimeRequest};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State, rejection::BytesRejection},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::post,
};
use hl_wire::{
    AssetId, DecimalScale, PriceTicks, QtyLots, SimUserId,
    api::{
        Cloid, ErrorCategory, ExchangeAction, ExchangeEnvelope, Grouping, InfoRequest, OrderType,
        PositiveDecimal, Signature,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sim_core::{
    ApplyResult, CancelOrder, CancelStatus, Command, CommandResult, OrderSnapshot, OrderState,
    PlaceOrder, PlacementDisposition, PlacementStatus, Side, TimeInForce,
};
use std::{error::Error, fmt, sync::Arc, time::Duration};

/// Resource bounds enforced before requests reach the runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpConfig {
    pub max_batch: usize,
    pub max_body_bytes: usize,
    pub max_observation_age_ms: u64,
    pub runtime_reply_timeout: Duration,
}

/// One accepted read-only oracle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketObservation {
    pub asset: AssetId,
    pub oracle_price: PriceTicks,
    pub observed_at: u64,
    pub is_stale: bool,
}

/// Atomic read-only market view used by a single HTTP request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketSnapshot {
    pub captured_at: u64,
    pub observations: Vec<MarketObservation>,
}

/// Failures exposed by the injected market provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketViewError {
    Unavailable,
    InvalidSnapshot,
}

impl fmt::Display for MarketViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "market view is unavailable",
            Self::InvalidSnapshot => "market view snapshot is invalid",
        })
    }
}

impl Error for MarketViewError {}

/// Read-only market seam. HTTP code never owns or mutates oracle state.
pub trait MarketView: Send + Sync {
    fn snapshot(&self) -> Result<MarketSnapshot, MarketViewError>;
}

#[derive(Clone)]
struct AppState {
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    config: HttpConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedAction {
    Order { input_count: usize },
    Cancel { input_count: usize },
}

type ApiResponse = (StatusCode, Json<Value>);

#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum RawL2BookRequest {
    #[serde(rename = "l2Book")]
    L2Book { coin: String },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawExchangeEnvelope {
    action: RawExchangeAction,
    nonce: u64,
    signature: Signature,
    vault_address: Option<SimUserId>,
    #[serde(default)]
    expires_after: Option<u64>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum RawExchangeAction {
    Order { orders: Vec<RawOrderRequest>, grouping: Grouping },
    Cancel { cancels: Vec<RawCancelRequest> },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOrderRequest {
    #[serde(rename = "a")]
    asset: u8,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    limit_px: PositiveDecimal,
    #[serde(rename = "s")]
    size: PositiveDecimal,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: OrderType,
    #[serde(rename = "c", default)]
    cloid: Option<Cloid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCancelRequest {
    #[serde(rename = "a")]
    asset: u8,
    #[serde(rename = "o")]
    order_id: u64,
}

/// Builds the complete HTTP router with explicitly injected bounded seams.
pub fn router_with_config(
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    config: HttpConfig,
) -> Router {
    Router::new()
        .route("/info", post(info))
        .route("/exchange", post(exchange))
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(AppState { runtime, market, config })
}

async fn info(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResponse {
    if !has_json_content_type(&headers) {
        return unsupported_media_type();
    }
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return body_rejection(rejection),
    };
    let value = match decode_json(&body) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let request_type = value.get("type").and_then(Value::as_str);
    if !matches!(
        request_type,
        Some(
            "meta"
                | "metaAndAssetCtxs"
                | "allMids"
                | "l2Book"
                | "openOrders"
                | "clearinghouseState"
        )
    ) {
        return error(
            StatusCode::BAD_REQUEST,
            ErrorCategory::Unsupported,
            "unsupported info request",
        );
    }
    let request: InfoRequest = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) if exact_unknown_info_asset(&body) => {
            return error(StatusCode::BAD_REQUEST, ErrorCategory::Unsupported, "unsupported asset");
        }
        Err(_) => return invalid("invalid info request"),
    };

    match request {
        InfoRequest::Meta {} => ok(meta()),
        InfoRequest::MetaAndAssetCtxs {} => match market_snapshot(&state) {
            Ok(snapshot) => ok(json!([meta(), contexts(&snapshot)])),
            Err(response) => response,
        },
        InfoRequest::AllMids {} => match market_snapshot(&state) {
            Ok(snapshot) => {
                let mut mids = serde_json::Map::new();
                for asset in AssetId::ALL {
                    let Some(observation) = observation(&snapshot, asset) else {
                        return internal("market snapshot is incomplete");
                    };
                    mids.insert(
                        asset.symbol().to_owned(),
                        json!(format_price(asset, observation.oracle_price)),
                    );
                }
                ok(Value::Object(mids))
            }
            Err(response) => response,
        },
        InfoRequest::L2Book { coin } => {
            match runtime_request(&state, RuntimeRequest::Book(coin)).await {
                Ok(RuntimeReply::Book(book)) => {
                    let bids = book
                    .bids
                    .iter()
                    .map(|level| json!({"px": format_price(coin, level.price), "sz": format_size(coin, level.quantity_lots), "n": 1}))
                    .collect::<Vec<_>>();
                    let asks = book
                    .asks
                    .iter()
                    .map(|level| json!({"px": format_price(coin, level.price), "sz": format_size(coin, level.quantity_lots), "n": 1}))
                    .collect::<Vec<_>>();
                    ok(json!({"coin": coin, "time": book.sequence, "levels": [bids, asks]}))
                }
                Ok(_) => internal("runtime returned an unexpected reply"),
                Err(response) => response,
            }
        }
        InfoRequest::OpenOrders { .. } => {
            let user = match authority(&headers) {
                Ok(user) => user,
                Err(response) => return response,
            };
            open_orders(&state, user).await
        }
        InfoRequest::ClearinghouseState { .. } => {
            let user = match authority(&headers) {
                Ok(user) => user,
                Err(response) => return response,
            };
            clearinghouse_state(&state, user).await
        }
    }
}

async fn exchange(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResponse {
    if !has_json_content_type(&headers) {
        return unsupported_media_type();
    }
    let user = match authority(&headers) {
        Ok(user) => user,
        Err(response) => return response,
    };
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return body_rejection(rejection),
    };
    let value = match decode_json(&body) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let action_type =
        value.get("action").and_then(|action| action.get("type")).and_then(Value::as_str);
    if !matches!(action_type, Some("order" | "cancel")) {
        return error(
            StatusCode::BAD_REQUEST,
            ErrorCategory::Unsupported,
            "unsupported exchange action",
        );
    }
    if value.as_object().is_some_and(|object| object.contains_key("expiresAfter")) {
        return error(
            StatusCode::BAD_REQUEST,
            ErrorCategory::Unsupported,
            "expiresAfter is unsupported",
        );
    }
    let envelope: ExchangeEnvelope = match serde_json::from_value(value) {
        Ok(envelope) => envelope,
        Err(_) if exact_unknown_exchange_asset(&body) => {
            return error(StatusCode::BAD_REQUEST, ErrorCategory::Unsupported, "unsupported asset");
        }
        Err(_) => return invalid("invalid exchange request"),
    };
    if envelope.vault_address.is_some() {
        return error(
            StatusCode::BAD_REQUEST,
            ErrorCategory::Unsupported,
            "vault actions are unsupported",
        );
    }

    let timestamp = envelope.nonce;
    let (command, expected_action) = match envelope.action {
        ExchangeAction::Order { orders, .. } => {
            if orders.is_empty() {
                return invalid("order batch must not be empty");
            }
            if orders.len() > state.config.max_batch {
                return invalid("order batch exceeds configured limit");
            }
            if orders.iter().any(|order| order.reduce_only) {
                return error(
                    StatusCode::BAD_REQUEST,
                    ErrorCategory::Unsupported,
                    "reduce-only orders are unsupported",
                );
            }
            let snapshot = match market_snapshot(&state) {
                Ok(snapshot) => snapshot,
                Err(response) => return response,
            };
            for order in &orders {
                let asset = order.asset.asset();
                let Some(observation) = observation(&snapshot, asset) else {
                    return internal("market snapshot is incomplete");
                };
                if observation.is_stale {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorCategory::OracleStale,
                        "oracle observation is stale",
                    );
                }
            }
            let input_count = orders.len();
            let mut translated = Vec::with_capacity(input_count);
            for order in orders {
                let asset = order.asset.asset();
                let price = match PriceTicks::parse(order.limit_px.as_str(), price_scale(asset)) {
                    Ok(price) => price,
                    Err(_) => return invalid("price does not exactly match the asset scale"),
                };
                let quantity = match QtyLots::parse(order.size.as_str(), size_scale(asset)) {
                    Ok(quantity) => quantity,
                    Err(_) => return invalid("size does not exactly match the asset scale"),
                };
                let OrderType::Limit(limit) = order.order_type;
                translated.push(PlaceOrder {
                    asset,
                    side: if order.is_buy { Side::Bid } else { Side::Ask },
                    price,
                    quantity,
                    time_in_force: match limit.tif {
                        hl_wire::api::TimeInForce::Gtc => TimeInForce::Gtc,
                        hl_wire::api::TimeInForce::Ioc => TimeInForce::Ioc,
                        hl_wire::api::TimeInForce::Alo => TimeInForce::Alo,
                    },
                    client_order_id: order.cloid.map(|cloid| cloid.as_str().to_owned()),
                });
            }
            (
                Command::PlaceBatch { user, timestamp, orders: translated },
                ExpectedAction::Order { input_count },
            )
        }
        ExchangeAction::Cancel { cancels } => {
            if cancels.is_empty() {
                return invalid("cancel batch must not be empty");
            }
            if cancels.len() > state.config.max_batch {
                return invalid("cancel batch exceeds configured limit");
            }
            let input_count = cancels.len();
            (
                Command::CancelBatch {
                    user,
                    timestamp,
                    cancels: cancels
                        .into_iter()
                        .map(|cancel| CancelOrder {
                            asset: cancel.asset.asset(),
                            order_id: cancel.order_id,
                        })
                        .collect(),
                },
                ExpectedAction::Cancel { input_count },
            )
        }
    };

    match runtime_request(&state, RuntimeRequest::Apply(command)).await {
        Ok(RuntimeReply::Applied(result)) => exchange_result(result, expected_action),
        Ok(_) => internal("runtime returned an unexpected reply"),
        Err(response) => response,
    }
}

async fn open_orders(state: &AppState, user: SimUserId) -> ApiResponse {
    let account = match runtime_request(state, RuntimeRequest::Account(user.clone())).await {
        Ok(RuntimeReply::Account(account)) => account,
        Ok(_) => return internal("runtime returned an unexpected reply"),
        Err(response) => return response,
    };
    let mut orders = Vec::with_capacity(account.open_orders.len());
    for order_id in account.open_orders {
        match runtime_request(state, RuntimeRequest::Order(order_id)).await {
            Ok(RuntimeReply::Order(Some(order)))
                if order.user == user && order.state == OrderState::Open =>
            {
                orders.push(open_order_json(&order));
            }
            Ok(RuntimeReply::Order(_)) => {}
            Ok(_) => return internal("runtime returned an unexpected reply"),
            Err(response) => return response,
        }
    }
    ok(Value::Array(orders))
}

async fn clearinghouse_state(state: &AppState, user: SimUserId) -> ApiResponse {
    let account = match runtime_request(state, RuntimeRequest::Account(user.clone())).await {
        Ok(RuntimeReply::Account(account)) => account,
        Ok(_) => return internal("runtime returned an unexpected reply"),
        Err(response) => return response,
    };
    let positions = AssetId::ALL
        .into_iter()
        .filter_map(|asset| {
            let lots = *account.positions.get(&asset)?;
            (lots != 0).then(|| {
                json!({"position": {"coin": asset, "szi": format_signed_size(asset, lots), "entryPx": null}})
            })
        })
        .collect::<Vec<_>>();
    ok(json!({
        "user": user,
        "marginSummary": {"accountValue": account.cash_tick_lots.to_string(), "totalNtlPos": "0"},
        "assetPositions": positions
    }))
}

fn exchange_result(result: ApplyResult, expected_action: ExpectedAction) -> ApiResponse {
    let corresponds = match (&result.result, expected_action) {
        (CommandResult::Placement { statuses }, ExpectedAction::Order { input_count }) => {
            statuses.len() == input_count
        }
        (CommandResult::Cancellation { statuses }, ExpectedAction::Cancel { input_count }) => {
            statuses.len() == input_count
        }
        _ => false,
    };
    if !corresponds {
        return internal("runtime returned an invalid apply result");
    }

    let kind =
        if matches!(&result.result, CommandResult::Placement { .. }) { "order" } else { "cancel" };
    let statuses = match result.result {
        CommandResult::Placement { statuses } => statuses
            .into_iter()
            .map(|status| match status {
                PlacementStatus::Rejected { reason } => item_error(reason.to_string()),
                PlacementStatus::Accepted {
                    order_id,
                    filled_lots,
                    remaining_lots,
                    disposition,
                } => match disposition {
                    PlacementDisposition::Resting => json!({"resting": {"oid": order_id}}),
                    PlacementDisposition::Filled => {
                        filled_status(order_id, filled_lots, &result.events)
                    }
                    PlacementDisposition::IocRemainderCancelled if filled_lots == 0 => {
                        item_error("IOC order had no executable liquidity")
                    }
                    PlacementDisposition::IocRemainderCancelled => {
                        let _ = remaining_lots;
                        filled_status(order_id, filled_lots, &result.events)
                    }
                },
            })
            .collect::<Vec<_>>(),
        CommandResult::Cancellation { statuses } => statuses
            .into_iter()
            .map(|status| match status {
                CancelStatus::Cancelled { .. } => json!("success"),
                CancelStatus::Rejected { reason, .. } => item_error(reason.to_string()),
            })
            .collect::<Vec<_>>(),
    };
    ok(json!({"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}))
}

fn filled_status(order_id: u64, filled_lots: u64, events: &[sim_core::EventRecord]) -> Value {
    let fill = events.iter().find_map(|record| match &record.event {
        sim_core::Event::Fill { fill } if fill.taker_order_id == order_id => Some(fill),
        _ => None,
    });
    match fill {
        Some(fill) => json!({"filled": {
            "totalSz": format_size(fill.asset, u128::from(filled_lots)),
            "avgPx": format_price(fill.asset, fill.price),
            "oid": order_id
        }}),
        None => item_error("accepted fill did not include fill details"),
    }
}

fn open_order_json(order: &OrderSnapshot) -> Value {
    json!({
        "coin": order.asset,
        "limitPx": format_price(order.asset, order.price),
        "oid": order.id,
        "side": if order.side == Side::Bid { "B" } else { "A" },
        "sz": format_size(order.asset, u128::from(order.remaining_lots)),
        "timestamp": order.accepted_sequence,
        "origSz": order.quantity.format(size_scale(order.asset)),
        "cloid": order.client_order_id
    })
}

fn meta() -> Value {
    json!({"universe": [
        {"name": AssetId::BTC, "szDecimals": 5},
        {"name": AssetId::ETH, "szDecimals": 4},
        {"name": AssetId::SOL, "szDecimals": 2}
    ]})
}

fn contexts(snapshot: &MarketSnapshot) -> Value {
    Value::Array(
        AssetId::ALL
            .into_iter()
            .filter_map(|asset| {
                observation(snapshot, asset).map(|entry| {
                    json!({
                        "oraclePx": format_price(asset, entry.oracle_price),
                        "midPx": format_price(asset, entry.oracle_price),
                        "observedAt": entry.observed_at,
                        "isStale": entry.is_stale
                    })
                })
            })
            .collect(),
    )
}

fn observation(snapshot: &MarketSnapshot, asset: AssetId) -> Option<&MarketObservation> {
    snapshot.observations.iter().find(|entry| entry.asset == asset)
}

fn market_snapshot(state: &AppState) -> Result<MarketSnapshot, ApiResponse> {
    let mut snapshot =
        state.market.snapshot().map_err(|_| internal("market view is unavailable"))?;
    let complete_and_unique = AssetId::ALL.into_iter().all(|asset| {
        snapshot.observations.iter().filter(|entry| entry.asset == asset).count() == 1
    }) && snapshot.observations.len() == AssetId::ALL.len();
    if !complete_and_unique {
        return Err(internal("market snapshot is invalid"));
    }
    for observation in &mut snapshot.observations {
        let stale_by_timestamp = match snapshot.captured_at.checked_sub(observation.observed_at) {
            Some(age) => age > state.config.max_observation_age_ms,
            None => true,
        };
        observation.is_stale |= stale_by_timestamp;
    }
    Ok(snapshot)
}

async fn runtime_request(
    state: &AppState,
    request: RuntimeRequest,
) -> Result<RuntimeReply, ApiResponse> {
    let pending = state.runtime.try_request(request).map_err(runtime_error)?;
    match tokio::time::timeout(state.config.runtime_reply_timeout, pending.receive()).await {
        Ok(reply) => reply.map_err(runtime_error),
        Err(_) => Err(unavailable("runtime is unavailable")),
    }
}

fn runtime_error(error_value: RuntimeError) -> ApiResponse {
    match error_value {
        RuntimeError::Overloaded => {
            error(StatusCode::TOO_MANY_REQUESTS, ErrorCategory::Overloaded, "runtime is overloaded")
        }
        RuntimeError::ShuttingDown | RuntimeError::ReplyDropped => {
            internal("runtime is unavailable")
        }
    }
}

fn authority(headers: &HeaderMap) -> Result<SimUserId, ApiResponse> {
    let value =
        headers.get("x-sim-user").and_then(|value| value.to_str().ok()).ok_or_else(unauthorized)?;
    SimUserId::parse(value).map_err(|_| unauthorized())
}

fn decode_json(body: &[u8]) -> Result<Value, ApiResponse> {
    serde_json::from_slice(body).map_err(|_| invalid("malformed JSON request"))
}

fn exact_unknown_info_asset(body: &[u8]) -> bool {
    let Ok(RawL2BookRequest::L2Book { coin }) = serde_json::from_slice(body) else {
        return false;
    };
    AssetId::from_symbol(&coin).is_err()
}

fn exact_unknown_exchange_asset(body: &[u8]) -> bool {
    let Ok(envelope) = serde_json::from_slice::<RawExchangeEnvelope>(body) else {
        return false;
    };
    let _syntax =
        (envelope.nonce, envelope.signature, envelope.vault_address, envelope.expires_after);
    match envelope.action {
        RawExchangeAction::Order { orders, grouping } => {
            let _grouping = grouping;
            orders.into_iter().any(|order| {
                let _syntax = (
                    order.is_buy,
                    order.limit_px,
                    order.size,
                    order.reduce_only,
                    order.order_type,
                    order.cloid,
                );
                order.asset > AssetId::SOL.value()
            })
        }
        RawExchangeAction::Cancel { cancels } => cancels.into_iter().any(|cancel| {
            let _order_id = cancel.order_id;
            cancel.asset > AssetId::SOL.value()
        }),
    }
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value.to_str().is_ok_and(is_json_media_type)
}

fn is_json_media_type(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut cursor = 0;
    skip_ows(bytes, &mut cursor);
    let Some(media_type) = parse_token(bytes, &mut cursor) else {
        return false;
    };
    if bytes.get(cursor) != Some(&b'/') {
        return false;
    }
    cursor += 1;
    let Some(media_subtype) = parse_token(bytes, &mut cursor) else {
        return false;
    };
    if !media_type.eq_ignore_ascii_case(b"application")
        || !media_subtype.eq_ignore_ascii_case(b"json")
    {
        return false;
    }

    loop {
        skip_ows(bytes, &mut cursor);
        if cursor == bytes.len() {
            return true;
        }
        if bytes.get(cursor) != Some(&b';') {
            return false;
        }
        cursor += 1;
        skip_ows(bytes, &mut cursor);
        if parse_token(bytes, &mut cursor).is_none() {
            return false;
        }
        skip_ows(bytes, &mut cursor);
        if bytes.get(cursor) != Some(&b'=') {
            return false;
        }
        cursor += 1;
        skip_ows(bytes, &mut cursor);
        if bytes.get(cursor) == Some(&b'"') {
            if !parse_quoted_string(bytes, &mut cursor) {
                return false;
            }
        } else if parse_token(bytes, &mut cursor).is_none() {
            return false;
        }
    }
}

fn skip_ows(bytes: &[u8], cursor: &mut usize) {
    while matches!(bytes.get(*cursor), Some(b' ' | b'\t')) {
        *cursor += 1;
    }
}

fn parse_token<'a>(bytes: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let start = *cursor;
    while bytes.get(*cursor).is_some_and(|byte| is_token_byte(*byte)) {
        *cursor += 1;
    }
    (*cursor > start).then(|| &bytes[start..*cursor])
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn parse_quoted_string(bytes: &[u8], cursor: &mut usize) -> bool {
    *cursor += 1;
    while let Some(&byte) = bytes.get(*cursor) {
        *cursor += 1;
        match byte {
            b'"' => return true,
            b'\\' => {
                let Some(&escaped) = bytes.get(*cursor) else {
                    return false;
                };
                if !matches!(escaped, b'\t' | b' '..=b'~') {
                    return false;
                }
                *cursor += 1;
            }
            b'\t' | b' ' | b'!' | b'#'..=b'[' | b']'..=b'~' => {}
            _ => return false,
        }
    }
    false
}

fn body_rejection(rejection: BytesRejection) -> ApiResponse {
    if rejection.into_response().status() == StatusCode::PAYLOAD_TOO_LARGE {
        error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCategory::InvalidRequest,
            "request body exceeds the configured limit",
        )
    } else {
        invalid("request body could not be read")
    }
}

fn price_scale(asset: AssetId) -> DecimalScale {
    DecimalScale::new(match asset {
        AssetId::BTC => 1,
        AssetId::ETH => 2,
        AssetId::SOL => 3,
    })
    .expect("fixed scales are valid")
}

fn size_scale(asset: AssetId) -> DecimalScale {
    DecimalScale::new(match asset {
        AssetId::BTC => 5,
        AssetId::ETH => 4,
        AssetId::SOL => 2,
    })
    .expect("fixed scales are valid")
}

fn format_price(asset: AssetId, price: PriceTicks) -> String {
    price.format(price_scale(asset))
}

fn format_size(asset: AssetId, lots: u128) -> String {
    let places = usize::from(size_scale(asset).decimal_places());
    format_unsigned_scaled(lots, places)
}

fn format_signed_size(asset: AssetId, lots: i128) -> String {
    let places = usize::from(size_scale(asset).decimal_places());
    if lots < 0 {
        format!("-{}", format_unsigned_scaled(lots.unsigned_abs(), places))
    } else {
        format_unsigned_scaled(lots.unsigned_abs(), places)
    }
}

fn format_unsigned_scaled(value: u128, places: usize) -> String {
    if places == 0 {
        return value.to_string();
    }
    let factor = 10_u128.pow(u32::try_from(places).expect("fixed scale fits u32"));
    let whole = value / factor;
    let remainder = value % factor;
    if remainder == 0 {
        return whole.to_string();
    }
    let mut fraction = format!("{remainder:0places$}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    format!("{whole}.{fraction}")
}

fn item_error(message: impl Into<String>) -> Value {
    json!({"error": {"category": ErrorCategory::DomainReject, "message": message.into()}})
}

fn ok(value: impl Serialize) -> ApiResponse {
    (StatusCode::OK, Json(serde_json::to_value(value).expect("response serialization succeeds")))
}

fn invalid(message: &'static str) -> ApiResponse {
    error(StatusCode::BAD_REQUEST, ErrorCategory::InvalidRequest, message)
}

fn unsupported_media_type() -> ApiResponse {
    error(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ErrorCategory::InvalidRequest,
        "Content-Type must be application/json",
    )
}

fn unauthorized() -> ApiResponse {
    error(
        StatusCode::UNAUTHORIZED,
        ErrorCategory::UnauthorizedSimUser,
        "a normalized X-Sim-User header is required",
    )
}

fn internal(message: &'static str) -> ApiResponse {
    error(StatusCode::INTERNAL_SERVER_ERROR, ErrorCategory::Internal, message)
}

fn unavailable(message: &'static str) -> ApiResponse {
    error(StatusCode::SERVICE_UNAVAILABLE, ErrorCategory::Internal, message)
}

fn error(status: StatusCode, category: ErrorCategory, message: impl Into<String>) -> ApiResponse {
    (status, Json(json!({"error": {"category": category, "message": message.into()}})))
}
