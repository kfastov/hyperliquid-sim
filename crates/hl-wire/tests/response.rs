use hl_wire::response::{
    AllMidsResponse, CanonicalDecimal, ChannelEnvelope, ClearinghouseStateResponse,
    ExchangeResponse, L2BookResponse, MetaAndAssetCtxsResponse, MetaResponse, OpenOrder,
    WsControlResponse,
};

const USER: &str = "0xabababababababababababababababababababab";
const CLOID: &str = "0x0123456789abcdef0123456789abcdef";

fn round_trip<T>(golden: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let parsed: T = serde_json::from_str(golden).unwrap();
    assert_eq!(serde_json::to_string(&parsed).unwrap(), golden);
}

#[test]
fn meta_and_asset_contexts_match_golden_json() {
    let meta = r#"{"universe":[{"name":"BTC","szDecimals":5},{"name":"ETH","szDecimals":4},{"name":"SOL","szDecimals":2}]}"#;
    round_trip::<MetaResponse>(meta);

    let combined = format!(
        r#"[{meta},[{{"oraclePx":"65000.5","midPx":"65001","observedAt":1720000000000,"isStale":false}},{{"oraclePx":"3500","midPx":null,"observedAt":1720000000001,"isStale":true}},{{"oraclePx":"150.25","midPx":"150.2","observedAt":1720000000002,"isStale":false}}]]"#
    );
    round_trip::<MetaAndAssetCtxsResponse>(&combined);
}

#[test]
fn all_mids_and_l2_book_preserve_decimal_strings() {
    round_trip::<AllMidsResponse>(r#"{"BTC":"65000.5","ETH":"3500","SOL":"150.25"}"#);
    round_trip::<L2BookResponse>(
        r#"{"coin":"BTC","time":1720000000000,"levels":[[{"px":"65000.5","sz":"0.1","n":2}],[{"px":"65001","sz":"0.2","n":1}]]}"#,
    );
}

#[test]
fn open_orders_match_golden_json() {
    let golden = format!(
        r#"[{{"coin":"BTC","limitPx":"65000.5","oid":42,"side":"B","sz":"0.01","timestamp":1720000000000,"origSz":"0.02","cloid":"{CLOID}"}},{{"coin":"SOL","limitPx":"150","oid":43,"side":"A","sz":"1","timestamp":1720000000001,"origSz":"1"}}]"#
    );
    round_trip::<Vec<OpenOrder>>(&golden);
}

#[test]
fn simplified_clearinghouse_state_matches_golden_json() {
    let golden = format!(
        r#"{{"user":"{USER}","marginSummary":{{"accountValue":"10000","totalNtlPos":"3250.25"}},"assetPositions":[{{"position":{{"coin":"BTC","szi":"0.05","entryPx":"64000"}}}},{{"position":{{"coin":"SOL","szi":"-2","entryPx":"151.5"}}}}]}}"#
    );
    round_trip::<ClearinghouseStateResponse>(&golden);
}

#[test]
fn ordered_order_and_cancel_statuses_match_golden_json() {
    round_trip::<ExchangeResponse>(
        r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":42}},{"filled":{"totalSz":"0.1","avgPx":"65000.5","oid":43}},{"error":{"category":"domain_reject","message":"would cross"}}]}}}"#,
    );
    round_trip::<ExchangeResponse>(
        r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success",{"error":{"category":"domain_reject","message":"unknown order"}}]}}}"#,
    );
}

#[test]
fn websocket_ack_pong_and_channel_envelope_match_golden_json() {
    round_trip::<WsControlResponse>(
        r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}"#,
    );
    round_trip::<WsControlResponse>(r#"{"channel":"pong"}"#);
    round_trip::<ChannelEnvelope<L2BookResponse>>(
        r#"{"channel":"l2Book","sequence":9,"data":{"coin":"BTC","time":1720000000000,"levels":[[],[]]}}"#,
    );
}

#[test]
fn response_dtos_reject_noncanonical_decimals_and_unknown_shape() {
    for invalid in ["", "+1", "-0", "01", "1.", ".1", "1.20", "--1"] {
        assert!(CanonicalDecimal::parse(invalid).is_err(), "accepted {invalid:?}");
    }
    assert_eq!(CanonicalDecimal::parse("0").unwrap().as_str(), "0");
    assert_eq!(CanonicalDecimal::parse("-1.25").unwrap().as_str(), "-1.25");

    assert!(
        serde_json::from_str::<MetaResponse>(r#"{"universe":[],"spotMeta":{"tokens":[]}}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<WsControlResponse>(r#"{"channel":"pong","data":{}}"#).is_err());
    assert!(
        serde_json::from_str::<ChannelEnvelope<L2BookResponse>>(
            r#"{"channel":"candles","sequence":1,"data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#
        )
        .is_err()
    );
}
