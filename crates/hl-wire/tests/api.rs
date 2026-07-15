use hl_wire::AssetId;
use hl_wire::api::{
    ApiAssetId, Cloid, ErrorCategory, ErrorEnvelope, ExchangeEnvelope, InfoRequest,
    PositiveDecimal, SignatureComponent, WsRequest,
};

const USER: &str = "0xabababababababababababababababababababab";
const R: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const S: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const CLOID: &str = "0x0123456789abcdef0123456789abcdef";

fn round_trip<T>(golden: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let parsed: T = serde_json::from_str(golden).unwrap();
    assert_eq!(serde_json::to_string(&parsed).unwrap(), golden);
}

#[test]
fn error_envelope_has_stable_golden_shape() {
    let error = ErrorEnvelope::new(ErrorCategory::InvalidRequest, "bad request");
    assert_eq!(
        serde_json::to_string(&error).unwrap(),
        r#"{"error":{"category":"invalid_request","message":"bad request"}}"#
    );
    assert!(
        serde_json::from_str::<ErrorEnvelope>(
            r#"{"error":{"category":"new_category","message":"bad request"}}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<ErrorEnvelope>(
            r#"{"error":{"category":"invalid_request","message":"bad request","code":1}}"#
        )
        .is_err()
    );
}

#[test]
fn info_requests_match_supported_golden_json() {
    for golden in [
        r#"{"type":"meta"}"#,
        r#"{"type":"metaAndAssetCtxs"}"#,
        r#"{"type":"allMids"}"#,
        r#"{"type":"l2Book","coin":"BTC"}"#,
        &format!(r#"{{"type":"openOrders","user":"{USER}"}}"#),
        &format!(r#"{{"type":"clearinghouseState","user":"{USER}"}}"#),
    ] {
        round_trip::<InfoRequest>(golden);
    }
}

#[test]
fn exchange_order_matches_compact_golden_json() {
    let golden = format!(
        r#"{{"action":{{"type":"order","orders":[{{"a":0,"b":true,"p":"65000.5","s":"0.01","r":false,"t":{{"limit":{{"tif":"Gtc"}}}},"c":"{CLOID}"}}],"grouping":"na"}},"nonce":1720000000000,"signature":{{"r":"{R}","s":"{S}","v":27}},"vaultAddress":null,"expiresAfter":1720000060000}}"#
    );
    round_trip::<ExchangeEnvelope>(&golden);
}

#[test]
fn exchange_cancel_matches_compact_golden_json() {
    let golden = format!(
        r#"{{"action":{{"type":"cancel","cancels":[{{"a":2,"o":42}}]}},"nonce":1720000000001,"signature":{{"r":"{R}","s":"{S}","v":28}},"vaultAddress":"{USER}"}}"#
    );
    round_trip::<ExchangeEnvelope>(&golden);
}

#[test]
fn websocket_controls_and_subscriptions_match_golden_json() {
    for golden in [
        r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#,
        r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}"#,
        r#"{"method":"unsubscribe","subscription":{"type":"trades","coin":"SOL"}}"#,
        &format!(
            r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{USER}"}}}}"#
        ),
        r#"{"method":"ping"}"#,
    ] {
        round_trip::<WsRequest>(golden);
    }
}

#[test]
fn validated_wire_scalars_reject_noncanonical_or_malformed_values() {
    assert_eq!(PositiveDecimal::parse("1.25").unwrap().as_str(), "1.25");
    assert_eq!(ApiAssetId::new(AssetId::ETH).asset(), AssetId::ETH);
    assert_eq!(Cloid::parse(CLOID).unwrap().as_str(), CLOID);
    assert_eq!(SignatureComponent::parse(R).unwrap().as_str(), R);

    for invalid in ["", "0", "01", "1.", ".1", "+1", "-1", "1.20", " 1"] {
        assert!(PositiveDecimal::parse(invalid).is_err(), "accepted {invalid:?}");
    }
    for invalid in ["0x1", "0123456789abcdef0123456789abcdef", "0xABCDEF0123456789abcdef0123456789"]
    {
        assert!(Cloid::parse(invalid).is_err(), "accepted {invalid:?}");
    }
}

#[test]
fn unsupported_or_semantically_extra_requests_fail_closed() {
    for invalid in [
        r#"{"type":"spotMeta"}"#,
        r#"{"type":"l2Book","coin":"DOGE"}"#,
        r#"{"type":"meta","coin":"BTC"}"#,
    ] {
        assert!(serde_json::from_str::<InfoRequest>(invalid).is_err(), "accepted {invalid}");
    }

    for invalid in [
        r#"{"method":"post","id":1}"#,
        r#"{"method":"subscribe","subscription":{"type":"candles","coin":"BTC"}}"#,
        r#"{"method":"ping","extra":true}"#,
    ] {
        assert!(serde_json::from_str::<WsRequest>(invalid).is_err(), "accepted {invalid}");
    }

    let unsupported_action = format!(
        r#"{{"action":{{"type":"modify","oid":1}},"nonce":1,"signature":{{"r":"{R}","s":"{S}","v":27}},"vaultAddress":null}}"#
    );
    assert!(serde_json::from_str::<ExchangeEnvelope>(&unsupported_action).is_err());

    let unsupported_asset = format!(
        r#"{{"action":{{"type":"cancel","cancels":[{{"a":3,"o":1}}]}},"nonce":1,"signature":{{"r":"{R}","s":"{S}","v":27}},"vaultAddress":null}}"#
    );
    assert!(serde_json::from_str::<ExchangeEnvelope>(&unsupported_asset).is_err());
}
