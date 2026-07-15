use hl_wire::{AssetId, DecimalScale, PriceTicks, QtyLots, SimUserId};

#[test]
fn fixed_asset_mapping_is_stable() {
    assert_eq!(AssetId::BTC.value(), 0);
    assert_eq!(AssetId::ETH.value(), 1);
    assert_eq!(AssetId::SOL.value(), 2);
    assert_eq!(AssetId::from_symbol("BTC"), Ok(AssetId::BTC));
    assert!(AssetId::try_from(3).is_err());
}

#[test]
fn decimals_convert_exactly_and_canonically() {
    let cents = DecimalScale::new(2).unwrap();
    assert_eq!(PriceTicks::parse("123.45", cents).unwrap().value(), 12_345);
    assert_eq!(QtyLots::parse("0.10", cents).unwrap().value(), 10);
    assert_eq!(PriceTicks::new(12_345).unwrap().format(cents), "123.45");
    assert_eq!(QtyLots::new(10).unwrap().format(cents), "0.1");
    assert!(PriceTicks::parse("1.001", cents).is_err());
    assert!(QtyLots::parse("-1", cents).is_err());
    assert!(QtyLots::parse("0", cents).is_err());
    assert!(PriceTicks::parse("1.", cents).is_err());
}

#[test]
fn sim_user_requires_normalized_local_identifier() {
    let raw = format!("0x{}", "a1".repeat(20));
    assert_eq!(SimUserId::parse(&raw).unwrap().as_str(), raw);
    assert!(SimUserId::parse(&raw.to_uppercase()).is_err());
    assert!(SimUserId::parse("0x1234").is_err());
}

#[test]
fn serde_revalidates_numeric_domain_values() {
    let scale: DecimalScale = serde_json::from_str("18").unwrap();
    let price: PriceTicks = serde_json::from_str("12345").unwrap();
    let quantity: QtyLots = serde_json::from_str("10").unwrap();

    assert_eq!(scale.decimal_places(), 18);
    assert_eq!(price.value(), 12_345);
    assert_eq!(quantity.value(), 10);
    assert_eq!(serde_json::to_string(&price).unwrap(), "12345");
    assert_eq!(serde_json::to_string(&quantity).unwrap(), "10");

    assert!(serde_json::from_str::<DecimalScale>("19").is_err());
    assert!(serde_json::from_str::<DecimalScale>("255").is_err());
    assert!(serde_json::from_str::<PriceTicks>("0").is_err());
    assert!(serde_json::from_str::<PriceTicks>("-1").is_err());
    assert!(serde_json::from_str::<QtyLots>("0").is_err());
    assert!(serde_json::from_str::<QtyLots>("-1").is_err());
}

#[test]
fn sim_user_serde_is_a_validated_string() {
    let raw = format!("0x{}", "ab".repeat(20));
    let encoded = serde_json::to_string(&raw).unwrap();
    let decoded: SimUserId = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.as_str(), raw);
    assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);

    for invalid in ["x", "0x1234", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"] {
        let encoded = serde_json::to_string(invalid).unwrap();
        assert!(serde_json::from_str::<SimUserId>(&encoded).is_err());
    }
    assert!(serde_json::from_str::<SimUserId>("1").is_err());
}

#[test]
fn formatter_cannot_receive_an_overscale_value_from_json() {
    assert!(serde_json::from_str::<DecimalScale>("255").is_err());

    let scale: DecimalScale = serde_json::from_str("18").unwrap();
    let price: PriceTicks = serde_json::from_str("1").unwrap();
    let quantity: QtyLots = serde_json::from_str("1").unwrap();
    assert_eq!(price.format(scale), "0.000000000000000001");
    assert_eq!(quantity.format(scale), "0.000000000000000001");
}
