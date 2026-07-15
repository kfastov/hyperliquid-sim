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
}

#[test]
fn sim_user_requires_normalized_local_identifier() {
    let raw = format!("0x{}", "a1".repeat(20));
    assert_eq!(SimUserId::parse(&raw).unwrap().as_str(), raw);
    assert!(SimUserId::parse(&raw.to_uppercase()).is_err());
    assert!(SimUserId::parse("0x1234").is_err());
}
