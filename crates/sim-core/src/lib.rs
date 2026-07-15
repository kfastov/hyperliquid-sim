//! Deterministic simulator core.

/// Fixed assets supported by the simulator.
pub const FIXED_ASSET_COUNT: usize = 3;

#[cfg(test)]
mod tests {
    use super::FIXED_ASSET_COUNT;

    #[test]
    fn fixed_asset_universe_has_three_assets() {
        assert_eq!(FIXED_ASSET_COUNT, 3);
    }
}
