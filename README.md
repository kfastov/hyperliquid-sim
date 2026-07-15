# Hyperliquid Simulator

A public experiment in two things:

1. a small Hyperliquid-compatible perpetual exchange simulator; and
2. a runtime-neutral GitHub Project Protocol for handing work between coding agents.

The simulator will expose Hyperliquid-shaped `POST /info`, `POST /exchange`, and `/ws` endpoints for BTC, ETH, and SOL. Prices are anchored to the live Hyperliquid mids while all orders, fills, positions, and balances remain local simulation state.

## Status

Phase 0 protocol bootstrap. Product implementation must arrive through linked GitHub Issues, versioned task contracts, Draft PR checkpoints, checks, and merge.

## Protocol

Read `AGENTS.md` first. The normative binding is under `protocol/v0.1/` and the helper is `tools/gpp.py`.

## Safety

This is a simulator. It never submits orders to Hyperliquid and must not hold real private keys or funds.
