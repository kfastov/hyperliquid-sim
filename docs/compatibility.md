# Hyperliquid Compatibility Profile: `sim-header-v1`

## Status

This document defines the complete MVP compatibility claim. “Compatible” means that the listed request shape, response category, and observable behavior are intentionally Hyperliquid-shaped for local testing. It does **not** mean wire-for-wire coverage of every field, cryptographic compatibility, economic equivalence, or suitability for real trading.

The profile name is `sim-header-v1`.

## Assets and identity

Only perpetual BTC, ETH, and SOL are supported, with fixed IDs BTC=0, ETH=1, and SOL=2. Asset names and IDs outside this table return `unsupported`.

User-scoped operations require an HTTP/WebSocket upgrade header:

```text
X-Sim-User: <local-user-id>
```

The value must be a bounded, normalized, non-empty simulator identifier. It selects local synthetic state. It is not an address and is not derived from a signature.

Hyperliquid signature-shaped fields may be accepted when they have the documented JSON and string shape. They are syntactic compatibility fields only: the simulator does not recover a signer, verify authority, or trust them for routing. `X-Sim-User` is authoritative for this profile.

## HTTP transport

All scoped HTTP methods use JSON. Unknown request types, unsupported fields that change semantics, spot asset encodings, and out-of-profile actions return an explicit `unsupported` or `invalid_request` response rather than being silently ignored.

### `POST /info`

| Request `type` | MVP behavior | User header |
| --- | --- | --- |
| `meta` | Metadata for exactly BTC, ETH, and SOL, in fixed ID order | No |
| `metaAndAssetCtxs` | The same metadata plus current per-asset oracle context and freshness | No |
| `allMids` | Current mids/oracle-derived price strings for the three assets | No |
| `l2Book` | One simulated book snapshot for a supported coin/asset | No |
| `openOrders` | Open local orders for `X-Sim-User`; any payload user/address is not trusted | Yes |
| `clearinghouseState` | Synthetic local positions/balance summary for `X-Sim-User` | Yes |

`l2Book` exposes only simulated resting liquidity. `allMids` and asset contexts expose the current accepted oracle values; they are not evidence of executable upstream prices. Snapshot numeric values are serialized as canonical decimal strings derived from integer ticks/lots.

The MVP account response contains only fields required to represent local synthetic orders, positions, and balances. Margin summaries, leverage, liquidation prices, funding, withdrawable production collateral, and chain state are unsupported.

### `POST /exchange`

Supported action categories:

- a batch `order` action containing only BTC/ETH/SOL perpetual limit orders;
- a batch `cancel` action scoped to the header-selected local user.

Supported time-in-force values:

- `Gtc` / GTC: cross then rest the remainder;
- `Ioc` / IOC: cross then cancel the remainder;
- `Alo` / ALO: reject if immediately marketable, otherwise rest.

A request batch is bounded by configuration and processed in array order. The response has one ordered status per input entry. Partial success is allowed: one rejected entry does not roll back preceding accepted entries. Stable error categories distinguish malformed input, unsupported behavior, local authorization failure, stale oracle, overload, and domain rejection.

New order placement is rejected for an asset whose oracle observation is older than 60 seconds. Cancel remains accepted during oracle staleness. No `/exchange` request is forwarded upstream.

Unsupported exchange behavior includes market orders, trigger orders, modify, cancel-by-client-ID unless later explicitly added to this profile, leverage updates, transfers, withdrawals, vault actions, agent approvals, spot actions, and every other Hyperliquid action not named above.

## WebSocket transport: `GET /ws`

Client control messages:

- `subscribe` to one supported subscription;
- `unsubscribe` from a prior supported subscription;
- `ping`, answered with `pong` without touching engine state.

Supported subscription types:

| Subscription | Initial data | Updates | User header |
| --- | --- | --- | --- |
| `allMids` | Current three-asset values | Accepted oracle/mid changes | No |
| `l2Book` | Current snapshot for one supported asset | Sequenced simulated book changes | No |
| `trades` | No historical replay in MVP | New simulated fills for one supported asset | No |
| `orderUpdates` | Current open-order snapshot or explicit subscribed acknowledgement | New updates for exactly `X-Sim-User` | Yes |

Subscribe and unsubscribe produce explicit acknowledgements. Events expose sequence information sufficient to detect a gap. A slow subscriber is never allowed to block matching; it is disconnected or receives an observable lag condition and must resubscribe.

WebSocket post requests and all subscription types not listed above are unsupported in the MVP. In particular, the service does not claim support for user fills/history, candles, funding, notifications, ledger updates, TWAP, spot, or validator topics.

## Numeric and ordering compatibility

Input prices and sizes are decimal strings that must convert exactly to configured integer ticks/lots. Invalid precision is rejected; the simulator does not silently round. Output values use deterministic canonical decimal strings.

Matching is deterministic price-time priority. Batches are evaluated in request order. Event/order/trade IDs are local monotonic identifiers and are not Hyperliquid chain or exchange IDs.

## Oracle behavior

The read-only oracle consumes live Hyperliquid `activeAssetCtx.oraclePx`. If that source is unavailable or incomplete, it reads `metaAndAssetCtxs`. The last valid observation may remain visible with explicit freshness, but after 60 seconds it is stale and disables new placement for the affected asset.

The oracle never uses private APIs and never sends orders, cancellations, transfers, or wallet actions upstream.

## Explicitly unsupported system behavior

- real funds, custody, wallets, deposits, withdrawals, or production accounts;
- signature recovery, cryptographic authorization, or address derivation;
- spot markets;
- leverage, margin enforcement, liquidation, funding, fees, settlement, or insurance;
- exact Hyperliquid latency, matching internals, risk controls, IDs, history, or economic behavior;
- durability across restart unless a later profile explicitly adds it;
- horizontal/multi-owner matching;
- any endpoint, action, field semantics, or subscription not explicitly listed above.

Clients must opt into `sim-header-v1`, provide `X-Sim-User` for user state, and treat all values as synthetic. A compatibility mismatch must fail observably rather than falling through to a plausible but incorrect production interpretation.
