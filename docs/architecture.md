# Hyperliquid Simulator MVP Architecture

## 1. Purpose and boundary

This repository will implement a local, deterministic perpetual-exchange simulator with a deliberately small Hyperliquid-shaped API. It is an integration and agent-workflow experiment, not a financial system, exchange emulator, or complete Hyperliquid implementation.

The MVP supports exactly three perpetual assets and keeps their protocol identifiers stable:

| Asset | ID |
| --- | ---: |
| BTC | 0 |
| ETH | 1 |
| SOL | 2 |

The service uses live Hyperliquid market data only as a read-only oracle. Every account, order, balance, fill, and event exposed by the simulator is local synthetic state. The process must never submit an order or other state-changing request upstream.

## 2. Workspace and module boundaries

The Rust workspace has five logical modules.

### `hl-wire`

Owns the API boundary types and deterministic conversions used by adapters:

- request and response DTOs for the supported `/info`, `/exchange`, and `/ws` messages;
- asset-ID and symbol mapping for BTC, ETH, and SOL;
- parsing of decimal strings into validated integer ticks and lots;
- serialization conventions, tagged unions, and stable error envelopes;
- parsing of the `X-Sim-User` authentication profile.

It does not own exchange state, perform matching, make network calls, or infer identity from a signature.

### `sim-core`

Owns the deterministic domain model and is independent of HTTP, WebSocket, clocks, and upstream clients:

- the three independent order books;
- local accounts, open orders, positions, and synthetic balances;
- command handling for batch order placement and cancellation;
- GTC, IOC, and ALO behavior;
- price-time-priority matching, fills, and ordered domain events;
- snapshots needed by adapters and actors.

All mutation passes through one `Engine::apply(command)`-style boundary. The engine receives explicit logical time and deterministic IDs/random choices; it must not read wall-clock time or entropy directly.

### `oracle-hyperliquid`

Owns read-only upstream ingestion:

- consumes Hyperliquid `activeAssetCtx` updates and reads `oraclePx`;
- falls back to `metaAndAssetCtxs` when the primary stream is unavailable or incomplete;
- maps only BTC, ETH, and SOL and rejects ambiguous or unknown mappings;
- normalizes values through `hl-wire` numeric types;
- publishes timestamped oracle observations and explicit health/staleness state.

This module has no private-key support and no upstream exchange/order method.

### `sim-server`

Owns process orchestration and network adapters:

- an HTTP adapter for `/info` and `/exchange`;
- a WebSocket adapter at `/ws`;
- a single-owner engine runtime and bounded command queues;
- event fan-out, subscriber lifecycle, health, configuration, and graceful shutdown;
- composition of the oracle and seeded synthetic actors.

Adapters translate wire DTOs into core commands and translate snapshots/events back to JSON. They cannot mutate core state directly.

### `acceptance`

Owns black-box fixtures and probes against a running service:

- SDK-shaped HTTP fixtures;
- external `curl` probes;
- WebSocket subscription and event probes;
- stale-oracle and safety-boundary checks;
- packaging/deployment smoke checks.

It must not reach into private core state to make an end-to-end test pass.

## 3. Dependency direction

The intended dependency graph is:

```text
hl-wire <--- sim-core
    ^           ^
    |           |
oracle-hyperliquid
    ^           ^
    +--- sim-server --- acceptance (black-box only)
```

More precisely:

- `hl-wire` is a leaf for shared validated value types and DTOs.
- `sim-core` may depend on `hl-wire`, but neither depends on server or oracle code.
- `oracle-hyperliquid` may depend on `hl-wire`; it cannot depend on `sim-core` internals.
- `sim-server` composes all libraries through public commands, snapshots, and events.
- `acceptance` talks to the public process boundary rather than becoming a runtime dependency.

To permit HTTP, WebSocket, and oracle work to proceed in parallel after task #2, task #2 establishes the workspace, shared command/snapshot/event interfaces, and a compilable `sim-server` module shell. Adapter implementations remain owned by their separate tasks.

## 4. Deterministic data model

### Numeric representation

Floating-point values are forbidden in matching and account state.

- `PriceTicks`: signed integer ticks validated against the asset's positive tick size.
- `QtyLots`: unsigned integer lots validated against the asset's positive lot size.
- Decimal strings at the wire/oracle boundary are converted exactly or rejected; no silent rounding.
- Multiplication used for notional/account reporting uses checked wider integer arithmetic and returns a bounded error on overflow.

Tick and lot scales are explicit per asset configuration. Their concrete MVP values are configuration data and acceptance fixtures, not inferred from a decimal's presentation.

### Stable identities and ordering

- `AssetId` is limited to `0..=2` with the fixed mapping above.
- `SimUserId` is a normalized non-empty local identifier from `X-Sim-User`.
- `OrderId`, `TradeId`, and `EventSequence` are monotonic integers allocated by the engine.
- Resting priority is `(price, accepted_sequence)`: highest bid or lowest ask first, then earliest accepted sequence.
- A batch is evaluated in request order by one engine turn. Every emitted event has one total sequence.

A seeded actor receives an explicit seed and deterministic logical-time inputs. Given the same initial state, commands, oracle observations, clock steps, and seed, command results, snapshots, and event sequences must be byte-for-byte reproducible after canonical serialization.

### Orders

An order contains user, asset, side, price ticks, quantity lots, remaining lots, time-in-force, engine order ID, optional client order ID, and accepted sequence.

- **GTC:** matches immediately, then rests any remainder.
- **IOC:** matches immediately and cancels any remainder without resting.
- **ALO:** is rejected atomically if it would cross the current opposite best price; otherwise it rests.
- A zero quantity, invalid tick/lot value, unsupported asset, overflow, or missing user is rejected before mutation.
- Batch entries return per-entry statuses. A rejected entry does not roll back successful earlier entries in the same batch.

Cancellation is scoped to the authenticated local user. A successful cancellation removes only the remaining quantity and emits an ordered update. Unknown, already-terminal, wrong-user, or wrong-asset cancellation returns a stable error and does not mutate state.

### Books, fills, and accounts

Each asset has an independent book and sequence-consistent snapshot. A command for one asset cannot change another asset's book.

A crossing incoming order consumes resting orders at each resting order's price. Each fill updates both local orders and both local accounts before its events are published. The MVP tracks synthetic position quantity and enough synthetic balance/notional data to serve the scoped account response; it does not model collateral transfer, leverage, margin, liquidation, funding, fees, or settlement.

## 5. Runtime and concurrency model

One async task exclusively owns `sim-core`. HTTP handlers, WebSocket handlers, the oracle, and synthetic actors send typed commands over bounded channels. A command reply is a one-shot response correlated with the command. No adapter holds a mutable engine lock.

After a committed engine transition, the owner publishes immutable sequenced events. Fan-out must not block the owner:

- subscribers have bounded buffers;
- a lagging subscriber is reported and disconnected or required to resubscribe for a fresh snapshot;
- initial subscription snapshot and subsequent events share a sequence boundary so clients can detect gaps;
- cancellation and shutdown are explicit commands.

Wall-clock time is read only in the runtime and converted into explicit timestamps/clock-step commands. Core tests use a fake clock.

## 6. Oracle and actor behavior

The source priority is:

1. live Hyperliquid `activeAssetCtx` updates using `oraclePx`;
2. read-only `metaAndAssetCtxs` fallback;
3. the last valid observation, marked stale after 60 seconds.

Malformed, non-positive, unmapped, or out-of-order observations do not replace the last valid value. Reconnect uses bounded exponential backoff with jitter sourced outside the deterministic engine. Upstream failure must not crash the process or mix asset values.

When any relevant oracle price is older than 60 seconds:

- new user and actor order placement for that asset is rejected with an observable `oracle_stale` error;
- cancellation remains available;
- existing state remains queryable;
- health reports the affected asset and observation age.

Synthetic liquidity/trade actors are local only. Their decisions are deterministic for a configured seed and logical-time/oracle input stream. They submit the same typed core commands as users and receive no privileged mutation path.

## 7. Adapter responsibilities

### HTTP

`POST /info` parses one supported info request and serves a consistent snapshot. `POST /exchange` parses a supported batch order/cancel action, requires the simulator auth profile, submits one typed command, and returns per-entry results. Invalid JSON, unsupported variants, overload, stale oracle, and domain rejection use stable, testable errors.

### WebSocket

`GET /ws` supports subscribe, unsubscribe, and ping plus the channels `allMids`, `l2Book`, `trades`, and `orderUpdates`. Public topics do not require a user. `orderUpdates` requires `X-Sim-User` at the upgrade and filters events to that exact local user. The MVP does not support authenticated identity derivation from payload signatures.

## 8. Failure model and observability

Errors are classified at boundaries:

- `invalid_request`: malformed or syntactically invalid input;
- `unsupported`: valid Hyperliquid behavior outside the MVP subset;
- `unauthorized_sim_user`: absent/invalid simulator identity where required;
- `domain_reject`: valid request rejected by order/account rules;
- `oracle_stale`: placement disabled for an asset;
- `overloaded`: bounded runtime queue cannot accept work;
- `internal`: unexpected failure, without leaking secrets or internals.

Health distinguishes process liveness from readiness. Readiness includes engine availability and per-asset oracle freshness. Logs include request/command correlation, asset, local user where safe, and event sequence; they never log signatures as credentials or claim that a signature authenticated a user.

## 9. Safety and explicit non-goals

The following are hard safety boundaries:

- no real funds, custody, deposits, withdrawals, or wallet integration;
- no private keys or signature recovery/verification;
- signature-shaped fields are checked only for documented syntax and are never trusted for identity;
- no upstream order placement, cancel, transfer, or other trading endpoint;
- no spot markets;
- no leverage, margin, liquidation, funding, fees, or production risk controls;
- no claim of economic realism, chain compatibility, or full SDK drop-in compatibility.

An upstream client must expose only the read operations required by the oracle. Tests must fail if a state-changing upstream method is introduced or called. Public deployment uses synthetic accounts and rate/body/connection limits and must be clearly labeled as a simulator.

## 10. Delivery decomposition

- **#2 Core:** workspace, `hl-wire`, `sim-core`, deterministic engine, and shared adapter/runtime interfaces.
- **#3 HTTP:** HTTP files and fixtures only.
- **#4 WebSocket:** WebSocket files and fixtures only.
- **#5 Market runtime:** oracle client, runtime owner loop, and seeded actors.
- **#6 Integration:** composition, executable, acceptance probes, packaging, deployment, and operator/user docs.
- **#7 Protocol:** evidence-led GPP hardening after the experiment.

The contracts intentionally isolate adapter directories. Shared manifests and module shells are established by #2; later manifest changes are allowed only where integration makes them unavoidable and must not be used to broaden product scope.
