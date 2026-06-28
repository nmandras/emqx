# 10 — Reimplementation Roadmap

This is the synthesis: how to build a **smaller MQTT broker in a different technology** from the
EMQX architecture, what to keep, what to drop, in what order, and how to know it works.

## 1. Guiding principles

1. **Per-connection concurrency is the load-bearing decision.** Pick a unit of concurrency that is
   cheap at 10⁵–10⁶ connections (async tasks / green threads / coroutines, or a sharded event-loop
   pool). Everything else follows from this. (See the mapping table in
   [01 §5](./01-overview-and-concepts.md).)
2. **Shared state in concurrent tables, per-connection state in the connection.** Subscriptions,
   routes, registry, caches → sharded concurrent maps. Session, channel FSM, timers → owned by the
   connection task.
3. **Build single-node first; keep two cluster seams clean.** "Forward a publish to node N" and
   "find the node owning client C." If those are the only places that know about the cluster,
   clustering is an additive phase, not a rewrite.
4. **Drop BEAM-only luxuries.** Hot code loading, OTP releases, BPAPI version negotiation, and
   Mnesia/Mria are large complexity sources you do not need for a smaller system. Use rolling
   restarts and an off-the-shelf replicated store instead.
5. **Keep the standards.** Prometheus exposition and OpenTelemetry are language-agnostic; reuse
   them verbatim.

## 2. Phased scope

### Phase 1 — Minimum Viable Broker (single node)
The smallest thing that is a correct, useful MQTT broker.

| Area | Include | Source to mirror |
|------|---------|------------------|
| Transport | TCP + TLS + WebSocket listeners; one task per connection; backpressure | [02 §1](./02-broker-core.md) |
| Protocol | MQTT 5.0 (+ 3.1.1) frame codec; channel FSM; keepalive; caps | [02 §1–2](./02-broker-core.md) |
| Sessions | In-memory sessions; inflight window; pending queue; QoS 0/1/2; clean-start & expiry | [03](./03-sessions-and-clients.md) |
| Pub/Sub | Local subscription maps; **wildcard topic trie**; dispatch to subscribers | [02 §4–5](./02-broker-core.md) |
| Messaging | Retained messages (in-memory); will messages | [02 §7](./02-broker-core.md) |
| Access control | Username/password (built-in/file/HTTP) AuthN; simple ACL AuthZ + per-conn cache | [04](./04-access-control.md) |
| Connection mgmt | Local client registry; local takeover on client-id conflict | [03 §4](./03-sessions-and-clients.md) |
| Config | YAML/TOML + schema validation; layered defaults→file→env; hot-swap snapshot | [06 §3](./06-clustering-and-configuration.md) |
| Ops | Metrics registry; `/metrics` (Prometheus); `GET /clients`,`/subscriptions`,`/nodes`,`POST /publish` | [08](./08-management-and-observability.md) |
| Extensibility | In-process **hook pipeline** (the seam everything else rides on) | [02 §8](./02-broker-core.md) |

### Phase 2 — Data integration & operability (still single node, or static cluster)
| Add | Why | Source |
|-----|-----|--------|
| Rule engine (SQL subset: `SELECT/FROM/WHERE` + `republish`) | The headline data-processing feature | [05 §1](./05-rule-engine-and-integration.md) |
| Resource/connector layer + buffer worker (batch/retry) | Reusable foundation for all integrations | [05 §2](./05-rule-engine-and-integration.md) |
| HTTP (webhook) + MQTT bridges; then one queue (Kafka) and one SQL sink | Cover the common integrations | [05 §3](./05-rule-engine-and-integration.md) |
| Shared subscriptions | Load-balanced consumers | [02 §6](./02-broker-core.md) |
| Full dashboard + OpenAPI + token auth | Operability | [08 §1–2](./08-management-and-observability.md) |
| ExHook-style gRPC extension contract | Polyglot extensibility | [09 §2](./09-extensibility-and-gateways.md) |
| Tracing, alarms, OpenTelemetry spans | Debuggability | [08 §5–6](./08-management-and-observability.md) |

### Phase 3 — Scale & durability (multi-node)
| Add | Why | Source |
|-----|-----|--------|
| Clustering: replicated routes + cluster client registry; RPC mesh (gRPC) + discovery | Horizontal scale & HA | [06 §2](./06-clustering-and-configuration.md) |
| Cluster-wide config propagation (validated transactions) | Consistent operations | [06 §3](./06-clustering-and-configuration.md) |
| Durable storage (embedded log/KV; Raft only if HA needed) | Persistence | [07](./07-durable-storage.md) |
| Durable sessions; message queue | Offline delivery at scale, session migration | [07 §3–4](./07-durable-storage.md) |
| Gateways (only protocols you need; prefer `exproto`-style) | Non-MQTT devices | [09 §3](./09-extensibility-and-gateways.md) |
| Multi-tenancy / mountpoints; node rebalancing | Operations at scale | [02 §7](./02-broker-core.md), README catalog |

## 3. Explicitly drop or defer

These EMQX features add disproportionate complexity for a smaller system:

- **Hot code loading & OTP releases** — use rolling restarts (drain connections, redeploy).
- **BPAPI version negotiation** — only needed for rolling *mixed-version* cluster upgrades; require
  a full-cluster restart for upgrades instead.
- **Mnesia/Mria specifically** — replace with a Raft-backed KV (config/ACL) and
  eventually-consistent replication or a replicated KV (routes/registry).
- **The full 50+ bridge catalog** — implement only the integrations you need; the rest are variations
  on the connector interface.
- **QUIC & DTLS, most gateways, cluster-link, license, AI apps, file transfer, telemetry** — all
  optional; add only on demand.
- **Durable storage / Raft** — ship in-memory sessions only unless durability is a hard requirement.

## 4. Suggested target-agnostic component design

A reimplementation, regardless of language, naturally falls into these modules (names illustrative):

```
transport/        # listeners, TLS, websocket, frame codec
protocol/         # mqtt packet types, channel state machine, capabilities
session/          # Session trait + in-memory impl (+ durable impl later)
broker/           # subscription tables, topic trie, dispatch
router/           # cluster routes (Phase 3); single-node no-op first
registry/         # clientid -> connection (local; cluster in Phase 3)
authn/ authz/     # provider chain, source chain, ACL matcher, cache
hooks/            # ordered middleware pipeline + hook-point catalog
rules/            # SQL parser + evaluator + events (Phase 2)
connectors/       # Connector trait + buffer/retry worker + bridges (Phase 2)
config/           # schema, validation, layered load, hot-swap snapshot
metrics/ tracing/ # registry, Prometheus, OpenTelemetry
api/              # REST endpoints + OpenAPI
cluster/          # discovery, RPC mesh, replicated stores (Phase 3)
storage/          # DurableStore trait + embedded backend (Phase 3)
```

Keep the **`Message`, `Session`, `Connector`, hook-point, and AuthN/AuthZ** interfaces stable from
day one — they are the contracts the rest of the system (and later phases) depend on.

## 5. Conformance & testing strategy

Correctness for a broker is mostly about MQTT semantics and the QoS state machines.

1. **MQTT specification conformance.** Run an existing third-party client/interop suite against
   your broker (e.g. Eclipse Paho interoperability tests / a published MQTT conformance harness).
   Cover: connect flags & CONNACK reason codes, retained delivery on subscribe, will on abnormal
   disconnect, wildcard matching edge cases (`+`, `#`, `$`-topics), topic aliases, and session
   takeover.
2. **QoS edge cases as targeted tests.** Duplicate PUBLISH, out-of-order/duplicate acks, packet-id
   wrap-around, retransmit on reconnect, and QoS-2 "route exactly once." Use EMQX's behaviour
   ([03 §3](./03-sessions-and-clients.md)) as the oracle for ambiguous cases — and, where useful,
   the EMQX source/tests under `apps/emqx/test/` as a reference.
3. **Topic-index unit tests.** Property-test wildcard matching against the spec examples — this is
   the most bug-prone data structure.
4. **Access-control tests.** AuthN chain ordering (ignore/ok/deny/continue), AuthZ source ordering
   and default-deny, ACL placeholder expansion, and cache invalidation on policy change.
5. **Load / fan-out test.** Sustained connections + publish fan-out to validate the concurrency
   model and backpressure at your target scale (the decision from §1). Measure connection memory,
   publish throughput, and tail latency.
6. **Cluster tests (Phase 3).** Route convergence after subscribe/unsubscribe, takeover across
   nodes, and config propagation/consistency under a node restart.

## 6. Where this leaves you

Build Phase 1 and you have a real, correct, single-node MQTT 5.0 broker with auth and basic
operability — a genuine subset of EMQX. Phase 2 makes it a data platform; Phase 3 makes it scale.
At each step the architecture documents (02–09) tell you what EMQX does and the **portable notes**
tell you how to do the equivalent without the BEAM. Start at
[01 — Overview & Concepts](./01-overview-and-concepts.md) if you haven't, and keep the mapping table
in [01 §5](./01-overview-and-concepts.md) next to your keyboard.
