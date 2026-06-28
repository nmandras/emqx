# 08 — Management & Observability

How operators control and observe the platform: a **REST API** and **dashboard**, a **CLI**, and
**metrics / traces / alarms / audit**. A small management API is **MVP** (you need to inspect
clients and publish for testing); the full dashboard, RBAC/SSO, and exporters are **Phase 2**.
The observability *standards* (Prometheus, OpenTelemetry) are language-agnostic — **keep them as
is**.

## 1. Management REST API

**Responsibility.** Expose cluster state and control over HTTP: list/inspect/kick clients, manage
subscriptions and topics, read node/cluster status, manage listeners/auth/rules/bridges/plugins,
publish messages, run traces, and back up/restore config.

**Key source.** `apps/emqx_management/`:
- `emqx_mgmt_api.erl` — shared helpers: **pagination** (`paginate/3`) and **cluster query**
  (`cluster_query/…`) that fan a query out to all nodes and merge results (using ETS match specs /
  `qlc`). This is the pattern most endpoints use to present cluster-wide views.
- `emqx_mgmt_api_*.erl` — the endpoints: `clients`, `subscriptions`, `topics`, `nodes`, `cluster`,
  `listeners`, `configs`, `alarms`, `plugins`, `trace`, `publish`, `stats`, `data_backup`, etc.
- `emqx_mgmt.erl` — orchestrates the management operations the endpoints call.

The HTTP server is **Cowboy** + **Minirest** (EMQX's thin routing/OpenAPI layer). Endpoints are
versioned under `/api/v5/...`. API auth is by **API key** or dashboard JWT.

**Portable notes.** Use any HTTP framework with OpenAPI support. Implement a handful of endpoints
first — `GET /clients`, `GET /subscriptions`, `GET /nodes`, `POST /publish` — since they make the
broker testable and operable. The **cluster-query** pattern (fan-out + merge + paginate) is worth
copying for any "list X across the cluster" endpoint.

## 2. Dashboard, OpenAPI, API auth, RBAC/SSO

**Key source.** `apps/emqx_dashboard/`:
- `emqx_dashboard_swagger.erl` — generates **OpenAPI/Swagger** from endpoint schema definitions and
  serves the interactive docs. Endpoint request/response schemas double as validation and
  documentation.
- `emqx_dashboard_token.erl` — issues/validates **JWT** for dashboard/API sessions.
- `apps/emqx_dashboard_rbac/` — role-based access control for dashboard users (**Opt**).
- `apps/emqx_dashboard_sso/` — SSO (LDAP/SAML/OIDC) for dashboard login (**Opt**).

The dashboard **front-end** (a separate single-page app) is served as static assets and talks to
the same REST API.

**Portable notes.** Derive OpenAPI from your endpoint schemas (most frameworks do this). Protect
the API with token auth from the start; add RBAC/SSO only if you have multiple operator roles. The
SPA can be reused or rebuilt independently — it is just an API client.

## 3. CLI

**Key source.** `apps/emqx_ctl/` provides a command-registration framework; subsystems register
commands (`emqx_ctl:register_command/…`) and `apps/emqx_conf/src/emqx_conf_cli.erl` (and others)
implement them. The `emqx ctl <command>` binary dispatches to the running node.

**Portable notes.** A simple command registry (`name → handler`) invoked by an admin binary that
RPCs into the running node (or hits the local REST API). Low priority for MVP.

## 4. Metrics & stats

**Key source.**
- `apps/emqx/src/emqx_metrics.erl` — a registry of named counters/gauges (messages received/sent/
  dropped, bytes, packets by type, auth outcomes, …), global and per-namespace.
- `apps/emqx/src/emqx_stats.erl` — periodically-sampled gauges (current connections, subscription
  counts, topic counts, …).

**Portable notes.** Keep a flat namespaced metric registry (`messages.publish`, `connections.count`,
`delivery.dropped`, …) backed by atomic counters; sample gauges on a timer. Increment on the hot
path with cheap atomics, not locks.

## 5. Exporters: Prometheus & OpenTelemetry (keep the standards)

**Key source.** `apps/emqx_prometheus/` implements a Prometheus collector and a
`/api/v5/prometheus/stats` endpoint, aggregating metrics across nodes via RPC.
`apps/emqx_opentelemetry/` exports **traces** (spans for connect/publish/subscribe/…) and metrics
in OTel format; `apps/emqx_telemetry/` is anonymous usage telemetry (drop it).

**Portable notes.** Prometheus exposition format and OpenTelemetry are cross-language standards
with libraries everywhere — expose `/metrics` in Prometheus format and emit OTel spans on the key
operations. This is the cheapest, highest-leverage observability to keep identical to EMQX.

## 6. Alarms, tracing, audit

- **Alarms** — `apps/emqx/src/emqx_alarm.erl`: activate/clear named alarms (high memory/CPU,
  process/connection limits, partition healed/lost) into active + history tables surfaced on the
  dashboard. **Opt** but cheap.
- **Tracing** — `apps/emqx/src/emqx_trace/` (`emqx_trace.erl`, `_handler.erl`, `_formatter.erl`):
  operator-defined traces filtered by client id / topic, hooked into the lifecycle, written to
  per-trace log files downloadable via the API. Invaluable for debugging a specific device.
  **Opt.**
- **Audit** — `apps/emqx_audit/`: records administrative actions (config changes, user/plugin
  operations) for compliance. **Opt.**

**Portable notes.** Structured logging plus a per-client/per-topic filter gets you most of tracing.
Alarms = a small "active conditions" table fed by your OS/VM monitors. Audit = an append log of
admin mutations. All optional; add when you have users asking for them.

---

**Next:** extending the platform and supporting other protocols →
[09 — Extensibility & Gateways](./09-extensibility-and-gateways.md).
