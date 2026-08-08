# AGENTS.md

Noor is a small, reliable notification service for the ptchan community. It
has two watchers and one notification delivery path:

- The **threads watcher** turns signed ptchan-gateway events into a notification
  request when a thread becomes active enough to matter.
- The **streams watcher** detects when a configured broadcast has gone live and
  creates a notification request for that transition.
- The **notifier** delivers those durable requests to configured destinations.

Noor is deliberately not a general community bot, an assistant, a scraper, or
a ptchan client. Keep the boundary sharp. New ptchan behaviour belongs behind
ptchan-gateway; Noor must not fetch from or post to ptchan directly.

## Philosophy

- Reliable delivery matters more than cleverness. Record a notification before
  attempting delivery, and make a restart or repeated source event safe.
- A notification is a promise to humans. Prefer one useful, timely alert over
  noisy, duplicate, or speculative alerts.
- Each watcher owns its observation policy and source state. Notification
  delivery is shared because both watchers have a real common consumer.
  Everything else should remain boring: configuration, process lifecycle,
  storage setup, transports, health, and metrics.
- Source truth and delivery truth are different. Keep enough source state to
  decide whether an alert is warranted, and enough delivery state to retry an
  alert without rediscovering or duplicating it.
- Operations are part of the product. Startup, readiness, logs, metrics,
  backoff, retention, and configuration errors should be unsurprising.
- Treat incoming payloads and external URLs as untrusted. Never put secrets,
  raw signed payloads, or high-cardinality/private identifiers into logs or
  metrics.

## Go Style

This repository should stay small, direct, and idiomatic. Follow
[Effective Go](https://go.dev/doc/effective_go) as the default style guide.

- Prefer concrete types, plain functions, explicit data flow, and early
  returns.
- Do not add interfaces, constructors, factories, managers, or generic
  frameworks until real consumers require them.
- Keep orchestration visible from top to bottom. A dependency-holding type is
  fine when it makes a notifier clearer; give it a domain name, not `Service`.
- Keep comments for invariants and non-obvious decisions, not for narration.
- Keep structs to fields Noor actually uses and translate external payloads at
  the boundary that owns the workflow.
- Do not write user-specific absolute paths into repository files or docs.

## Boundaries

- `cmd/noor` is the process entrypoint. Noor runs the watchers configured for
  that process; do not invent command roles merely to mirror Martie.
- `internal/app` owns strict config loading and validation, process wiring,
  health/readiness, metrics registration, and webhook server plumbing. It does
  not own watcher policy.
- `internal/watch/threads` owns ptchan thread eligibility, gateway event
  idempotency, thread tracking, and source-state retention.
- `internal/watch/streams` owns liveness transition policy, stream probing, and
  source-state retention. Its probe implementation stays private unless a real
  second consumer appears.
- `internal/notify` owns the durable notification queue, delivery scheduling,
  retry policy, and destination-agnostic notification content. It is shared by
  the two real watcher consumers, not a speculative event framework.
- Watcher state and notification-delivery state use separate tables and
  retention policies, even when they share one SQLite file.
- `internal/storage` owns SQLite connection setup and low-level schema helpers
  only; it must not become a domain layer.
- The official `github.com/loynet/ptchan-gateway/clients/go` SDK owns the
  signed webhook DTOs and protocol. Noor uses ptchan-gateway rather than
  ptchan directly and must not duplicate that contract locally.
- `internal/telegram` turns a notification into a Telegram request. Telegram
  rendering and Bot API details do not belong in either watcher.
- Delivery transports must not decide watcher eligibility. If another
  destination is added, adapt a queued notification without mixing transport
  concerns into watcher code.

## Notification Rules

- Treat source event delivery as at-least-once. Durable event receipts and
  notification records must make repeated events harmless.
- ptchan-gateway's at-least-once guarantee begins only after it has accepted
  and committed an upstream event. Noor cannot recover activity the gateway
  never ingested (for example, during a gateway or upstream-socket outage).
  Describe Noor as reporting gateway-observed activity, not as a complete
  ptchan history or reconciliation system.
- Do not add a thread read merely to reconstruct webhook order. Event receipts
  and local observed-thread state are Noor's normal path. A future bounded
  reconciliation feed is a separate capability, not a disguised webhook retry.
- Use a durable outbox (or equivalently durable pending-delivery record) before
  external delivery. Retry transient delivery failures with bounded backoff.
- Do not mark an alert delivered until the delivery transport has accepted it.
  Telegram has no universal delivery idempotency key, so unknown outcomes are
  retried and delivery is explicitly at-least-once: duplicates are possible.
- Keep watcher policy explicit. Thread thresholds,
  allow/deny rules, age limits, stream liveness criteria, and offline debounce
  are product rules—not generic infrastructure knobs.
- Define restart semantics deliberately. In particular, a fresh threads
  watcher must not announce historical events merely because it has no state;
  a streams watcher must not repeatedly announce an already-live stream.
- Retention is required for durable state. Pruning must preserve every record
  still needed for idempotency or pending delivery.

## Configuration and Operations

- TOML holds non-secret application settings. Environment variables hold
  secrets and deployment paths.
- Decode TOML strictly: unknown fields, duplicate keys, malformed values, and
  invalid enabled-watcher settings fail at startup with a clear error.
- Validate only enabled watchers' external dependencies. A streams-only
  deployment should not require a gateway secret, and a threads-only deployment
  should not require stream configuration.
- ptchan-gateway secrets use the integration-specific environment variable
  `PTCHAN_INTEGRATION_<INTEGRATION_NAME>_SECRET`.
- Logs go to stdout. Do not add application-managed log files.
- Metrics are public operational contracts: use low-cardinality labels and keep
  their documented meaning stable. Never use message text, URLs, event IDs,
  thread/post IDs, chat IDs, signatures, or secrets as labels.
- A lightweight thread-meta endpoint would not currently make a thread lookup
  cheap: the gateway still fetches and parses the full upstream thread. Do not
  make Noor depend on one unless the gateway exposes a genuinely cheaper
  upstream summary source or it enables a specific measured workflow.
- Routine watcher failures must be logged and retried without taking down an
  unrelated watcher. Shared infrastructure failures (SQLite, the notifier, or
  either HTTP listener) stop the process for external restart.

## Development

- Prefer the repository's `make` targets for normal workflows.
- Run `gofmt` on changed Go files.
- Validate relevant changes with `go test ./...` and `go vet ./...`.
- If the environment blocks the default Go build cache, use the ignored
  repo-local cache with `GOCACHE="$PWD/.gocache"`.
- Prefer tests for state transitions, idempotency, persistence, retry policy,
  formatting edge cases, and external protocol handling. Do not add tests for
  trivial wiring that is clearer by inspection.
