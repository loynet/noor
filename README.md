# noor

Noor is a focused ptchan community notification service. It watches thread
activity and stream liveness, then delivers durable Telegram notifications when
they become worth attention.

It has two watchers:

- `threads` consumes signed events from ptchan-gateway and creates a
  notification when an eligible thread reaches a configured reply threshold.
- `streams` polls configured stream probes and creates a notification for the
  transition from offline to live.

The notifier delivers queued notifications to Telegram. Watching and delivery
are deliberately separate: a watcher decides that something matters; the
notifier makes the announcement reliable.

Noor deliberately does **not**:

- answer messages, call language models, or retain conversations;
- fetch from or post to ptchan directly;
- make notification policy depend on Telegram details; or
- treat an event as delivered merely because it was observed.

ptchan-gateway remains Noor's only ptchan boundary. It supplies signed webhook
events and any sanctioned, sanitized ptchan data Noor needs.

## Design principles

### Useful once, not noisy often

Each watcher should produce a notification only for a meaningful state
transition or threshold crossing. Repeated gateway deliveries, polling cycles,
and process restarts must not create duplicate announcements.

The threads watcher reports activity that ptchan-gateway observed. The gateway
durably retries events it has committed while Noor is offline, but it cannot
replay posts created while the gateway itself was disconnected from ptchan.
Noor is therefore a low-latency notification service, not a complete ptchan
history or reconciliation system.

### Durable before external side effects

Noor records its source decisions and pending deliveries in SQLite before it
calls Telegram. If Telegram is temporarily unavailable, the message remains
pending and is retried with bounded backoff. Telegram has no universal delivery
idempotency key, so an unknown send outcome is retried and duplicate alerts are
possible. Delivery is explicitly at-least-once. Delivery state is separate from
the source state that made an alert eligible.

### Two watchers, one delivery path

Noor runs whichever watchers are configured for the process. They may later be
deployed separately if there is an operational reason, but deployment shape is
not a product abstraction. The watchers share a single durable notification
queue because they have the same real delivery need. Their source-state tables
and retention policies remain separate.

### Safe and observable

Configuration is strict, secrets stay in the environment, and logs go to
stdout. Health, readiness, and Prometheus metrics are process-level features.
Metrics must be low-cardinality and never contain message bodies, URLs,
identifiers, signatures, or secrets.

## Intended behaviour

### Thread notifications

The threads watcher receives at-least-once, signed ptchan-gateway events that
the gateway has accepted and committed. It
tracks each thread, applies its configured filters, and queues a single
announcement after the thread reaches its reply threshold. Keyword deny lists,
an optional maximum thread age, and the threshold are explicit product policy.
Board admission belongs to ptchan-gateway's integration policy.

It counts unique observed reply events rather than reading the thread for every
event. Gateway delivery is best-effort ordered, so a short disruption may make
an announcement late or miss a thread whose activity happened while the gateway
was unable to observe ptchan. A future bounded reconciliation feed would be a
separate feature; a per-thread metadata read alone cannot close that gap.

On a first start, it establishes a bootstrap watermark: old events are tracked
as needed but do not cause a historical notification flood. Receipt records,
thread state, and pending deliveries are retained only as long as needed for
correctness and then pruned.

### Stream notifications

The streams watcher records, but does not announce, each stream's initial
observed state. It then announces once when a stream moves from offline to live.
A short offline debounce
prevents a transient probe failure from resetting that decision; sustained
absence returns the stream to offline and makes a later live transition
eligible for one new announcement.

The probe URL is for machine liveness checks, while the page URL is the link
people receive. Those two purposes stay separate in configuration and code.

## Project shape

The planned source tree keeps ownership obvious:

```text
cmd/noor                 process entrypoint
internal/app             process wiring and operational endpoints
internal/watch/threads   gateway-event observation and thread state
internal/watch/streams   liveness observation and stream state
internal/notify          durable notification queue and delivery scheduling
internal/telegram        Telegram delivery adapter
internal/storage         SQLite setup and schema utilities
```

## Run locally

Requirements: Go 1.25 or newer and a Telegram bot token. The threads watcher
also needs a ptchan-gateway integration secret.

```bash
cp config/example.toml config/noor.toml
export TELEGRAM_BOT_TOKEN=...
export PTCHAN_INTEGRATION_NOOR_SECRET=...
go run ./cmd/noor
```

`CONFIG_FILE` overrides the default `config/noor.toml`. Settings are strict:
unknown TOML keys and invalid enabled-watcher configuration stop Noor before it
starts.

Set either `threads.enabled` or `streams.enabled` to `false` to run only the
other watcher. A streams-only process does not need a gateway secret; a
threads-only process does not need stream channels.

The gateway integration must send signed events to Noor's stable
`/internal/ptchan/events` endpoint.

## Destination and retention

`telegram.notification_chat_id` is Noor's single notification destination.
Every thread and stream announcement goes there; configure a negative ID for a
Telegram channel or supergroup. It is private operational configuration, but
not a secret: the bot token is the credential that authorizes delivery.

`retention.completed_after` controls cleanup of completed notification rows and
inactive thread state/event receipts. Pending notifications are never pruned.
Known permanent Telegram rejections are completed without retrying and are
also retained for this period; alert on the corresponding delivery metric.
Choose a duration comfortably longer than any expected gateway retry window;
the example retains completed state for one week. The same duration is Noor's
maximum accepted gateway-event age: a delivery older than that is acknowledged
and ignored. This makes cleanup safe even though ptchan-gateway can retain a
pending delivery indefinitely, and deliberately limits how long Noor supports
an endpoint outage. Stream state is retained so a restart does not announce an
already-live stream again. Streams continuously offline for the retention
period are removed during Noor's hourly cleanup; a later live transition then
correctly becomes a new notification.

## Docker

Create the development configuration and secret file, then deploy the
development instance:

```bash
cp config/example.toml config/dev.toml
cp .env.example .env.dev
make docker-deploy
```

The container runs non-root with a read-only filesystem. Configuration is
mounted read-only and SQLite lives in the selected named volume
(`noor-dev-data` by default). Use `make docker-logs` for logs and
`make docker-clean` to remove the container and its volume.

`make docker-deploy` defaults to the development instance and builds
`noor:<current-commit>`. It validates configuration before replacing the
container and preserves its SQLite volume. Override `IMAGE` only when using a
registry or a specific image tag.

```text
NOOR_ENV=dev  -> .env.dev,  config/dev.toml,  noor-dev, noor-dev-data
NOOR_ENV=prod -> .env.prod, config/prod.toml, noor,     noor-data
```

Deploy the primary Noor instance with `make docker-deploy NOOR_ENV=prod`.
`ENV_FILE` and `CONFIG_FILE` remain available for an intentionally separate
local setup.

Docker calls `/healthz` through Noor's `check-health` command. `/readyz` and
`/metrics` share `runtime.http_addr`; keep that endpoint on an internal Docker
network or publish it deliberately for Prometheus.

## Metrics

`runtime.http_addr` exposes Prometheus metrics. Noor includes the standard Go
and process collectors, including CPU time, resident memory, heap/GC, and
goroutine metrics. Its application metrics are:

- `noor_gateway_webhook_requests_total{result}`
- `noor_thread_events_total{board,kind,result}`; `board` comes only from the
  gateway integration's bounded board policy and `kind` is `thread.created`,
  `post.created`, or `unknown`.
- `noor_stream_probes_total{result}`
- `noor_notification_enqueues_total{source,result}`
- `noor_notification_delivery_attempts_total{result}`
- `noor_notification_pending`
- `noor_notification_oldest_pending_seconds`

Metrics never include thread/event IDs, chat IDs, text, URLs, or secrets.

Noor's SQLite state is disposable. It is safe to drop the database when an
incompatible schema change is deployed; Noor will rebuild its state from new
gateway events and stream probes.

## License

GNU General Public License, version 3 or later.
