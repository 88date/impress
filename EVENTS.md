# Application events

Events use PostgreSQL outbox → Debezium → NATS JetStream → subscribers.
Impress writes outbox records, manages durable consumers and consumes messages.
The application owns its database migrations, Debezium deployment and event
stream configuration.
Tasks and cron also use NATS JetStream, in a separate stream; see [TASKS.md](TASKS.md).

## Declaring and publishing

Event names follow `service:version:entity:action`, for example
`example:1:message:created`. Use letters, digits, `_` and `-` inside name
parts, with a numeric version and `:` as the separator. Subscriber names
follow the same convention. Correct names are the developer's responsibility;
Impress does not validate the name format or escape special characters.

`application/events/chat.1/messageCreated.js`:

```js
({
  caption: 'Message created',
  examples: [{ messageId: 42 }],
});
```

The path becomes the event name `chat:1:message:created` and subject
`events.chat.1.message.created`. A directory without a version defaults to
version 1. All events use NATS JetStream; declarations do not select a transport.

```js
const id = await events.emit('chat:1:message:created', { messageId: 42 });
```

Publication requires a declared event and an outbox configured through
`events.configure()`. It always writes to the outbox and returns the generated
ID after the INSERT succeeds. There is no direct NATS or pg-boss publication.
The publisher does not need an active connection to either broker.

## PostgreSQL outbox

Configure the outbox after initializing the application's database client.
For example, in `application/db/pg/start.js`, after assigning `db.pg.query`:

```js
events.configure({
  database: {
    query: (text, values) => db.pg.query({ text, values }),
  },
  schema: 'public',
  table: 'outbox',
});
```

`database.query(text, values)` must return a promise or thenable that settles
when the query finishes. Its result is not used. The application owns the
client and its lifecycle. `schema` and `table` default to `public` and
`outbox`; both are quoted SQL identifiers, preserving camelCase names.
Query errors reject publication.

The application creates the table through its own migrations. Impress does
not create it at runtime. All columns are NOT NULL; `id` is the primary key.

- `id` (uuid): generated event UUID.
- `name` (text): event name, e.g. `chat:1:message:created`.
- `subject` (text): subject, e.g. `events.chat.1.message.created`.
- `createdAt` (timestamptz): event creation time.
- `payload` (jsonb): user data without another metadata envelope.

Payloads use `JSON.stringify` serialization: objects, arrays, strings,
numbers, booleans and `null` are supported. JSON `null` is stored as a JSON
value, not SQL NULL. Top-level `undefined`, BigInt and circular references
reject publication. Normal JSON serialization rules apply to nested values.

### Transactions

Pass an adapter for an existing transaction to override the configured client:

```js
await events.emit('chat:1:message:created', data, { transaction });
```

The transaction must expose `query(text, values)` and execute the INSERT on
the connection used for the business changes. Impress does not begin, commit
or roll back transactions. With a supplied transaction, the returned ID does
not mean that the transaction has committed.

Without an explicit transaction, the configured adapter executes the INSERT,
normally using autocommit. An adapter backed by `AsyncLocalStorage`, such as
the application's `db.pg`, can select the current transaction implicitly.
Prisma transactions need an adapter backed by their own transaction client.

## Outbox message contract

Configure the CDC pipeline to publish committed outbox inserts to the NATS
subject stored in `subject`, using JetStream publication with a storage ACK.
Only advance the CDC offset after that acknowledgement. Configure timestamp
and JSON conversion so the message body has this shape:

```json
{
  "id": "4e301daf-962c-4d62-aa47-38888c924870",
  "name": "chat:1:message:created",
  "subject": "events.chat.1.message.created",
  "createdAt": "2026-10-31T12:00:00.000Z",
  "payload": { "messageId": 42 }
}
```

For outbox events, `subject` in the body is optional; the other fields are
required by this message contract. `createdAt` must be a date string and
`payload` must be the decoded JSON value. Forwarding only the payload loses
the event metadata. Delete events and tombstones from outbox cleanup must not
be forwarded.

Subscribers receive the complete decoded JSON without extracting `payload`
or unwrapping Debezium envelopes. Other subjects can carry a different JSON
structure, including CDC messages with `before`, `after`, `op` and `source`.
Arrays, scalar values and JSON `null` are also passed through unchanged.

Debezium connectors, transformations and outbox cleanup
are application infrastructure and are not configured by Impress.

## JetStream configuration

Each subscriber selects an existing stream through its `stream` field.
Provision streams outside Impress, for example in the Debezium deployment.
For outbox events, an `EVENTS` stream can capture `events.>`. Configure storage,
retention, limits and replicas in that deployment. Limits retention allows
independent subscribers to read the same events and durable consumers to
resume after downtime. JetStream must be enabled for the NATS account.

Impress only checks whether the stream exists; it never creates or updates
it. `nats.jetstream.replicas` remains a default for new task streams and has
no effect on events. See [TASKS.md](TASKS.md).

The application configures its connection and consumer namespace:

```js
({
  nats: {
    enabled: true,
    servers: ['nats://127.0.0.1:4222'],
    credentials: 'application.creds',
    discovery: { maxWait: 1000 },
    jetstream: {
      consumerPrefix: 'impress',
    },
  },
  events: true,
});
```

These fields belong to `server` configuration. Set `server.events: true` on
instances that create missing subscriber consumers and apply explicitly set
`maxAckPending`, independently of their discovery role. Existing consumers keep
their other settings and saved position; Impress does not recreate them.
Replicas use the same stream and consumer names. Workers with
`server.events: false` only attach to existing consumers. If the stream or a
consumer is missing, or an explicit `maxAckPending` differs from the consumer's
setting, they retry once per second in the background. Managing instances also
wait for the stream to be provisioned. Without subscribers, no stream lookup
is made.

`nats.servers` is an array of addresses, including when there is only one
server. For a comma-separated environment variable, configure it as
`process.env.NATS_SERVERS?.split(',').map((server) => server.trim())`.

The `server.events` flag controls consumer creation and updates to an explicit
`maxAckPending`. With `false`, server workers still consume using existing
consumers once their configuration matches.
Outbox publication and catalog discovery continue independently of this flag.

NATS permissions must allow reading stream and consumer information, pulling
messages and acknowledging them. Managing instances also need permission to
create and update consumers. Permission, creation and update errors are
propagated; they are not treated as missing resources.

Only server workers consume events. Regular workers and balancers can publish
through the outbox. NATS remains the transport for RPC and event discovery.

The subscriber's `deliverPolicy` selects where a new consumer starts reading:

| Mode                | Starting point                                                 |
| ------------------- | -------------------------------------------------------------- |
| `all`               | All retained matching messages.                                |
| `new`               | Messages published after the consumer is created; the default. |
| `last`              | The last retained matching message.                            |
| `last_per_subject`  | The last retained message for each matching subject.           |
| `by_start_sequence` | Stream sequence `startSequence`, inclusive.                    |
| `by_start_time`     | Stream timestamp `startTime`, inclusive.                       |

All modes continue with subsequent messages. `startSequence` is a stream
sequence number, not an outbox event ID. `startTime` is an RFC 3339 string,
for example `'2026-10-31T00:00:00Z'`, referring to when NATS stored the message,
not the outbox `createdAt`. These options can be set in subscriber declarations
for new consumers or in the infrastructure when provisioning consumers.
Only the start option required by the selected policy is sent to NATS.

The policy and starting point apply only when creating a consumer. Restarts
resume its saved position, including with `new`: retained events published
during downtime are still delivered. Initial delivery options in a declaration
do not override an existing consumer. To choose a new starting point, create
a separately named subscriber or explicitly retire the existing consumer
in the infrastructure before recreating it.

Existing consumers must use pull delivery, explicit acknowledgments and the
subscriber's exact subject filter. An explicitly set `maxAckPending` is applied
to the consumer's `max_ack_pending` when Impress connects. Without it, the
existing value is preserved. Other settings, including `max_deliver`,
`ack_wait`, `backoff` and replay policy, are managed in NATS and preserved.
For one unacknowledged message at a time across all workers of a subscriber,
set `maxAckPending: 1` in its declaration. Use `max_deliver: -1` in NATS for
unlimited delivery attempts. No `sequential` flag is needed.

## Subscribers

`application/subscribers/feed.1/messageCreated.js`:

```js
({
  stream: 'EVENTS',
  subject: 'events.chat.1.message.created',
  deliverPolicy: 'new',
  maxMessages: 5,
  maxAckPending: -1,
  retryLimit: 5,
  retryDelay: 1000,
  timeout: 30000,
  method: async (message, context) => {
    await domain.feed.append(message.payload, message.id);
  },
});
```

`stream` and `subject` are required. The subject is passed directly to NATS
as the consumer filter and may include `*` or `>`. Each subscriber can select
a different stream. A missing stream leaves its subscribers waiting while
subscribers of available streams continue connecting.

Here `deliverPolicy: 'new'` selects the mode when creating the consumer.
Without it, a new consumer uses `new`. The same declaration can include
`startSequence` or `startTime` for the corresponding mode. For example, set
`deliverPolicy: 'by_start_time'` and `startTime: '2026-10-31T00:00:00Z'`.

The subscriber name is `feed:1:message:created`. The durable consumer name
joins `consumerPrefix`, `subscriberName` and `subject` with `|`. Subject
dots become colons in this name; the actual NATS subject is unchanged:

```text
impress|feed:1:message:created|events:chat:1:message:created
```

Subject tokens are URI-encoded, including `*`, before joining them with `:`.
Existing `events.chat.1.message.created` filters keep their previous durable
names. `consumerPrefix` uses letters, digits, `_` or `-`; the default is
`impress`. Prefixes and subscriber names must not contain the `|` delimiter.
No instance or worker ID enters the name. Copies of the same subscriber share
work; different names receive independent copies.
Applications that need independent copies for identical subscriber names must
use different consumer prefixes. Replicas of one application use the same
prefix and stream.

Subscribers use `method(message, context)`: the first argument is the entire
decoded JSON message, and the second is `{ signal }` for cancellation. Outbox
handlers read `message.payload`, `message.id`, `message.name` and
`message.createdAt`; CDC handlers read their own envelope fields. Subscribers
can consume messages without a local event declaration.

`SubscriberDeclaration<Message>` describes the complete message type, not
just its payload. Its default is `unknown` because a subject can carry any
JSON value.

- `maxMessages`: JetStream receive buffer size, default 5.
- `maxAckPending`: maximum unacknowledged messages across all workers sharing
  the consumer. Use a positive integer for a limit, or `-1` for no limit.
  An explicit value applies to both new and existing consumers. If omitted,
  new consumers use `-1` and existing consumers keep their setting.
- `retryLimit`: redeliveries configured when creating a consumer, default 2.
- `retryDelay`: delay after a failed handler in milliseconds, default 0.
- `timeout`: handler timeout in milliseconds, default 30000.

Each server worker opens one `consumer.consume()` iterator per subscriber,
passing `maxMessages` as the SDK's `max_messages` option. Each received message
starts its handler immediately, without waiting for previous handlers or a
whole batch. `maxMessages` controls receiving; it does not limit active
handlers. Impress does not impose a local concurrency limit.

The SDK handles continued reading and its built-in recovery. Impress does not
reopen the iterator in a retry loop. A terminal error opening or reading the
iterator is logged and marks the local reader as stopped. Subscriber
reconciliation or an application restart can start it again.

Successful handling sends ACK. A JSON decoding error, handler failure or
timeout sends NAK with `retryDelay`. If a worker disappears without
acknowledging, JetStream uses the consumer's `ack_wait` or `backoff` to schedule
redelivery. When creating a consumer, Impress sets `ack_wait` to
`timeout + 1000` milliseconds and `max_deliver` to `retryLimit + 1`. These
defaults do not override an existing consumer. Native `backoff` does not apply
to the explicit NAK sent by a failed handler; that uses `retryDelay`.
Keep the consumer's acknowledgment deadline longer than the handler timeout
to avoid premature redelivery. Exhausted messages remain
subject to stream retention; Impress does not create a dead-letter queue or
replay them automatically.

Timeout aborts `context.signal` but cannot forcibly stop JavaScript or undo side
effects. Handlers should honor cancellation where possible and be idempotent
using a stable message identifier such as the outbox `message.id`: CDC
restarts, missing ACKs and timeouts can cause duplicates.
Concurrent handlers and retries do not guarantee completion order.

New consumers start according to their configured delivery policy. Normal
restarts resume the existing durable position. Changing the subscriber's stream
or subject selects a different durable consumer. Updating only its method keeps
the existing consumer and position. Change retry limits and acknowledgment
deadlines directly in NATS for an existing consumer. The subscriber's
`timeout` still controls the local handler timeout.
Changing `maxMessages` or `maxAckPending` restarts the local reader using the
same durable consumer and position. Removing `maxAckPending` preserves the
consumer's current limit; set `-1` explicitly to remove the limit.

Shutdown stops new pulls and waits for in-flight handlers to finish or time
out. Removing a subscriber stops its local handler but does not delete the
durable consumer or its pending messages. Retiring old consumers is an
explicit infrastructure operation.

Subscriber declarations require non-empty `stream` and `subject` strings and
a `method` function. Startup fails if declarations cannot be read, parsed or
validated.
A file reload that fails these checks logs the error and retains the last
working declaration and handler.

During synchronization, a failure to bind or remove one subscriber does not
prevent other subscribers from being updated. Each failure is logged, and
failed operations are retried in the background. Binding failures during
startup still fail startup and stop any subscribers already started in that
attempt.

## Migration

Replace subscriber `event: 'chat:1:message:created'` with
`stream: 'EVENTS'` and `subject: 'events.chat.1.message.created'`. Move the stream
name from `nats.jetstream.stream` into each subscriber. Keeping the same stream,
subscriber name, consumer prefix and event subject preserves the existing
durable consumer and its position.

Handlers now receive the full JSON as `method(message, context)`. For outbox
events, replace the old first argument with `message.payload` and read event
metadata from `message.id`, `message.name` and `message.createdAt`. The
cancellation signal remains in the second argument as `context.signal`.
Update `SubscriberDeclaration<Message>` annotations to describe the complete
message envelope.

Configure the outbox, application table and CDC pipeline before switching
publishers and subscribers. Provision the event stream through the Debezium
deployment, then start a server with `server.events: true` to create consumers
before publishing events through CDC. With
the default `deliverPolicy: 'new'`, publish after consumers have been created.
Remove `transports` from event declarations. The old direct Core NATS
publication and pg-boss event delivery paths have been removed.

Set `nats.servers` to an array of addresses. Wrap an existing single address
in an array or split a comma-separated environment variable in the config.

Replace subscriber `concurrency` with `maxMessages` to configure receive
buffering. The former local handler limit is removed; `maxMessages` does not
provide the same limit.

Readable consumer names replace the previous hash-based names. Existing
consumers and their positions are not renamed or removed automatically. A
consumer with a new name uses its configured initial delivery policy.

Old pg-boss event queues and their pending jobs are not migrated or deleted
automatically. Drain or migrate them as a separate deployment step. The pg-boss
dependency and configuration have also been removed from tasks and cron.

## Runtime modules

The event subsystem lives in `lib/events/`:

- `publisher.js` builds event metadata and writes the outbox.
- `outbox.js` inserts records using the application's database adapter.
- `declarations.js` loads and normalizes declarations.
- `subscriptions.js` reconciles declarations and active handlers.
- `transports.js` checks the existing stream and configures durable consumers.
- `consumer.js` pulls messages, dispatches handlers and acknowledges delivery.

`lib/nats.js` owns the shared NATS connection and JetStream clients.
`lib/scheduler.js` manages task schedules and `lib/task-consumer.js` receives
task jobs. Catalogs and their shared `DiscoveryWorker` live in `lib/catalog.js`.

The `events` field from `await application.getDocumentation()` contains local
declarations and discovered remote metadata. Local declarations take
precedence. `origin: 'local' | 'remote'` describes the declaration's source;
all events use the `nats` transport.

Optional integration tests use `NATS_TEST_SERVERS` and
`NATS_TEST_CREDENTIALS` for NATS with JetStream. They create and remove a
unique test stream. Event tests prefix subjects with an isolated
`impress_test.<suffix>.` namespace, so they can run alongside an existing
`EVENTS` stream without changing it. They simulate the database adapter and
CDC publication; they do not start or verify Debezium.
`OUTBOX_TEST_CONNECTION_STRING` enables PostgreSQL autocommit, commit and
rollback tests in a unique temporary schema.
