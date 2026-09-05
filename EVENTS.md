# Application events

Event declarations describe what an application publishes. Subscriber
declarations describe how an application handles an event.

## Declaring and publishing

`application/events/chat.1/messageCreated.js`:

```js
({
  caption: 'Message created',
  examples: [{ messageId: 42 }],
  transports: ['local', 'nats'],
});
```

The path becomes the event name `chat:1:message:created` and NATS subject
`chat.1.message.created`. A directory without a version defaults to version 1.
Use the same canonical event name in subscribers and publishers:

```js
const id = await events.emit('chat:1:message:created', { messageId: 42 });
```

The event envelope contains `id`, `name`, `createdAt`, and `data`.
Subscribers receive `data` as the first argument and the envelope metadata as
the second argument.

`emit()` rejects unknown events, declarations without delivery transports,
and publication when a required transport is unavailable. Local delivery
requires a pg-boss client; NATS delivery requires a NATS connection.
All declared transports are checked before sending, so a declaration with
both `local` and `nats` cannot silently publish to just one of them.
Availability is checked on each call, including after a transport restarts.
This check does not guarantee subscriber processing.

## Subscribers

`application/subscribers/feed.1/messageCreated.js`:

```js
({
  event: 'chat:1:message:created',
  concurrency: 2,
  retryLimit: 5,
  retryDelay: 1000,
  timeout: 30000,
  method: async (data, event) => {
    await domain.feed.append(data, event.id);
  },
});
```

The subscriber name is `feed:1:message:created`. Its pg-boss queue is
`subscribers/feed/1/messageCreated`; its NATS queue group is
`feed.1.message.created`. Copies of this subscriber share the same queue or
queue group. Different subscriber names receive independent copies of an event.

The pg-boss queue name preserves the subscriber's file and directory names,
without `.js`, and inserts the version after the top-level directory.
For example, `application/subscribers/profile/handleCreateMessage.js` becomes
`subscribers/profile/1/handleCreateMessage`.

- `local` delivers through pg-boss within the application, across its
  workers and instances. It does not mean an in-process callback.
- `nats` publishes to NATS. Server workers consume NATS events;
  regular workers and balancers may publish but do not subscribe to them.
- A subscriber uses pg-boss when the event is declared locally with
  `local` delivery and pg-boss is enabled. Otherwise an event declared with
  `nats`, or one published by another application,
  uses NATS when available.

`concurrency`, `retryLimit`, `retryDelay`, and `timeout` configure pg-boss
consumers. Delays and timeouts in declarations are in milliseconds.
These settings do not apply to Core NATS subscribers.

## Configuration and ownership

Connections are configured through `server.pgboss` and `server.nats`.
Set `server.pubsub.active: true` on the instance responsible for managing
subscriber queues. Its designated discovery worker creates and updates
bindings and removes queues for deleted subscribers. Other workers consume
without changing the shared topology.

Changing a subscriber's event replaces its persisted pg-boss binding,
including after an application restart. Moving a subscriber to NATS clears
its previous local binding without deleting the queue.

Startup fails if a declaration cannot be read, parsed, or validated.
Queue cleanup runs only after declarations have loaded successfully.
A failed file reload logs the error and retains the last working declaration.
Deleting a subscriber on the managing instance removes its queue and jobs.

## Transactions and delivery

With pg-boss enabled, outgoing NATS events first enter the shared
`events/nats/publish` outbox queue. A publisher forwards them to NATS.
Without pg-boss, NATS events publish directly.

When both `local` and `nats` are declared, local subscriber jobs and the
outbox entry are written in one PostgreSQL transaction. If no transaction is
supplied, `emit()` opens one and returns only after it commits. A failed write
rolls back both directions. NATS forwarding happens after commit, outside
this transaction.

```js
await events.emit('chat:1:message:created', data, { transaction });
```

The transaction must use the same PostgreSQL database as pg-boss and expose
`query(text, values)`, returning a result with `rows`. Both local delivery
and outbox insertion use the supplied transaction. Direct NATS publication
cannot participate in a database transaction.

With a supplied transaction, `emit()` neither commits nor rolls it back and
does not open a nested transaction. The caller must commit after success or
roll back on failure. Use this form to commit business changes and event jobs
together; the automatic transaction covers only event publication.

Automatic transactions use the built-in pg-boss database connection. A custom
`server.pgboss.db` adapter must implement `withTransaction(callback)`, passing
a database with `executeSql(text, values)` to the callback and handling commit
and rollback. Otherwise, pass an existing transaction to `emit()`.

Core NATS does not acknowledge handler completion or redeliver failed
subscriber calls. The outbox covers publication to NATS, not processing by
NATS subscribers. Local handlers should tolerate retries and use the
event ID when deduplication is required.

## Runtime modules

The event subsystem lives in `lib/events/`. The framework imports its classes
through `lib/events/index.js`.

| File               | Responsibility                                                                         |
| ------------------ | -------------------------------------------------------------------------------------- |
| `index.js`         | Exports the subsystem's classes to the framework.                                      |
| `publisher.js`     | Builds envelopes, publishes events, and forwards the outbox.                           |
| `subscriptions.js` | Reconciles declarations and active handlers.                                           |
| `declarations.js`  | Loads event and subscriber declarations, validates them, and derives names from paths. |
| `transports.js`    | Implements the pg-boss and NATS subscription adapters.                                 |

The connection modules `lib/pgboss.js` and `lib/nats.js` also serve scheduled
tasks and RPC, respectively. Catalogs and their shared `DiscoveryWorker`
are grouped in `lib/catalog.js`.

The `events` field returned by `await application.getDocumentation()` contains
local declarations and discovered remote event metadata. Local declarations
take precedence when a name appears in both sources. NATS discovery uses
cached snapshots.

Event documentation includes `origin: 'local' | 'remote'` relative to the
application serving the documentation and the declared `transports`.
A local declaration published through NATS remains local. Remote entries
come from applications that respond to NATS discovery and advertise the
`nats` transport; their local-only events are not visible. Service method
documentation also includes `origin`, based on where the method executes.

## Migration and verification

Use `nats` instead of `external` in event `transports`.
The old `external` transport name is no longer accepted.

Use `method` instead of `handler` in subscriber declarations.
Both local and NATS delivery invoke `method(data, event)`.

Use `server.pgboss` instead of the misspelled `server.bgboss`.
The old `service.<name>.emit/on` API and `EventBroker` have been removed.
Move publishers to `events.emit` and listeners to subscriber declarations.
RPC action calls remain `service.<name>.<action>(args)`.

The optional integration tests use:

- `NATS_TEST_SERVERS` and `NATS_TEST_CREDENTIALS` for NATS.
- `PGBOSS_TEST_CONNECTION_STRING` for PostgreSQL. The test creates and removes
  a unique schema, leaving other schemas unchanged.
