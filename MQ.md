# Message Queue (NATS JetStream)

MQ publishes JSON tasks through NATS JetStream and dispatches them to regular
application workers. It uses `server.nats`; Redis is not required.
The first worker with MQ handlers creates the default `MQ` stream if missing.
Custom streams are provisioned by infrastructure.

## Handlers

Each directory under `application/mq` names a queue. Each job file names a
handler. For example, `application/mq/push/send.js` handles `push:send`:

```js
/** @type {MQDeclaration} */
({
  caption: 'Send a push notification',
  method: async (data, job) => {
    job.signal.throwIfAborted();
    await domain.notification.push.send(data);
  },
});
```

The method receives decoded JSON and a `job` with `id`, `name`, `data`,
`createdAt`, `retryCount`, `signal`, and `headers`. The ID is `stream:sequence`
and remains the same on redelivery. `retryCount` starts at zero. Headers are
the original NATS headers. Return values are not stored as task results.

## Publishing

The universal API needs no local queue directory, configuration file, or
handler:

```js
const acknowledgment = await mq.add('analysisProfile:profile', { profile });
```

For locally declared queues and handlers, these calls use the same publisher:

```js
await mq.push.send(data);
await mq.push.add('send', data);
await mq.add('push:send', data);
```

The route is exactly `queue:job`, with non-empty names containing no dots,
wildcards, whitespace, or additional colons. Unicode names are supported.
`push:send` publishes to `mq.push.send`. The body is `JSON.stringify(data)`,
without an additional envelope. Objects, arrays, scalars, and `null` are
supported; values that cannot produce JSON reject publication.

Publish after the application has started. Each call waits for the native
JetStream `PubAck`, containing `stream`, `seq`, and `duplicate`. This confirms
storage, not execution. Connection, permission, missing-stream, and publication
errors reject the call. A failed acknowledgment does not prove that the task
was not stored, so retrying can create duplicates.

## Bulk publication

Publish several tasks as one native JetStream atomic batch:

```js
const acknowledgment = await mq.addBulk([
  {
    name: 'reaction:resetDislikes',
    data: { userId: 1 },
    options: { msgID: 'reset:1' },
  },
  {
    name: 'reaction:resetDislikes',
    data: { userId: 2 },
    options: { msgID: 'reset:2', delay: '1h' },
  },
]);

await mq.reaction.addBulk([
  { name: 'resetDislikes', data: { userId: 3 } },
  { name: 'resetDislikes', data: { userId: 4 } },
]);
```

The universal form requires no local declarations. The queue form uses job
names without the queue prefix. `addBulk` is reserved as a queue and handler
name. Each entry accepts the same `options` as `mq.add`.

Requires NATS Server 2.12+ (2.12.1+ with `msgID`). All subjects must belong to
one stream with `allow_atomic: true` enabled by infrastructure.
MQ prepares all entries before sending, then uses the SDK's
`startBatch`, `add`, and `commit`. A single entry is sent with the native
single-message batch headers. Empty arrays return `null` without publishing.

NATS stores the entire batch or rejects it. The result is one native
`BatchAck` with `stream`, `seq`, `batch`, and `count`; `seq` is the final
stored sequence. A lost final acknowledgment leaves the outcome unknown:
the entire batch may already be stored. Execution remains separate for each
task, using the queue's normal processing limits and retry policy.
The current SDK can report commit rejection with the generic message
`batch didn't contain number of published messages`.

`delay` works at any position in a batch. It stores a schedule instead of an
immediate task and also requires `allow_msg_schedules: true`. Delays start
when MQ prepares the entries; if a deadline passes before commit, delivery
can begin as soon as the batch commits. The batch ACK confirms schedule
storage, not execution.

Unlike individual publishes, any duplicate `msgID` rejects the entire batch,
including duplicate IDs within the batch. Server batch-size and inactivity
limits apply; MQ does not split a large batch into separate transactions.
See the [NATS batch protocol](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md).

## Deduplication

Individual publication forms accept the native NATS `msgID` option:

```js
await mq.add('push:send', data, { msgID: 'push:reminder:42' });
await mq.push.send(data, { msgID: 'push:reminder:43', delay: '1h' });
await mq.push.add('send', data, { msgID: 'push:reminder:44' });
```

MQ passes the ID unchanged. Reusing it within the stream's `duplicate_window`
returns the original sequence with `duplicate: true`; the first payload is
preserved. IDs are shared across the entire stream, including different
queues and handlers. Include the queue or operation in the ID when needed.

Infrastructure configures `duplicate_window` on the stream. The window starts
at publication, independently of execution or ACK. After it expires, the same
ID can create another task. Omitting `msgID` publishes each call separately.

With `delay`, deduplication applies to the schedule: a duplicate does not
replace its payload or postpone its deadline. NATS removes the ID when the
schedule fires, so the scheduled task can be delivered within the same
deduplication window. This prevents duplicate publications, not redeliveries
after a missing ACK; handlers must still tolerate repeated execution.

## Delayed tasks

All publication forms accept a delay:

```js
await mq.add('push:send', data, { delay: '7d' });
await mq.push.send(data, { delay: '15m' });
await mq.push.add('send', data, { delay: 60000 });
```

Numbers are milliseconds. Strings contain a non-negative integer followed by
`ms`, `s`, `m`, `h`, or `d`: `'250ms'`, `'30s'`, `'15m'`, `'2h'`, `'7d'`.
A day is exactly 24 hours. Fractions, compound values, negative numbers, and
unknown units are rejected before publication. Omit `delay` or use `0` for
immediate publication. Only `delay` and `msgID` are supported; other publication
options, including `jobId` and `priority`, are rejected.

Delayed publication requires NATS Server 2.12+ and infrastructure to enable
`allow_msg_schedules: true` on the target stream. Use `discard: 'old'`;
scheduling is incompatible with `discard: 'new'`. The framework does not
enable scheduling or change stream settings automatically.

For `push:send`, MQ stores the schedule at `mq.push.send.<uuid>` with
`mq.push.send` as its target. Both subjects must belong to the same stream;
`subjects: ['mq.>']` covers them. Publisher credentials need permission to
publish to the schedule subjects as well. The queue consumer's existing
`mq.push.*` filter excludes schedules, so waiting tasks do not occupy workers
or use `maxAckPending`.

The returned `PubAck` confirms storage of the schedule. Its sequence differs
from the later task's `job.id`. NATS creates the task at the scheduled time;
execution can be later if workers are unavailable or busy. The schedule
survives publisher disconnection and worker restarts. Stream age and capacity
limits must allow the schedule to remain stored until it fires: removing it
also cancels delivery. See the
[NATS scheduler design](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md).

## Queue settings

`application/mq/push/.queue.js` is optional and configures consumption:

```js
/** @type {MQQueueOptions} */
({
  stream: 'MQ',
  maxMessages: 50,
  maxAckPending: 2,
  retryLimit: 2,
  retryDelay: 1000,
  timeout: 30000,
});
```

| Setting         | Meaning                                                 | Default                                 |
| --------------- | ------------------------------------------------------- | --------------------------------------- |
| `stream`        | Existing stream containing this queue's subjects        | `MQ`                                    |
| `maxMessages`   | Receive buffer size                                     | `5`                                     |
| `maxAckPending` | Unacknowledged messages across all workers of the queue | New consumer: `-1`; existing: preserved |
| `retryLimit`    | Redeliveries after failure or missing ACK               | New consumer: `-1`; existing: preserved |
| `retryDelay`    | Delay after a failed handler, in milliseconds           | `0`                                     |
| `timeout`       | Handler deadline, in milliseconds                       | `30000`                                 |

When a worker starts consuming the default `MQ` stream and it is missing,
Impress creates it with `subjects: ['mq.>']`, file storage, work-queue retention,
and both `allow_msg_schedules` and `allow_atomic` enabled. Replicas default to
`server.nats.jetstream.replicas`, then `1`. Existing stream settings are preserved.
Workers need stream-creation permission for this initial setup, in addition to
consumer management and consumption permissions.

A custom `stream` must already exist and use subjects that do not overlap other
streams. Publication routes by subject, independently of queue configuration.
Publishers need publication and acknowledgment permissions; they do not create
streams. Before publishing from a producer-only service, start a worker with
handlers using `MQ`, or provision the stream through infrastructure.

## Delivery and lifecycle

Each queue uses one durable consumer named `mq-<URI-encoded queue name>`, with
the filter `mq.<queue>.*`. Its workers share that consumer. A new consumer
reads all pending tasks, uses explicit acknowledgments, and has an ACK deadline
of `timeout + 1000` milliseconds. Explicit `maxAckPending` and `retryLimit`
update the corresponding consumer settings. Omitted settings are preserved.

`maxAckPending` is a total limit across the queue's workers. It is not BullMQ's
per-worker `concurrency`. `maxMessages` only controls the receive buffer.
Success sends ACK. Handler, JSON decoding, and timeout failures send NAK with
`retryDelay`. `retryLimit: 2` allows three deliveries in total, `0` permits
only the first delivery, and `-1` removes the limit. It maps to JetStream's
`max_deliver` and counts deliveries across all workers, including recovery
after a missing ACK. Restarting a worker does not reset the counter.

After the limit is reached, NATS stops redelivering that message. The message
remains in the stream, subject to its retention limits; MQ does not ACK it
as successful, delete it, or move it to a dead-letter queue. See the
[NATS redelivery documentation](https://docs.nats.io/learn/jetstream/acknowledgment).
Completed tasks have no retained result history.

Timeout aborts `job.signal` but cannot forcibly stop handler code. Keep an
existing consumer's ACK deadline longer than the handler timeout. Code that
continues after a timeout can overlap a retry even with `maxAckPending: 1`.
Delivery is at least once; handlers must tolerate repeated execution.

Receivers start only in regular application workers, after NATS is ready.
Shutdown stops reads and waits for tracked processing before closing NATS,
modules, or database clients. If a worker disappears, its unacknowledged tasks
become available for redelivery after the consumer's ACK deadline.

Handler files can be reloaded without recreating the consumer. Removing the
last handler or its queue directory stops the local receiver. The durable
consumer and stored tasks remain in NATS; infrastructure manages their removal.

## Status

Read a specific queue directly from NATS, including queues handled by another
service:

```js
const status = await mq.getStatus('analysisProfile');
const custom = await mq.getStatus('analysisProfile', { stream: 'ANALYSIS' });
```

No local queue directory or handler is needed. The stream defaults to the
local queue's `stream` setting, if present, then `MQ`. The query reads the
durable `mq-<URI-encoded queue name>` and does not create or change it. If it
has not been created yet, `consumer` is `null`, `unacknowledged` is `0`, and
`pending` counts stored messages matching `mq.<queue>.*`. This includes jobs
published before the first worker starts. Missing streams and permission
errors reject the call instead of reporting an empty queue.

`await mq.getStatus()` reports the current instance's active queue consumers:

```js
({
  push: {
    stream: 'MQ',
    consumer: 'mq-push',
    running: true,
    pending: 4,
    unacknowledged: 2,
  },
});
```

`pending` counts messages not yet delivered; `unacknowledged` counts delivered
messages still awaiting ACK, including retries. Both are shared across the
consumer's workers. To check for unfinished work, use their sum.
`running` describes only this instance's reader; `false` does not
mean that remote workers are stopped. Status queries require account-info
(`$JS.API.INFO`) and consumer-info permissions, plus stream-info permissions
for queues without a consumer.
This API does not provide BullMQ completed/failed counts or history.
Delayed schedules are not included until NATS creates their target messages.

## Migrating from BullMQ

Remove the Redis settings file `application/config/mq.js`. Move handler
deadlines to the queue's `.queue.js`, and choose an explicit `maxAckPending`
when a shared pending limit is needed.

For a queue-wide attempt limit, replace `defaultJobOptions.attempts: 3` with
`retryLimit: 2`. Per-handler attempt limits and exponential backoff remain
unsupported; `retryDelay` is a fixed delay after failure.

Legacy queue options such as `concurrency`, `limiter`, `defaultJobOptions`,
`schedules`, lock settings, and queue events are rejected. Handler options
such as `attempts`, `backoff`, `timeout`, `onCompleted`, and `onFailed` are also
unsupported. They must be removed or migrated explicitly.

Direct `.queue`, `.worker`, and `.flow()` access is removed. `jobId`
is unsupported; use `msgID` for deduplication within the stream's configured
window. Recurring MQ schedules, progress, and result history are not
implemented. One-off delays use the publication `delay` option.
Scheduled tasks under `application/tasks` use their separate NATS scheduler;
see [TASKS.md](TASKS.md).

Replace `queue.getJobCounts('waiting')` with `mq.getStatus(queueName)` and
read `pending`. Counts are observations, not a lock against other publishers.
Replace `mq.<queue>.queue.addBulk(jobs)` with `mq.<queue>.addBulk(jobs)`.
Per-job publication settings belong in `options`. All entries must route
to the same stream, and the result is one batch ACK.

Existing jobs in Redis are not moved or deleted. Migrating application queues
and external workers is a separate deployment step. Arrange for outstanding
BullMQ jobs to finish before switching their publishers and handlers.

## Isolated integration test

Set `NATS_SERVER_BINARY` to an installed `nats-server` executable and run:

```text
node --test test/mq.integration.js
```

The test starts its own server on a random loopback port, with temporary file
storage, and stops it afterward. It does not use `NATS_TEST_SERVERS` or the
application's NATS credentials. It checks storage/ACK, retries, two workers
sharing a pending limit, graceful drain, and redelivery after terminating a
worker thread. It also checks deduplication of immediate and delayed tasks,
delayed delivery after publisher disconnection and worker restart, plus
rejection when stream scheduling is disabled. Queue status is checked from
a publisher with no local handlers, both before and after consumer creation.
Native batches are checked for atomic storage, delayed delivery, duplicate
IDs, and stream configuration errors.
Without `NATS_SERVER_BINARY`, the integration test is skipped.
