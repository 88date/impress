# Tasks (NATS JetStream)

Task files live in `application/tasks`. NATS Server **2.14 or newer** with
JetStream enabled is required for recurring cron schedules and time zones.
Impress uses the existing NATS servers and credentials from `server.nats`.

```javascript
({
  cron: '30 0 * * *',
  tz: 'Europe/Moscow',

  method: async () => {
    return await modules.statistic.usecase.calculateDailyCounts('2023-06-16');
  },

  onCompleted: async (result, job) => {
    console.info(`Task ${job.id} completed`, result);
  },

  onFailed: async (reason, job) => {
    console.error(`Task ${job.id} failed`, reason);
  },
});
```

Five-field cron expressions are converted to six fields by adding `0` for
seconds. Six-field expressions are passed through. `tz` is an IANA time zone,
defaulting to `UTC`. Cron syntax is validated by NATS when the schedule is
registered; task methods and option types are the developer's responsibility.

## Configuration

```javascript
({
  nats: {
    enabled: process.env.NATS_ENABLED === 'true',
    servers: process.env.NATS_SERVERS?.split(',').map((server) =>
      server.trim(),
    ),
    credentials: process.env.NATS_CREDENTIALS,
    discovery: { maxWait: 1000 },
    jetstream: { consumerPrefix: 'backend', replicas: 3 },
  },
  scheduler: {
    enabled: process.env.SCHEDULER_ENABLED === 'true',
    active: process.env.SCHEDULER_ACTIVE === 'true',
    stream: 'TASKS',
  },
});
```

- `scheduler.enabled` enables task loading and consumption. It requires
  `nats.enabled: true` and at least one regular application worker.
- `scheduler.active` enables creation, updates, and removal of schedules and
  updates to consumer settings. Instances with `active: false` still execute
  tasks. At least one instance must manage schedules.
- `scheduler.stream` defaults to `TASKS`, separate from the events stream.
  Each task stream uses its own `tasks.<stream>.>` subject namespace.
- `scheduler.consumerPrefix` defaults to `nats.jetstream.consumerPrefix`,
  then `impress`. It namespaces both schedules and consumers.
- `scheduler.replicas` defaults to `nats.jetstream.replicas`, then `1`.
  It is used when creating the task stream; existing replication settings
  are preserved. Three replicas require three JetStream nodes.

All enabled instances can create a missing task stream and missing consumers.
The stream uses file storage, `workqueue` retention, subjects
`tasks.<stream>.>` (by default `tasks.TASKS.>`), and
`allow_msg_schedules: true`. Credentials need the corresponding JetStream
management permissions. These operations happen when the application starts.

## Multiple instances

Instances of the same application must share the NATS account, stream, prefix,
task paths, and task settings. For `reports/daily.js` with the default stream
and prefix:

- Schedule subject: `tasks.TASKS.impress.schedule.reports:daily`.
- Execution subject: `tasks.TASKS.impress.run.reports:daily`.
- Durable consumer: `impress|tasks:reports:daily`.

There is one schedule per subject and one shared consumer per task. Multiple
instances consume from that same durable, so a scheduled occurrence normally
runs on one instance. A second instance does not create an independent timer.
Unchanged schedules are reused on restart. A changed declaration replaces the
schedule on the same subject. Applications sharing `TASKS` must use different
prefixes; task names and prefixes should use letters, digits, `_`, and `-`,
with directories represented by `:` in NATS names.
Applications using different task streams have independent subject namespaces
and may reuse the same prefix and task paths.

Earlier experimental task streams capturing `tasks.>` are not converted
automatically. Migrate their schedules, pending jobs, and consumers to the
stream-specific subjects before deploying this layout; a broad `tasks.>`
stream would still prevent creation of another task stream.

When deploying different task definitions, designate one version to manage
schedules (`active: true`). Multiple managers with different declarations can
overwrite each other's schedules or remove tasks absent from their version.

## Task options

| Field         | Meaning                                                                   | Default                                              |
| ------------- | ------------------------------------------------------------------------- | ---------------------------------------------------- |
| `cron`        | Five- or six-field cron expression                                        | Required                                             |
| `tz`          | IANA time zone                                                            | `UTC`                                                |
| `data`        | JSON data passed to `method`                                              | `{}`                                                 |
| `maxMessages` | Native `consume({ max_messages })` receive buffer                         | `5`                                                  |
| `retryLimit`  | Redeliveries after failure or missing acknowledgment                      | `2`                                                  |
| `retryDelay`  | Delay after failure, in milliseconds                                      | `0`                                                  |
| `timeout`     | Deadline including worker dispatch, method and callbacks, in milliseconds | Positive `server.workers.timeout`, otherwise `30000` |

`maxMessages` is not a concurrency limit. Received jobs are dispatched without
awaiting the previous job. Separate occurrences of a task can overlap.
Timeouts do not forcibly stop a method already running in a worker.

`method(data, job)` runs in the regular application worker pool. Successful
execution calls `onCompleted(result, job)`; method errors call
`onFailed(reason, job)` with an error message string. Callback errors are logged
and do not change the method's result. NATS is acknowledged after the worker
returns; a dispatch failure or execution deadline causes a negative
acknowledgment. The receive loop starts only after application workers are ready.

`job` contains `id`, `name`, `data`, `createdAt`, and `retryCount`. Its ID combines
the stream and message sequence: it changes for each occurrence and remains
the same on redelivery while the stream exists. `retryCount` starts at zero.
This replaces the previous pg-boss job metadata.

## Storage and lifecycle

Consumers filter only execution subjects. A successful acknowledgment removes
that job from the work-queue stream, leaving its schedule intact. Consumers
use `deliver_policy: 'all'` so pending jobs survive application restarts.
Jobs that exhaust their delivery attempts remain stored for investigation;
there is no automatic dead-letter routing or cleanup of failed jobs.

Do not apply a global age or count limit that can evict schedule messages:
removing a schedule message stops that schedule. Deleting a task file removes
its schedule and stops its local consumer, but preserves pending jobs and the
server-side durable. Stopping an application stops consumption without deleting
schedules; NATS continues producing scheduled jobs while applications are down.

Delivery is at least once. A completed task can be delivered again if its
acknowledgment is lost, or if execution exceeds its deadline. Methods and
callbacks that cause external effects should tolerate retries.

## Migrating from pg-boss

`cron`, `tz`, `data`, `method`, `onCompleted`, and `onFailed` retain their syntax.
Remove `server.pgboss` and `server.scheduler.notify`. PostgreSQL is no longer
used for task scheduling or delivery. The separate BullMQ subsystem under
`application/mq` is unchanged.

pg-boss-specific options such as priority, singleton keys, queue policies,
groups, polling, expiration, and local concurrency are no longer supported.
`retryDelay` now uses milliseconds, matching event subscriber settings.

Existing pg-boss jobs are not migrated, and its database tables are not removed.
Stop the previous task workers before enabling NATS schedules to avoid having
both schedulers execute the same cron task. Upgrade all NATS nodes to a version
supporting recurring schedules before enabling this implementation.

References: [NATS schedule API](https://github.com/nats-io/nats.js/blob/v3.4.0/jetstream/README.md#jetstream-message-schedules),
[scheduler design and retention](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md).
