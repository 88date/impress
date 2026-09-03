# Tasks (pg-boss)

Tasks live directly in `application/tasks`. There is no
`.queue.js`: one file declares one queue, one cron schedule, and one handler.

```text
application/
  tasks/
    cleanup.js
    updateCounters.js
```

## Task declaration

```js
/** @type {TaskDeclaration} */
({
  retryLimit: 3,
  retryDelay: 60,
  retryBackoff: true,
  expireInSeconds: 1800,
  heartbeatSeconds: 60,
  priority: 1,
  localConcurrency: 1,
  cron: '0 3 * * *',
  tz: 'Europe/Moscow',
  data: { automatic: true },

  method: async (data, job) => {
    await domain.cleanup.run(data);
    return { completed: true, jobId: job.id };
  },

  onCompleted: async (result, job) => {
    console.info(`Cleanup completed: ${job.id}`, result);
  },

  onFailed: async (reason, job) => {
    console.error(`Cleanup failed: ${job.id}`, reason);
  },
});
```

The relative file path identifies the task. Impress adds the `tasks/` prefix
before using it as the pg-boss queue name. For example, `reports/daily.js`
uses the `tasks/reports/daily` queue.

All options are declared at the top level. Impress routes each option to the
queue, scheduled job, consumer, or schedule according to its name. Nested
`queue`, `job`, `worker`, and `schedule` blocks are not supported.

`cron` and `method` are required. `tz` selects the schedule's time zone.
`data`, `onCompleted`, and `onFailed` are optional.

Queue settings:

- `policy`, `partition`, `warningQueueSize`, `deadLetter`
- `retryLimit`, `retryDelay`, `retryBackoff`, `retryDelayMax`
- `expireInSeconds`, `retentionSeconds`, `deleteAfterSeconds`
- `heartbeatSeconds`

The active scheduler applies explicitly declared queue settings during startup
and live reload. Existing queues and their jobs are preserved; only changed
settings are passed to `updateQueue`. Omitting a setting preserves its stored
value. Queue notifications are controlled globally by `server.scheduler.notify`.
Set `deadLetter`, `retryDelayMax`, or `heartbeatSeconds` to `null` to clear them.
`policy` and `partition` can only be set when creating a queue; changing either
on an existing queue reports an error.

Retry, expiration, retention, heartbeat, and dead letter settings are stored
on the queue and inherited by scheduled jobs. Task declarations do not provide
per-job overrides for these settings.

Scheduled job settings:

- `priority`
- `singletonKey`, `singletonSeconds`, `singletonNextSlot`
- `group`

Consumer settings:

- `pollingIntervalSeconds`, `notifyPollingIntervalSeconds`
- `burstWhenReadyExceeds`, `burstWhenBatchFull`
- `orderByCreatedOn`, `minPriority`, `maxPriority`
- `localConcurrency`, `localGroupConcurrency`, `groupConcurrency`
- `heartbeatRefreshSeconds`

## Configuration

The pg-boss connection is controlled by `application/config/pgboss.js`:

```js
({
  enabled: process.env.PG_BOSS_ENABLED === 'true',
  connectionString: process.env.DATABASE_URL_PG,
  ssl: {
    caPath: 'application/cert/postgres.crt',
    rejectUnauthorized: true,
  },
});
```

Relative `caPath` values are resolved from the application working directory
and loaded before connecting.

`server.scheduler.notify` selects the task delivery mode. It defaults to
`false` (polling). Setting it to `true` enables the pg-boss listener and queue
notifications for all task queues. Task declarations do not accept `notify`;
the loader reports an error directing you to `server.scheduler.notify`.

When the task subsystem is enabled, Impress derives the shared client's
`useListenNotify` option from `server.scheduler.notify`, taking precedence over
the value in `config.pgboss`. When tasks are disabled, the pg-boss connection
configuration is used as supplied.

Notifications wake consumers when immediately available jobs are inserted.
Polling remains as a fallback; future-dated jobs still rely on polling when
they become available. The listener uses one dedicated PostgreSQL connection
in the master thread in addition to the query pool. It requires a direct or
session-pooled connection; PgBouncer transaction pooling does not support it.
Logical replication is not required.

Restart the application after changing `server.scheduler.notify`. The active
scheduler updates the stored queue flags without recreating queues or jobs,
even when the cron schedule is unchanged. Switching the global flag to `false`
disables the listener and turns off task queue notifications during
synchronization. Use the same global mode on all task-consuming instances.

Only the designated instance should manage task declarations:

```js
// application/config/server.js
({
  // Other server settings
  scheduler: {
    enabled: process.env.SCHEDULER_ENABLED === 'true',
    active: process.env.SCHEDULER_ACTIVE === 'true',
    notify: process.env.SCHEDULER_NOTIFY === 'true',
  },
});
```

The `scheduler.enabled` setting controls the entire task subsystem. Disabled
instances do not load task declarations, register consumers, or manage
schedules. Every instance with `scheduler.enabled` and `pgboss.enabled`
registers task consumers, so pg-boss can distribute jobs between application
instances. Only the designated instance should set `scheduler.active` to
create and remove schedules and update existing queue settings. Inactive
instances may create a missing queue with its declared settings before
registering a consumer, but do not change an existing queue.

Task files are loaded through the common `Code` loader. Only serializable
declarations are sent to the master thread. Schedules are synchronized during
startup and live reload. Deleting a task file removes its schedule, but does
not delete its queue or stored jobs. The shared pg-boss client receives jobs in
the master thread and delegates their methods to regular application workers.
