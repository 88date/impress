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
  cron: '0 3 * * *',
  tz: 'Europe/Moscow',
  data: { automatic: true },

  localConcurrency: 1,
  retryLimit: 2,
  retryDelay: 60,
  retryBackoff: true,
  expireInSeconds: 1800,
  heartbeatSeconds: 60,

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

The following pg-boss job options can be placed at the top level:

- `priority`
- `retryLimit`, `retryDelay`, `retryBackoff`, `retryDelayMax`
- `expireInSeconds`, `retentionSeconds`, `deleteAfterSeconds`
- `heartbeatSeconds`
- `singletonKey`, `singletonSeconds`, `singletonNextSlot`
- `group`, `deadLetter`

Worker options are also declared at the top level:

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

Only the designated instance should manage task declarations:

```js
// application/config/server.js
({
  // Other server settings
  scheduler: {
    enabled: process.env.SCHEDULER_ENABLED === 'true',
    active: process.env.SCHEDULER_ACTIVE === 'true',
  },
});
```

The `scheduler.enabled` setting controls the entire task subsystem. Disabled
instances do not load task declarations, register consumers, or manage
schedules. Every instance with `scheduler.enabled` and `pgboss.enabled`
registers task consumers, so pg-boss can distribute jobs between application
instances. Only the designated instance should set `scheduler.active` to
create and remove schedules.

Task files are loaded through the common `Code` loader. Only serializable
declarations are sent to the master thread. Schedules are synchronized during
startup and live reload. Deleting a task file removes its schedule, but does
not delete its queue or stored jobs. The shared pg-boss client receives jobs in
the master thread and delegates their methods to regular application workers.
