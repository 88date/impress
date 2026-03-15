# Message Queue (BullMQ)

## File structure

```
mq/
  push/
    .queue.js         - queue configuration
    welcome.js        - handler for job "welcome"
    promo.js          - handler for job "promo"
  scheduler/
    .queue.js
    cleanup.js
    healthCheck.js
```

Folder = queue. File = job handler. File name = `job.name`.

## Queue configuration - `.queue.js`

```js
({
  concurrency: 5,
  limiter: { max: 100, duration: 60000 },
  lockDuration: 30000,
  stalledInterval: 15000,

  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: { age: 3600, count: 1000 },
    removeOnFail: { age: 86400, count: 5000 },
  },

  schedules: [
    { name: 'cleanup', pattern: '0 3 * * *', data: {} },
    { name: 'healthCheck', every: 300000, data: {} },
  ],

  events: {
    completed: (job, result) => {},
    failed: (job, error) => {},
    stalled: (jobId) => {},
  },
});
```

## Job handler - `welcome.js`

```js
({
  attempts: 5,
  backoff: { type: 'exponential', delay: 2000 },
  timeout: 30000,

  method: async (data, job) => {
    const { userId } = data;
    await job.updateProgress(50);
    await job.log('Sending push');
    await domain.notification.push.send(userId, 'welcome');
    return { sent: true };
  },
});
```

Job options merge priority: `defaultJobOptions` < job file options < `opts` at call site.

### Job events (QueueEvents)

Job files can optionally define `onCompleted` and `onFailed` handlers.
These use BullMQ `QueueEvents` (Redis Pub/Sub) and work across processes/servers.
The connection is created lazily — only if at least one job file in the folder has event handlers.

```js
// mq/push/welcome.js
({
  method: async (data, job) => {
    await domain.notification.push.send(data.userId);
    return { sent: true };
  },

  onCompleted: (result, job) => {
    console.log(`Welcome push sent, job ${job.id}`);
  },

  onFailed: (reason, job) => {
    console.log(`Welcome push failed: ${reason}`);
  },
});
```

## API

### Add job

```js
await mq.push.welcome({ userId: 123 });
await mq.push.welcome({ userId: 123 }, { priority: 1 });
```

### Bulk

```js
await mq.push.queue.addBulk([
  { name: 'welcome', data: { userId: 1 } },
  { name: 'promo', data: { userId: 2 } },
]);
```

### Flow (parent waits for children)

```js
await mq.push.flow({
  name: 'welcome',
  data: { step: 'final' },
  children: [
    { name: 'promo', queueName: 'push', data: { step: 1 } },
  ],
});
```

### Schedules (dynamic)

```js
await mq.push.queue.upsertJobScheduler('newTask', { every: 60000 }, { data: {} });
await mq.push.queue.removeJobScheduler('newTask');
await mq.push.queue.getJobSchedulers();
```

### Monitoring

```js
const status = await mq.getStatus();
// { push: { counts: { waiting: 0, active: 1, ... }, paused: false } }
```

### Direct BullMQ access

```js
mq.push.queue    // Queue instance
mq.push.worker   // Worker instance (null on server threads)
```

## Redis config - `config/mq.js`

```js
({ host: '127.0.0.1', port: 6379, db: 0, prefix: 'bull' });
```

## Reserved file names

`flow`, `queue`, `worker`, `events`, `add`, `getStatus` - cannot be used as job names.