'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { randomBytes } = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');
const { Nats } = require('../lib/nats.js');
const { Scheduler } = require('../lib/scheduler.js');

const servers = process.env.NATS_TEST_SERVERS;
const credentials = process.env.NATS_TEST_CREDENTIALS;
const skip = !servers || !credentials;

const createRuntime = (suffix, index, cron) => {
  const stream = `TASKS_TEST_${suffix}_${index}`;
  const consumerPrefix = `integration${suffix}`;
  const application = {
    console,
    config: {
      server: {
        nats: {
          enabled: true,
          servers: servers.split(',').map((server) => server.trim()),
          credentials,
          jetstream: { replicas: 1 },
        },
        workers: { pool: 1 },
        timeouts: { stop: 1000 },
      },
    },
  };
  const nats = new Nats(application);
  const scheduler = new Scheduler(
    { enabled: true, active: true, stream, consumerPrefix },
    nats,
  );
  const declaration = {
    name: 'tasks/reports/daily',
    path: 'reports/daily',
    cron,
    tz: 'UTC',
    data: { stream },
    maxMessages: 5,
    retryLimit: 2,
    retryDelay: 50,
    timeout: 10000,
  };
  const attempts = [];
  const retried = Promise.withResolvers();
  const nextOccurrence = Promise.withResolvers();
  const finish = Promise.withResolvers();
  const execute = async (task, job) => {
    attempts.push(job);
    if (attempts.length === 1) throw new Error('Retry this scheduled task');
    if (job.id === attempts[0].id) {
      retried.resolve(job);
      await finish.promise;
    } else {
      nextOccurrence.resolve(job);
    }
  };
  return {
    stream,
    nats,
    scheduler,
    declaration,
    execute,
    attempts,
    retried,
    nextOccurrence,
    finish,
    manager: null,
    ownsStream: false,
  };
};

const waitForRemoval = async (runtime, job, signal) => {
  const sequence = Number(job.id.slice(runtime.stream.length + 1));
  while (
    await runtime.manager.streams.getMessage(runtime.stream, {
      seq: sequence,
    })
  ) {
    await delay(25, undefined, { signal });
  }
};

test(
  'scheduler integration - isolates streams and retries recurring tasks',
  { skip, timeout: 30000 },
  async (t) => {
    const suffix = randomBytes(8).toString('hex');
    const tomorrow = new Date(Date.now() + 86400000);
    const cron = [
      0,
      tomorrow.getUTCMinutes(),
      tomorrow.getUTCHours(),
      tomorrow.getUTCDate(),
      tomorrow.getUTCMonth() + 1,
      '*',
    ].join(' ');
    const runtimes = [
      createRuntime(suffix, 1, cron),
      createRuntime(suffix, 2, cron),
    ];
    for (const runtime of runtimes) {
      t.after(async () => {
        runtime.finish.resolve();
        try {
          await runtime.scheduler.stop(1000);
        } finally {
          try {
            if (runtime.ownsStream) {
              await runtime.manager.streams
                .delete(runtime.stream)
                .catch((error) => {
                  if (error.code !== 10059) throw error;
                });
            }
          } finally {
            await runtime.nats.close();
          }
        }
      });
    }

    for (const runtime of runtimes) {
      const { scheduler, nats, stream, declaration, execute } = runtime;
      await scheduler.initialize();
      const { manager } = await nats.getJetStream();
      runtime.manager = manager;
      await assert.rejects(manager.streams.info(stream), { code: 10059 });
      runtime.ownsStream = true;
      await scheduler.synchronize([declaration], execute);
      const binding = scheduler.createBinding(declaration);
      const query = { last_by_subj: binding.scheduleSubject };
      const schedule = await manager.streams.getMessage(stream, query);
      const consumer = await manager.consumers.info(
        stream,
        binding.consumerName,
      );
      assert.ok(schedule);

      await scheduler.synchronize([declaration], execute);
      const reused = await manager.streams.getMessage(stream, query);
      const durable = await manager.consumers.info(
        stream,
        binding.consumerName,
      );
      assert.strictEqual(reused.seq, schedule.seq);
      assert.strictEqual(durable.created, consumer.created);
      declaration.cron = '* * * * * *';
      await scheduler.synchronize([declaration], execute);
    }

    for (const runtime of runtimes) runtime.scheduler.start();
    for (const runtime of runtimes) {
      const job = await runtime.retried.promise;
      const first = runtime.attempts[0];
      assert.strictEqual(first.retryCount, 0);
      assert.strictEqual(job.retryCount, 1);
      assert.strictEqual(job.id, first.id);
      assert.deepStrictEqual(job.data, { stream: runtime.stream });
      const sequence = Number(job.id.slice(runtime.stream.length + 1));
      const pending = await runtime.manager.streams.getMessage(runtime.stream, {
        seq: sequence,
      });
      assert.ok(pending, 'Task must remain stored until execution succeeds');
      runtime.finish.resolve();
      await waitForRemoval(runtime, job, t.signal);
      const next = await runtime.nextOccurrence.promise;
      assert.notStrictEqual(next.id, first.id);
      assert.strictEqual(next.retryCount, 0);
      assert.deepStrictEqual(next.data, { stream: runtime.stream });
      await waitForRemoval(runtime, next, t.signal);
    }
  },
);
