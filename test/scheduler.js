'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { Scheduler } = require('../lib/scheduler.js');
const { declarations } = require('../lib/tasks.js');

const createDeclarations = (options = {}) =>
  declarations({
    reports: {
      daily: {
        cron: '30 0 * * *',
        tz: 'Europe/Moscow',
        data: { date: '2023-06-16' },
        method: async () => {},
        ...options,
      },
    },
  });

const createNats = () => {
  const streams = new Map();
  const calls = [];
  const manager = {
    streams: {
      info: async (name, options) => {
        const stream = streams.get(name);
        if (!stream) {
          throw Object.assign(new Error('Stream not found'), { code: 10059 });
        }
        const filter = options?.subjects_filter?.slice(0, -1);
        const subjects = {};
        for (const subject of stream.schedules.keys()) {
          if (!filter || subject.startsWith(filter)) subjects[subject] = 1;
        }
        return { config: stream.config, state: { subjects } };
      },
      add: async (config) => {
        calls.push(['stream.add', config]);
        const stream = streams.get(config.name);
        if (stream) {
          assert.deepStrictEqual(config, stream.config);
          return;
        }
        streams.set(config.name, {
          config,
          schedules: new Map(),
          consumers: new Map(),
        });
      },
      getMessage: async (stream, query) =>
        streams.get(stream).schedules.get(query.last_by_subj) ?? null,
      purge: async (stream, options) => {
        calls.push(['purge', options.filter]);
        streams.get(stream).schedules.delete(options.filter);
      },
    },
    consumers: {
      info: async (stream, name) => {
        const config = streams.get(stream).consumers.get(name);
        if (!config) {
          throw Object.assign(new Error('Consumer not found'), { code: 10014 });
        }
        return { config };
      },
      add: async (stream, config) => {
        calls.push(['consumer.add', config]);
        const { consumers } = streams.get(stream);
        const current = consumers.get(config.durable_name);
        if (current) assert.deepStrictEqual(config, current);
        consumers.set(config.durable_name, config);
      },
      update: async (stream, name, changes) => {
        calls.push(['consumer.update', name, changes]);
        Object.assign(streams.get(stream).consumers.get(name), changes);
      },
    },
  };
  const client = {
    publish: async (subject, payload, options) => {
      calls.push(['publish', subject, payload, options]);
      const matching = [...streams.values()].filter((stream) =>
        stream.config.subjects.some((pattern) =>
          subject.startsWith(pattern.slice(0, -1)),
        ),
      );
      assert.strictEqual(matching.length, 1);
      const { schedules } = matching[0];
      const { specification, target, timezone } = options.schedule;
      const header = new Map([
        ['Nats-Schedule', specification.cron],
        ['Nats-Schedule-Target', target],
        ['Nats-Schedule-Time-Zone', timezone],
      ]);
      schedules.set(subject, { header, json: () => JSON.parse(payload) });
    },
    consumers: {
      get: async (stream, name) => ({
        consume: async (options) => {
          calls.push(['consume', name, options]);
          let finish;
          const ended = new Promise((resolve) => {
            finish = resolve;
          });
          return {
            stop: () => finish(),
            close: async () => finish(),
            [Symbol.asyncIterator]: () => ({
              next: async () => {
                await ended;
                return { done: true };
              },
            }),
          };
        },
      }),
    },
  };
  return {
    application: {
      config: { server: { nats: { jetstream: { replicas: 3 } } } },
    },
    getJetStream: async () => ({ client, manager }),
    streams,
    calls,
  };
};

const createScheduler = (nats, options = {}) =>
  new Scheduler({ enabled: true, active: true, ...options }, nats);

test('scheduler - prepares cron and waits for workers', async () => {
  const nats = createNats();
  const scheduler = createScheduler(nats);
  await scheduler.synchronize(createDeclarations(), async () => {});
  assert.deepStrictEqual(nats.calls[0], [
    'stream.add',
    {
      name: 'TASKS',
      subjects: ['tasks.TASKS.>'],
      storage: 'file',
      retention: 'workqueue',
      num_replicas: 3,
      allow_msg_schedules: true,
    },
  ]);
  const publish = nats.calls.find(([name]) => name === 'publish');
  assert.deepStrictEqual(publish.slice(1), [
    'tasks.TASKS.impress.schedule.reports:daily',
    '{"date":"2023-06-16"}',
    {
      schedule: {
        specification: { cron: '0 30 0 * * *' },
        target: 'tasks.TASKS.impress.run.reports:daily',
        timezone: 'Europe/Moscow',
      },
    },
  ]);
  const config = nats.streams
    .get('TASKS')
    .consumers.get('impress|tasks:reports:daily');
  assert.strictEqual(
    config.filter_subject,
    'tasks.TASKS.impress.run.reports:daily',
  );
  assert.strictEqual(config.deliver_policy, 'all');
  assert.strictEqual(config.max_deliver, 3);
  assert.strictEqual(config.ack_wait, 31_000_000_000);
  assert.strictEqual(
    nats.calls.some(([name]) => name === 'consume'),
    false,
  );
  scheduler.start();
  assert.deepStrictEqual(nats.calls.at(-1), [
    'consume',
    'impress|tasks:reports:daily',
    { max_messages: 5 },
  ]);
  await scheduler.stop();
  assert.strictEqual(nats.streams.get('TASKS').schedules.size, 1);
  assert.strictEqual(nats.streams.get('TASKS').consumers.size, 1);
});

test('scheduler - reuses schedules between instances', async () => {
  const nats = createNats();
  const first = createScheduler(nats);
  const second = createScheduler(nats);
  const tasks = createDeclarations();
  await first.synchronize(tasks, async () => {});
  await second.synchronize(tasks, async () => {});
  await first.synchronize(tasks, async () => {});
  assert.strictEqual(nats.streams.get('TASKS').schedules.size, 1);
  assert.strictEqual(nats.streams.get('TASKS').consumers.size, 1);
  assert.strictEqual(
    nats.calls.filter(([name]) => name === 'publish').length,
    1,
  );
  assert.deepStrictEqual(
    first.createBinding(tasks[0]),
    second.createBinding(tasks[0]),
  );
});

test('scheduler - concurrent startup shares one schedule', async () => {
  const nats = createNats();
  const tasks = createDeclarations();
  await Promise.all([
    createScheduler(nats).synchronize(tasks, async () => {}),
    createScheduler(nats).synchronize(tasks, async () => {}),
  ]);
  assert.strictEqual(nats.streams.get('TASKS').schedules.size, 1);
  assert.strictEqual(nats.streams.get('TASKS').consumers.size, 1);
});

test('scheduler - updates schedules and preserves the durable', async () => {
  const nats = createNats();
  const scheduler = createScheduler(nats);
  await scheduler.synchronize(createDeclarations(), async () => {});
  const original = nats.streams
    .get('TASKS')
    .consumers.get('impress|tasks:reports:daily');
  const tasks = createDeclarations({
    cron: '15 0 1 * * *',
    tz: 'UTC',
    data: { date: '2023-06-17' },
    retryLimit: 5,
    timeout: 60000,
  });
  await scheduler.synchronize(tasks, async () => {});
  const schedule = nats.streams
    .get('TASKS')
    .schedules.get('tasks.TASKS.impress.schedule.reports:daily');
  assert.strictEqual(schedule.header.get('Nats-Schedule'), '15 0 1 * * *');
  assert.strictEqual(schedule.header.get('Nats-Schedule-Time-Zone'), 'UTC');
  assert.deepStrictEqual(schedule.json(), { date: '2023-06-17' });
  assert.strictEqual(
    nats.streams.get('TASKS').consumers.get('impress|tasks:reports:daily'),
    original,
  );
  assert.strictEqual(original.max_deliver, 6);
  assert.strictEqual(original.ack_wait, 61_000_000_000);
});

test('scheduler - passive instances only consume', async () => {
  const nats = createNats();
  const owner = createScheduler(nats);
  await owner.synchronize(createDeclarations(), async () => {});
  const scheduler = createScheduler(nats, { active: false });
  nats.calls.length = 0;
  const tasks = createDeclarations({ cron: '0 8 * * *' });
  await scheduler.synchronize(tasks, async () => {});
  scheduler.start();
  assert.deepStrictEqual(
    nats.calls.map(([name]) => name),
    ['consume'],
  );
  await scheduler.synchronize([], async () => {});
  assert.strictEqual(nats.streams.get('TASKS').schedules.size, 1);
  assert.strictEqual(nats.streams.get('TASKS').consumers.size, 1);
  await scheduler.stop();
});

test('scheduler - removes only its own schedules', async () => {
  const nats = createNats();
  const scheduler = createScheduler(nats);
  const other = createScheduler(nats, { consumerPrefix: 'other' });
  await scheduler.synchronize(createDeclarations(), async () => {});
  await other.synchronize(createDeclarations(), async () => {});
  await scheduler.synchronize([], async () => {});
  assert.deepStrictEqual(
    [...nats.streams.get('TASKS').schedules.keys()],
    ['tasks.TASKS.other.schedule.reports:daily'],
  );
  assert.strictEqual(nats.streams.get('TASKS').consumers.size, 2);
  assert.strictEqual(scheduler.consumers.size, 0);
});

test('scheduler - isolates tasks in different streams', async () => {
  const nats = createNats();
  const first = createScheduler(nats);
  const second = createScheduler(nats, { stream: 'OTHER_TASKS' });
  const tasks = createDeclarations();
  await first.synchronize(tasks, async () => {});
  await second.synchronize(tasks, async () => {});

  const firstStream = nats.streams.get('TASKS');
  const secondStream = nats.streams.get('OTHER_TASKS');
  assert.strictEqual(nats.streams.size, 2);
  assert.deepStrictEqual(firstStream.config.subjects, ['tasks.TASKS.>']);
  assert.deepStrictEqual(secondStream.config.subjects, ['tasks.OTHER_TASKS.>']);
  assert.deepStrictEqual(
    [...firstStream.schedules.keys()],
    ['tasks.TASKS.impress.schedule.reports:daily'],
  );
  assert.deepStrictEqual(
    [...secondStream.schedules.keys()],
    ['tasks.OTHER_TASKS.impress.schedule.reports:daily'],
  );
  const consumerName = 'impress|tasks:reports:daily';
  const firstConsumer = firstStream.consumers.get(consumerName);
  const secondConsumer = secondStream.consumers.get(consumerName);
  assert.strictEqual(
    firstConsumer.filter_subject,
    'tasks.TASKS.impress.run.reports:daily',
  );
  assert.strictEqual(
    secondConsumer.filter_subject,
    'tasks.OTHER_TASKS.impress.run.reports:daily',
  );

  await first.synchronize([], async () => {});
  assert.strictEqual(firstStream.schedules.size, 0);
  assert.strictEqual(first.consumers.size, 0);
  assert.strictEqual(secondStream.schedules.size, 1);
  assert.strictEqual(second.consumers.size, 1);
  assert.strictEqual(secondStream.consumers.get(consumerName), secondConsumer);
});

test('scheduler - preserves existing stream replicas', async () => {
  const nats = createNats();
  await createScheduler(nats).synchronize([], async () => {});
  await createScheduler(nats, { replicas: 1 }).synchronize([], async () => {});
  const creations = nats.calls.filter(([name]) => name === 'stream.add');
  assert.strictEqual(creations.length, 1);
  const { manager } = await nats.getJetStream();
  const info = await manager.streams.info('TASKS');
  assert.strictEqual(info.config.num_replicas, 3);
});

test('scheduler - disabled mode does not access NATS', async () => {
  const scheduler = new Scheduler();
  await scheduler.synchronize(createDeclarations(), async () => {});
  scheduler.start();
  await scheduler.stop();
  assert.strictEqual(scheduler.consumers.size, 0);
});

test('scheduler - can synchronize again after an API error', async () => {
  const nats = createNats();
  const scheduler = createScheduler(nats);
  const { manager } = await nats.getJetStream();
  const original = manager.streams.info;
  manager.streams.info = async () => {
    throw new Error('Permission denied');
  };
  await assert.rejects(
    scheduler.synchronize(createDeclarations(), async () => {}),
    /Permission denied/,
  );
  assert.strictEqual(nats.streams.size, 0);
  manager.streams.info = original;
  await scheduler.synchronize(createDeclarations(), async () => {});
  assert.strictEqual(nats.streams.get('TASKS').schedules.size, 1);
});
