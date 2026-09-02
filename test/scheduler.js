'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { Pgboss } = require('../lib/pgboss.js');
const { Scheduler, TASK_NAMESPACE } = require('../lib/scheduler.js');

class PgbossClient {
  constructor() {
    this.calls = [];
    this.queues = new Set();
    this.schedules = [];
    this.consumers = new Map();
    this.lastWorkId = 0;
  }

  async getQueue(name) {
    this.calls.push(['getQueue', name]);
    return this.queues.has(name) ? { name } : null;
  }

  async createQueue(name) {
    this.calls.push(['createQueue', name]);
    this.queues.add(name);
  }

  async schedule(name, cron, data, options) {
    this.calls.push(['schedule', name, cron, data, options]);
  }

  async unschedule(...args) {
    this.calls.push(['unschedule', ...args]);
  }

  async getSchedules() {
    this.calls.push(['getSchedules']);
    return this.schedules;
  }

  async work(name, options, handler) {
    const id = `work-${++this.lastWorkId}`;
    this.calls.push(['work', name, options, id]);
    this.consumers.set(name, { id, handler });
    return id;
  }

  async offWork(name, options) {
    this.calls.push(['offWork', name, options]);
    this.consumers.delete(name);
  }
}

test('lib/scheduler - should use shared pgboss', () => {
  const pgboss = new Pgboss({ enabled: false, schedule: true });
  const scheduler = new Scheduler({ enabled: true, active: true }, pgboss);

  assert.strictEqual(scheduler.enabled, true);
  assert.strictEqual(scheduler.active, true);
  assert.strictEqual(TASK_NAMESPACE, 'tasks/');
  assert.strictEqual(scheduler.pgboss, pgboss);
  assert.strictEqual(scheduler.client, null);
  assert.strictEqual(scheduler.declarations.size, 0);
  assert.strictEqual(scheduler.consumers.size, 0);
  assert.strictEqual(scheduler.start, undefined);
  assert.strictEqual(scheduler.stop, undefined);
});

test('lib/scheduler - should be disabled by default', () => {
  const scheduler = new Scheduler();

  assert.strictEqual(scheduler.enabled, false);
  assert.strictEqual(scheduler.active, false);
  assert.strictEqual(scheduler.pgboss, null);
  assert.strictEqual(scheduler.client, null);
});

test('lib/scheduler - should register declarations', async () => {
  const client = new PgbossClient();
  const scheduler = new Scheduler({ enabled: true, active: true }, { client });
  const declarations = [
    {
      name: `${TASK_NAMESPACE}cleanup`,
      cron: '0 3 * * *',
      data: { automatic: true },
      options: { tz: 'Europe/Moscow' },
    },
  ];

  await scheduler.register(declarations);

  assert.deepStrictEqual(client.calls, [
    ['getQueue', `${TASK_NAMESPACE}cleanup`],
    ['createQueue', `${TASK_NAMESPACE}cleanup`],
    [
      'schedule',
      `${TASK_NAMESPACE}cleanup`,
      '0 3 * * *',
      { automatic: true },
      { tz: 'Europe/Moscow' },
    ],
  ]);
  assert.strictEqual(
    scheduler.declarations.get(`${TASK_NAMESPACE}cleanup`),
    declarations[0],
  );
});

test('lib/scheduler - should unregister declarations', async () => {
  const client = new PgbossClient();
  const scheduler = new Scheduler({ enabled: true, active: true }, { client });
  scheduler.declarations.set(`${TASK_NAMESPACE}cleanup`, {});

  await scheduler.unregister([`${TASK_NAMESPACE}cleanup`]);

  assert.deepStrictEqual(client.calls, [
    ['unschedule', `${TASK_NAMESPACE}cleanup`],
  ]);
  assert.strictEqual(
    scheduler.declarations.has(`${TASK_NAMESPACE}cleanup`),
    false,
  );
});

test('lib/scheduler - should synchronize declarations', async () => {
  const client = new PgbossClient();
  client.queues.add(`${TASK_NAMESPACE}cleanup`);
  client.schedules = [
    {
      name: `${TASK_NAMESPACE}cleanup`,
      cron: '0 3 * * *',
      data: {},
      options: {},
    },
    { name: `${TASK_NAMESPACE}removed`, key: '' },
    { name: 'mq/cleanup', key: '' },
  ];
  const scheduler = new Scheduler({ enabled: true, active: true }, { client });
  const declarations = [
    { name: `${TASK_NAMESPACE}cleanup`, cron: '0 3 * * *' },
  ];

  await scheduler.synchronize(declarations);

  assert.strictEqual(
    client.calls.some((call) => call[0] === 'schedule'),
    false,
  );
  assert.strictEqual(
    client.calls.some((call) => call[0] === 'getQueue'),
    false,
  );
  assert.deepStrictEqual(
    client.calls.find((call) => call[0] === 'unschedule'),
    ['unschedule', `${TASK_NAMESPACE}removed`],
  );
  assert.strictEqual(
    client.calls.some(
      (call) => call[0] === 'unschedule' && call[1] === 'mq/cleanup',
    ),
    false,
  );
  assert.deepStrictEqual(
    [...scheduler.declarations.keys()],
    [`${TASK_NAMESPACE}cleanup`],
  );
});

test('lib/scheduler - should update changed schedule', async () => {
  const client = new PgbossClient();
  const name = `${TASK_NAMESPACE}cleanup`;
  client.schedules = [{ name, cron: '0 2 * * *', data: {}, options: {} }];
  const scheduler = new Scheduler({ enabled: true, active: true }, { client });
  const declarations = [{ name, cron: '0 3 * * *' }];

  await scheduler.synchronize(declarations);

  assert.deepStrictEqual(client.calls, [
    ['getSchedules'],
    ['schedule', name, '0 3 * * *', {}, {}],
  ]);
});

test('lib/scheduler - should remove all application schedules', async () => {
  const client = new PgbossClient();
  client.schedules = [
    { name: `${TASK_NAMESPACE}cleanup`, key: '' },
    { name: 'mq/cleanup', key: '' },
  ];
  const scheduler = new Scheduler({ enabled: true, active: true }, { client });

  await scheduler.synchronize([]);

  assert.deepStrictEqual(
    client.calls.find((call) => call[0] === 'unschedule'),
    ['unschedule', `${TASK_NAMESPACE}cleanup`],
  );
  assert.strictEqual(scheduler.declarations.size, 0);
});

test('lib/scheduler - should do nothing when disabled', async () => {
  const client = new PgbossClient();
  const scheduler = new Scheduler({ enabled: false, active: true }, { client });
  const declarations = [{ name: 'cleanup', cron: '0 3 * * *' }];
  const execute = async () => {};

  await scheduler.register(declarations);
  await scheduler.unregister(['cleanup']);
  await scheduler.synchronize(declarations, execute);

  assert.deepStrictEqual(client.calls, []);
  assert.strictEqual(scheduler.declarations.size, 0);
  assert.strictEqual(scheduler.consumers.size, 0);
});

test('lib/scheduler - should consume jobs while inactive', async () => {
  const client = new PgbossClient();
  const scheduler = new Scheduler({ enabled: true, active: false }, { client });
  const declaration = {
    name: 'tasks/cleanup',
    cron: '0 3 * * *',
    worker: { localConcurrency: 2 },
    path: 'cleanup',
  };
  const executions = [];
  const execute = async (active, job) => {
    executions.push([active.path, job.id, 'signal' in job]);
    return { completed: true };
  };

  await scheduler.synchronize([declaration], execute);

  assert.deepStrictEqual(client.calls, [
    ['getQueue', 'tasks/cleanup'],
    ['createQueue', 'tasks/cleanup'],
    [
      'work',
      'tasks/cleanup',
      { localConcurrency: 2, includeMetadata: true },
      'work-1',
    ],
  ]);
  const consumer = client.consumers.get('tasks/cleanup');
  const job = { id: 'job-1', data: {}, signal: new AbortController().signal };
  const result = await consumer.handler([job]);
  assert.deepStrictEqual(result, { completed: true });
  assert.deepStrictEqual(executions, [['cleanup', 'job-1', false]]);

  await scheduler.synchronize([declaration], execute);
  assert.strictEqual(client.calls.length, 3);

  const changed = { ...declaration, worker: { localConcurrency: 3 } };
  await scheduler.synchronize([changed], execute);
  assert.deepStrictEqual(client.calls.slice(3), [
    ['offWork', 'tasks/cleanup', { id: 'work-1' }],
    ['getQueue', 'tasks/cleanup'],
    [
      'work',
      'tasks/cleanup',
      { localConcurrency: 3, includeMetadata: true },
      'work-2',
    ],
  ]);

  await scheduler.synchronize([], execute);
  assert.deepStrictEqual(client.calls[6], [
    'offWork',
    'tasks/cleanup',
    { id: 'work-2' },
  ]);
  assert.strictEqual(scheduler.consumers.size, 0);
});
