'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { PgbossSubscriptions } = require('../lib/events/transports.js');
const {
  SubscriberLoader,
  contractNames,
} = require('../lib/events/declarations.js');
const {
  EventPublisher,
  NATS_EVENT_QUEUE,
} = require('../lib/events/publisher.js');

const managerPath = path.join(
  path.dirname(require.resolve('pg-boss')),
  'manager.js',
);
const { default: Manager } = require(managerPath);

const subscriberFiles = [
  {
    fileName: path.join('profile', 'handleCreateMessage.js'),
    queueName: 'subscribers/profile/1/handleCreateMessage',
  },
  {
    fileName: path.join('feed.2', 'chat', 'messageRenamed.js'),
    queueName: 'subscribers/feed/2/chat/messageRenamed',
  },
];

for (const { fileName, queueName: expectedQueueName } of subscriberFiles) {
  test(`pgboss subscriber - fetches queue for ${fileName}`, async () => {
    const directory = path.join(process.cwd(), 'test', 'subscribers');
    const names = contractNames(directory, path.join(directory, fileName));
    const contract = SubscriberLoader.compile(
      { event: 'chat:1:message:created', method: async () => {} },
      names,
    );
    const adapter = new PgbossSubscriptions(null);
    const { queueName } = adapter.createBinding(contract);
    assert.strictEqual(queueName, expectedQueueName);
    const queue = { name: queueName, table: 'job', policy: 'standard' };
    const queries = [];
    const db = {
      async executeSql(text, values) {
        queries.push({ text, values });
        return { rows: queries.length === 1 ? [queue] : [] };
      },
    };
    const manager = new Manager(db, { schema: 'pgboss' });

    const jobs = await manager.fetch(queueName);

    assert.deepStrictEqual(jobs, []);
    assert.strictEqual(queries.length, 2);
    assert.strictEqual(contract.eventName, 'chat:1:message:created');
  });
}

for (const kind of ['subscriber', 'publisher']) {
  test(`pgboss ${kind} - waits for handlers on stop`, async () => {
    const manager = new Manager({}, {});
    const pending = Promise.withResolvers();
    const entered = Promise.withResolvers();
    const name = kind === 'publisher' ? NATS_EVENT_QUEUE : 'subscriber';
    manager.workers.set('worker-1', {
      id: 'worker-1',
      workId: 'worker-1',
      name,
      stopping: false,
      stopped: false,
      async stop() {
        entered.resolve();
        await pending.promise;
      },
    });
    manager.work = async () => 'worker-1';
    manager.getQueue = async () => ({ name });
    const pgboss = { client: manager };
    let handle;
    if (kind === 'publisher') {
      handle = new EventPublisher(pgboss);
      handle.workId = 'worker-1';
    } else {
      const adapter = new PgbossSubscriptions(pgboss);
      handle = await adapter.bind(
        { queueName: name, workOptions: {} },
        async () => {},
      );
    }
    let stopped = false;
    const stopping = handle.stop().then(() => {
      stopped = true;
    });
    await entered.promise;
    await new Promise((resolve) => setImmediate(resolve));
    try {
      assert.strictEqual(stopped, false);
    } finally {
      pending.resolve();
      await stopping;
    }
    assert.strictEqual(manager.workers.size, 0);
  });
}
