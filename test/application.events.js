'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const wt = require('node:worker_threads');

const root = process.cwd();
wt.workerData = {
  id: 0,
  kind: 'worker',
  root,
  path: root,
  port: 8000,
  discoveryLoader: false,
};

const { Nats } = require('../lib/nats.js');
const { Pgboss } = require('../lib/pgboss.js');
const {
  EventPublisher,
  EventLoader,
  SubscriberLoader,
  SubscriptionManager,
} = require('../lib/events/index.js');
const application = require('../lib/application.js');

const createConfig = (subscriptionsActive = false) => ({
  server: {
    host: '127.0.0.1',
    protocol: 'http',
    queue: {
      concurrency: 1,
      size: 10,
      timeout: 1000,
    },
    timeouts: {
      stop: 5000,
    },
    scheduler: {
      enabled: true,
      notify: true,
    },
    pubsub: {
      active: subscriptionsActive,
    },
    nats: {
      enabled: true,
      discovery: {
        maxWait: 100,
      },
    },
    pgboss: {
      enabled: true,
    },
  },
  sessions: {},
});

const initialize = (
  kind = 'worker',
  subscriptionsActive = false,
  discoveryLoader = false,
) => {
  wt.workerData.discoveryLoader = discoveryLoader;
  application.kind = kind;
  application.config = createConfig(subscriptionsActive);
  application.console = console;
  application.initializeMessaging();
};

test('application events - initializes transports by worker kind', () => {
  for (const kind of ['server', 'worker', 'balancer']) {
    initialize(kind);

    const { pgboss, nats, eventPublisher, subscriptions, events, subscribers } =
      application;
    assert.strictEqual(pgboss instanceof Pgboss, true);
    assert.strictEqual(nats instanceof Nats, true);
    assert.strictEqual(eventPublisher instanceof EventPublisher, true);
    assert.strictEqual(subscriptions instanceof SubscriptionManager, true);
    assert.strictEqual(events instanceof EventLoader, true);
    assert.strictEqual(subscribers instanceof SubscriberLoader, true);
    assert.strictEqual(eventPublisher.pgboss, pgboss);
    assert.strictEqual(eventPublisher.nats, nats);
    assert.strictEqual(subscriptions.local.pgboss, pgboss);
    assert.strictEqual(
      subscriptions.nats.nats,
      kind === 'server' ? nats : null,
    );
    assert.strictEqual(subscriptions.local.managesTopology, false);
    assert.strictEqual(events.subscriptions, subscriptions);
    assert.strictEqual(subscribers.subscriptions, subscriptions);
    assert.strictEqual(eventPublisher.notify, true);
  }
});

test('application events - selects one active topology worker', () => {
  const cases = [
    [false, false, false],
    [false, true, false],
    [true, false, false],
    [true, true, true],
  ];
  for (const [active, loader, expected] of cases) {
    initialize('worker', active, loader);
    assert.strictEqual(
      application.subscriptions.local.managesTopology,
      expected,
    );
  }
  wt.workerData.discoveryLoader = false;
});

test('application events - forwards sandbox events.emit', async (t) => {
  initialize();
  const data = { messageId: 42 };
  const transaction = { id: 'transaction-1' };
  const options = { transaction };
  const emit = t.mock.method(
    application.eventPublisher,
    'emit',
    async () => 'event-1',
  );

  application.createSandbox();
  const id = await application.sandbox.events.emit(
    'chat:1:message:created',
    data,
    options,
  );

  assert.strictEqual(id, 'event-1');
  assert.strictEqual(emit.mock.callCount(), 1);
  const [call] = emit.mock.calls;
  assert.strictEqual(call.arguments[0], 'chat:1:message:created');
  assert.strictEqual(call.arguments[1], data);
  assert.strictEqual(call.arguments[2], options);
  assert.strictEqual(call.arguments[2].transaction, transaction);
});

test('application events - follows lifecycle order', async (t) => {
  initialize();
  application.mode = 'prod';
  application.createSandbox();
  wt.workerData.kind = 'worker';

  const calls = [];
  t.mock.method(application.pgboss, 'start', async () => {
    calls.push('pgboss.start');
  });
  t.mock.method(application.nats, 'start', async () => {
    calls.push('nats.start');
  });
  t.mock.method(application.eventPublisher, 'start', async () => {
    calls.push('events.start');
  });
  t.mock.method(application.subscriptions, 'start', async () => {
    calls.push('subscriptions.start');
  });

  await application.start();

  assert.deepStrictEqual(calls, [
    'pgboss.start',
    'nats.start',
    'events.start',
    'subscriptions.start',
  ]);

  calls.length = 0;
  t.mock.method(application.subscriptions, 'stop', async () => {
    calls.push('subscriptions.stop');
  });
  t.mock.method(application.eventPublisher, 'stop', async () => {
    calls.push('events.stop');
  });
  t.mock.method(application.nats, 'close', async () => {
    calls.push('nats.close');
  });
  t.mock.method(application.pgboss, 'stop', async (timeout) => {
    calls.push(['pgboss.stop', timeout]);
  });
  for (const target of [
    application.modules,
    application.domain,
    application.integration,
    application.tasks,
    application.mq,
    application.db,
    application.lib,
  ]) {
    t.mock.method(target, 'stop', async () => {});
  }

  await application.shutdown();

  assert.deepStrictEqual(calls, [
    'subscriptions.stop',
    'events.stop',
    'nats.close',
    ['pgboss.stop', 5000],
  ]);
});

test('application events - load failure prevents queue cleanup', async (t) => {
  initialize();
  t.mock.method(application, 'initializeMessaging', () => {});
  t.mock.method(application, 'startWatch', () => {});
  t.mock.method(application.console, 'error', () => {});
  const cleanup = t.mock.method(
    application.subscriptions,
    'removeStaleQueues',
    async () => {},
  );
  const starting = t.mock.method(application, 'start', async () => {});
  for (const name of [
    'static',
    'resources',
    'cert',
    'schemas',
    'lib',
    'db',
    'bus',
    'tasks',
    'events',
    'integration',
    'domain',
    'modules',
  ]) {
    t.mock.method(application[name], 'load', async () => {});
  }
  t.mock.method(application.subscribers, 'load', async () => {
    throw new Error('Invalid subscriber declaration');
  });

  await assert.rejects(
    application.load({ invoke: async () => {} }),
    /initialize an Application/,
  );
  assert.strictEqual(starting.mock.callCount(), 0);
  assert.strictEqual(cleanup.mock.callCount(), 0);
});
