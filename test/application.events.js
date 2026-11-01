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
const {
  EventPublisher,
  EventLoader,
  SubscriberLoader,
  SubscriptionManager,
} = require('../lib/events/index.js');
const application = require('../lib/application.js');

const createConfig = (manageEvents = false) => ({
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
    },
    events: manageEvents,
    nats: {
      enabled: true,
      discovery: {
        maxWait: 100,
      },
    },
  },
  sessions: {},
});

const initialize = (
  kind = 'worker',
  manageEvents = false,
  discoveryLoader = false,
) => {
  wt.workerData.discoveryLoader = discoveryLoader;
  application.kind = kind;
  application.config = createConfig(manageEvents);
  application.console = console;
  application.initializeMessaging();
};

test('application events - initializes transports by worker kind', () => {
  for (const kind of ['server', 'worker', 'balancer']) {
    initialize(kind);

    const { nats, eventPublisher, subscriptions, events, subscribers } =
      application;
    assert.strictEqual(nats instanceof Nats, true);
    assert.strictEqual(eventPublisher instanceof EventPublisher, true);
    assert.strictEqual(subscriptions instanceof SubscriptionManager, true);
    assert.strictEqual(events instanceof EventLoader, true);
    assert.strictEqual(subscribers instanceof SubscriberLoader, true);
    assert.strictEqual(
      subscriptions.transport.nats,
      kind === 'server' ? nats : null,
    );
    assert.strictEqual(subscriptions.transport.managesTopology, false);
    assert.strictEqual(events.subscriptions, subscriptions);
    assert.strictEqual(subscribers.subscriptions, subscriptions);
    assert.strictEqual(eventPublisher.outbox, null);
  }
});

test('application events - consumer management ignores discovery', () => {
  const cases = [
    ['server', false, false, false],
    ['server', false, true, false],
    ['server', true, false, true],
    ['server', true, true, true],
    ['worker', true, true, false],
    ['balancer', true, true, false],
  ];
  for (const [kind, active, loader, expected] of cases) {
    initialize(kind, active, loader);
    assert.strictEqual(
      application.subscriptions.transport.managesTopology,
      expected,
    );
  }
  wt.workerData.discoveryLoader = false;
});

test('application events - subscribers configure initial delivery', () => {
  initialize('server');
  const names = {
    name: 'feed:1:message:created',
    path: 'feed/1/messageCreated',
  };
  const declaration = {
    stream: 'EVENTS',
    subject: 'events.chat.1.message.created',
    method: async () => {},
  };
  const transport = application.subscriptions.transport;
  const defaultBinding = transport.createBinding(
    SubscriberLoader.compile(declaration, names),
  );
  assert.strictEqual(defaultBinding.options.deliver_policy, 'new');
  assert.strictEqual(defaultBinding.options.opt_start_seq, undefined);
  assert.strictEqual(defaultBinding.options.opt_start_time, undefined);
  const cases = [
    { deliverPolicy: 'all' },
    { deliverPolicy: 'new' },
    { deliverPolicy: 'last' },
    { deliverPolicy: 'last_per_subject' },
    { deliverPolicy: 'by_start_sequence', startSequence: 200 },
    { deliverPolicy: 'by_start_time', startTime: '2026-10-31T00:00:00Z' },
  ];
  for (const options of cases) {
    const contract = SubscriberLoader.compile(
      { ...declaration, ...options },
      names,
    );
    const binding = transport.createBinding(contract);
    assert.strictEqual(binding.consumerName, defaultBinding.consumerName);
    assert.strictEqual(binding.options.deliver_policy, options.deliverPolicy);
    assert.strictEqual(binding.options.opt_start_seq, options.startSequence);
    assert.strictEqual(binding.options.opt_start_time, options.startTime);
  }
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

test('application events - configures outbox through sandbox', async () => {
  initialize();
  application.createSandbox();
  const calls = [];
  application.sandbox.events.configure({
    database: { query: async (...args) => calls.push(args) },
    schema: 'appEvents',
    table: 'eventOutbox',
  });
  application.eventPublisher.registerEvent({
    eventName: 'chat:1:message:created',
    eventSubject: 'events.chat.1.message.created',
  });

  const id = await application.sandbox.events.emit('chat:1:message:created', {
    messageId: 42,
  });

  assert.strictEqual(calls.length, 1);
  assert.ok(calls[0][0].startsWith('INSERT INTO "appEvents"."eventOutbox"'));
  assert.strictEqual(calls[0][1][0], id);
});

test('application events - follows lifecycle order', async (t) => {
  initialize();
  application.mode = 'prod';
  application.createSandbox();
  wt.workerData.kind = 'worker';

  const calls = [];
  t.mock.method(application.nats, 'start', async () => {
    calls.push('nats.start');
  });
  t.mock.method(application.subscriptions, 'start', async () => {
    calls.push('subscriptions.start');
  });
  t.mock.method(application.mq, 'start', async () => {
    calls.push('mq.start');
  });

  await application.start();

  assert.deepStrictEqual(calls, [
    'nats.start',
    'subscriptions.start',
    'mq.start',
  ]);

  calls.length = 0;
  const entered = Promise.withResolvers();
  const drained = Promise.withResolvers();
  t.mock.method(application.subscriptions, 'stop', async () => {
    calls.push('subscriptions.stop');
  });
  t.mock.method(application.mq, 'stop', async () => {
    calls.push('mq.stop');
    entered.resolve();
    await drained.promise;
  });
  t.mock.method(application.nats, 'close', async () => {
    calls.push('nats.close');
  });
  const dependencies = [
    'modules',
    'domain',
    'integration',
    'tasks',
    'db',
    'lib',
  ];
  for (const name of dependencies) {
    t.mock.method(application[name], 'stop', async () => {
      calls.push(name + '.stop');
    });
  }

  const closing = application.shutdown();
  await entered.promise;
  try {
    assert.deepStrictEqual(calls, ['subscriptions.stop', 'mq.stop']);
  } finally {
    drained.resolve();
  }
  await closing;

  assert.deepStrictEqual(calls, [
    'subscriptions.stop',
    'mq.stop',
    'nats.close',
    ...dependencies.map((name) => name + '.stop'),
  ]);
});

test('application events - load failure prevents startup', async (t) => {
  initialize();
  t.mock.method(application, 'initializeMessaging', () => {});
  t.mock.method(application, 'startWatch', () => {});
  t.mock.method(application.console, 'error', () => {});
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
});
