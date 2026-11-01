'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const fsp = require('node:fs/promises');
const metavm = require('metavm');
const {
  EventLoader,
  SubscriberLoader,
  SubscriptionManager,
} = require('../lib/events/index.js');
const { contractNames } = require('../lib/events/declarations.js');

const createApplication = () => ({
  path: path.join(process.cwd(), 'test'),
  console: { error() {} },
  sandbox: metavm.createContext({}),
  watcher: { watch() {} },
  absolute(relative) {
    return path.join(this.path, relative);
  },
});

const createEvent = () => ({
  eventName: 'chat:1:message:created',
  eventSubject: 'events.chat.1.message.created',
});

const createSubscriber = (options = {}) => ({
  subscriberName: 'feed:1:message:created',
  stream: 'EVENTS',
  subject: 'events.chat.1.message.created',
  method: async () => {},
  ...options,
});

const createManager = () => {
  const calls = [];
  const emitter = {
    registerEvent: (event) => calls.push(['registerEvent', event]),
    unregisterEvent: (name) => calls.push(['unregisterEvent', name]),
  };
  const transport = {
    enabled: true,
    available: true,
    managesTopology: true,
    ensureStream: async () => transport.available,
    createBinding: (contract) => ({
      subscriberName: contract.subscriberName,
      stream: contract.stream,
      subject: contract.subject,
      maxMessages: contract.maxMessages,
      maxAckPending: contract.maxAckPending,
      retryLimit: contract.retryLimit,
      retryDelay: contract.retryDelay,
      timeout: contract.timeout,
      deliverPolicy: contract.deliverPolicy,
    }),
    bind: async (binding, dispatch) => {
      const handle = {
        running: true,
        dispatch,
        stop: async () => {
          calls.push(['stop', binding]);
          handle.running = false;
        },
      };
      calls.push(['bind', binding]);
      return handle;
    },
  };
  const logger = { error: (...args) => calls.push(['error', ...args]) };
  const manager = new SubscriptionManager(emitter, transport, logger);
  return { manager, transport, calls };
};

test('subscriptions - derive names from declaration paths', () => {
  const directory = path.join(process.cwd(), 'test', 'events');
  const file = path.join(directory, 'chat.2', 'messageCreated.js');
  assert.deepStrictEqual(contractNames(directory, file), {
    name: 'chat:2:message:created',
    subject: 'chat.2.message.created',
    path: 'chat/2/messageCreated',
  });
});

test('subscriptions - load event and subscriber declarations', async () => {
  const { manager } = createManager();
  const application = createApplication();
  const events = new EventLoader(application, manager);
  const subscribers = new SubscriberLoader(application, manager);

  await events.load();
  await subscribers.load();

  const event = manager.events['chat:1:message:created'];
  assert.strictEqual(event.eventSubject, 'events.chat.1.message.created');
  assert.deepStrictEqual(manager.describeEvents()[0].transports, ['nats']);
  assert.strictEqual(event.caption, 'Message created');
  const subscriber = manager.subscribers['feed:1:message:created'];
  assert.strictEqual(subscriber.stream, 'EVENTS');
  assert.strictEqual(subscriber.subject, event.eventSubject);
  assert.strictEqual(subscriber.maxMessages, 5);
  assert.strictEqual(subscriber.maxAckPending, 50);
  assert.strictEqual(subscriber.retryLimit, 5);
  assert.strictEqual(subscriber.retryDelay, 1000);
  assert.strictEqual(subscriber.timeout, 30000);

  await subscribers.delete(
    path.join(subscribers.path, 'feed.1', 'messageCreated.js'),
  );
  await events.delete(path.join(events.path, 'chat.1', 'messageCreated.js'));
  assert.deepStrictEqual(Object.keys(manager.subscribers), []);
  assert.deepStrictEqual(Object.keys(manager.events), []);
});

test('subscriptions - register publishers before startup', async () => {
  const { manager, calls } = createManager();
  const event = createEvent();
  await manager.registerEvent(event);
  await manager.registerSubscriber(createSubscriber());
  assert.deepStrictEqual(calls, [['registerEvent', event]]);

  await manager.start();
  assert.strictEqual(manager.active.size, 1);
  await manager.stop();
});

test('subscriptions - idle managing server skips JetStream', async () => {
  const { manager, transport } = createManager();
  transport.ensureStream = () => assert.fail('Unexpected JetStream access');
  await manager.start();
  assert.strictEqual(manager.retryTimer, null);
  assert.strictEqual(manager.active.size, 0);
  await manager.stop();
});

test('subscriptions - idle passive server skips JetStream', async () => {
  const { manager, transport } = createManager();
  transport.managesTopology = false;
  transport.ensureStream = () => assert.fail('Unexpected JetStream access');
  await manager.start();
  assert.strictEqual(manager.retryTimer, null);
  await manager.stop();
});

test('subscriptions - wait for stream before binding consumers', async () => {
  const { manager, transport, calls } = createManager();
  const preparing = Promise.withResolvers();
  const entered = Promise.withResolvers();
  transport.ensureStream = () => {
    entered.resolve();
    return preparing.promise;
  };
  await manager.registerSubscriber(createSubscriber());
  const starting = manager.start();
  await entered.promise;
  assert.deepStrictEqual(calls, []);
  preparing.resolve(false);
  await starting;
  assert.deepStrictEqual(calls, []);
  assert.ok(manager.retryTimer);
  transport.ensureStream = async () => true;
  await manager.synchronize();
  assert.strictEqual(manager.active.size, 1);
  assert.strictEqual(manager.retryTimer, null);
  await manager.stop();
});

test('subscriptions - propagate stream failure before binding', async () => {
  const { manager, transport, calls } = createManager();
  const failure = new Error('Cannot read stream');
  transport.ensureStream = async () => {
    throw failure;
  };
  await manager.registerSubscriber(createSubscriber());
  await assert.rejects(manager.start(), {
    message: 'Failed to reconcile event subscribers',
    cause: failure,
  });
  assert.strictEqual(manager.started, false);
  assert.deepStrictEqual(calls, [['error', failure]]);
  await manager.stop();
});

test('subscriptions - isolates missing streams', async (t) => {
  for (const missingFirst of [true, false]) {
    const { manager, transport } = createManager();
    t.after(() => manager.stop());
    const events = createSubscriber();
    const cdc = createSubscriber({
      subscriberName: 'replica:1:users:changed',
      stream: 'CDC',
      subject: 'cdc.public.users',
    });
    transport.ensureStream = async (stream) => stream === 'EVENTS';
    const contracts = missingFirst ? [cdc, events] : [events, cdc];
    for (const contract of contracts) {
      await manager.registerSubscriber(contract);
    }
    await manager.start();
    assert.strictEqual(manager.active.size, 1);
    assert.ok(manager.active.has(events.subscriberName));
    assert.ok(manager.retryTimer);
    transport.ensureStream = async () => true;
    await manager.synchronize();
    assert.strictEqual(manager.active.size, 2);
    assert.strictEqual(manager.retryTimer, null);
  }
});

test('subscriptions - forwards full JSON and context', async () => {
  const { manager } = createManager();
  let received;
  const subscriber = createSubscriber({
    method: (message, context) => {
      received = { message, context };
      return 'handled';
    },
  });
  await manager.registerSubscriber(subscriber);
  await manager.start();
  try {
    const messages = [
      {
        id: 'event-1',
        name: 'chat:1:message:created',
        createdAt: '2026-10-31T00:00:00.000Z',
        payload: null,
      },
      {
        before: null,
        after: { id: 42 },
        op: 'c',
        source: { table: 'users', lsn: 100 },
      },
      null,
      ['value', 42],
      'message',
      42,
      false,
    ];
    const context = { signal: new AbortController().signal };
    const { handle } = manager.active.get(subscriber.subscriberName);
    for (const message of messages) {
      assert.strictEqual(await handle.dispatch(message, context), 'handled');
      assert.strictEqual(received.message, message);
      assert.strictEqual(received.context, context);
    }
  } finally {
    await manager.stop();
  }
});

test('subscriptions - update a handler without rebinding', async () => {
  const { manager, calls } = createManager();
  const subscriber = createSubscriber();
  await manager.registerSubscriber(subscriber);
  await manager.start();
  let updated = false;
  await manager.registerSubscriber({
    ...subscriber,
    method: () => {
      updated = true;
    },
  });
  const { handle } = manager.active.get(subscriber.subscriberName);
  await handle.dispatch({ payload: {} }, {});
  assert.strictEqual(updated, true);
  assert.strictEqual(calls.filter((call) => call[0] === 'bind').length, 1);
  await manager.stop();
});

test('subscriptions - rebinds when routing or options change', async () => {
  const { manager, calls } = createManager();
  const subscriber = createSubscriber();
  await manager.registerSubscriber(subscriber);
  await manager.start();
  const updated = { ...subscriber, maxMessages: 10 };
  await manager.registerSubscriber(updated);
  const moved = {
    ...updated,
    subject: 'events.profile.1.details.updated',
  };
  await manager.registerSubscriber(moved);
  await manager.registerSubscriber({ ...moved, stream: 'OTHER_EVENTS' });
  assert.deepStrictEqual(
    calls.map((call) => call[0]),
    ['bind', 'stop', 'bind', 'stop', 'bind', 'stop', 'bind'],
  );
  await manager.removeSubscriber(subscriber.subscriberName);
  assert.strictEqual(manager.active.size, 0);
  await manager.stop();
});

test('subscriptions - isolate conflicts during reload', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { manager, transport, calls } = createManager();
  t.after(() => manager.stop());
  const first = createSubscriber({ deliverPolicy: 'new' });
  const second = createSubscriber({
    subscriberName: 'audit:1:message:created',
    method: () => 'old',
  });
  const third = createSubscriber({
    subscriberName: 'search:1:message:created',
    method: () => 'new',
  });
  const conflict = new Error('Conflicting JetStream consumer');
  const bind = transport.bind;
  let failures = 0;
  transport.bind = async (binding, dispatch) => {
    if (
      binding.subscriberName === first.subscriberName &&
      binding.deliverPolicy === 'all'
    ) {
      failures++;
      throw conflict;
    }
    return bind(binding, dispatch);
  };
  await manager.registerSubscriber(first);
  await manager.registerSubscriber(second);
  await manager.start();
  const original = manager.active.get(second.subscriberName).handle;

  await manager.registerSubscriber({ ...first, deliverPolicy: 'all' });
  await manager.registerSubscriber({ ...second, method: () => 'updated' });
  await manager.registerSubscriber(third);
  assert.strictEqual(
    manager.active.get(second.subscriberName).handle,
    original,
  );
  assert.strictEqual(await original.dispatch({ payload: {} }), 'updated');
  const added = manager.active.get(third.subscriberName).handle;
  assert.strictEqual(await added.dispatch({ payload: {} }), 'new');
  assert.ok(manager.retryTimer);
  const error = calls.find(
    (call) => call[1] === 'Failed to synchronize event subscribers',
  )[2];
  assert.strictEqual(error.constructor, Error);
  assert.strictEqual(error.cause, conflict);

  const attempts = failures;
  t.mock.timers.tick(1000);
  await manager.operation;
  assert.strictEqual(failures, attempts + 1);
  assert.ok(manager.retryTimer);
  assert.strictEqual(manager.active.get(third.subscriberName).handle, added);

  await manager.registerSubscriber(first);
  assert.strictEqual(manager.active.size, 3);
  assert.strictEqual(manager.retryTimer, null);
});

test('subscriptions - isolate failed removals', async (t) => {
  const { manager, transport } = createManager();
  const first = createSubscriber();
  const second = createSubscriber({
    subscriberName: 'audit:1:message:created',
  });
  await manager.registerSubscriber(first);
  await manager.registerSubscriber(second);
  await manager.start();
  const retained = manager.active.get(first.subscriberName).handle;
  const removed = manager.active.get(second.subscriberName).handle;
  const stop = retained.stop;
  retained.stop = async () => {
    throw new Error('Cannot drain subscriber');
  };
  t.after(async () => {
    retained.stop = stop;
    await manager.stop();
  });

  await manager.removeSubscriber(first.subscriberName);
  transport.enabled = false;
  transport.ensureStream = () => assert.fail('Unexpected JetStream access');
  await manager.removeSubscriber(second.subscriberName);
  assert.strictEqual(removed.running, false);
  assert.strictEqual(manager.active.size, 1);
  assert.ok(manager.retryTimer);

  transport.enabled = true;
  transport.managesTopology = false;
  await manager.synchronize();
  assert.ok(manager.retryTimer);
  retained.stop = stop;
  await manager.synchronize();
  assert.strictEqual(manager.active.size, 0);
  assert.strictEqual(manager.retryTimer, null);
});

test('subscriptions - wait for connection and consumer', async () => {
  const { manager, transport } = createManager();
  const subscriber = createSubscriber();
  await manager.registerSubscriber(subscriber);
  transport.available = false;
  await manager.start();
  assert.strictEqual(manager.active.size, 0);
  assert.ok(manager.retryTimer);
  transport.available = true;
  const bind = transport.bind;
  transport.bind = async () => null;
  await manager.synchronize();
  assert.strictEqual(manager.active.size, 0);
  transport.bind = bind;
  await manager.synchronize();
  assert.strictEqual(manager.active.size, 1);
  assert.strictEqual(manager.retryTimer, null);
  await manager.stop();
});

test('subscriptions - propagate bind failures at startup', async () => {
  const { manager, transport } = createManager();
  await manager.registerSubscriber(createSubscriber());
  const failure = new Error('JetStream unavailable');
  transport.bind = async () => {
    throw failure;
  };
  await assert.rejects(manager.start(), {
    message: 'Failed to reconcile event subscribers',
    cause: failure,
  });
  assert.strictEqual(manager.started, false);
  await manager.stop();
});

test('subscriptions - clean up bindings after startup failures', async () => {
  const { manager, transport, calls } = createManager();
  const first = createSubscriber();
  const second = createSubscriber({
    subscriberName: 'audit:1:message:created',
  });
  const third = createSubscriber({
    subscriberName: 'search:1:message:created',
  });
  const failures = new Map([
    [first.subscriberName, new Error('First consumer unavailable')],
    [third.subscriberName, new Error('Third consumer unavailable')],
  ]);
  const bind = transport.bind;
  transport.bind = async (binding, dispatch) => {
    const failure = failures.get(binding.subscriberName);
    if (failure) throw failure;
    return bind(binding, dispatch);
  };
  await manager.registerSubscriber(first);
  await manager.registerSubscriber(second);
  await manager.registerSubscriber(third);

  await assert.rejects(manager.start(), (error) => {
    assert.strictEqual(error.constructor, Error);
    assert.strictEqual(error.cause, failures.get(first.subscriberName));
    return true;
  });
  assert.deepStrictEqual(
    calls.filter((call) => call[0] !== 'error').map((call) => call[0]),
    ['bind', 'stop'],
  );
  assert.deepStrictEqual(
    calls.filter((call) => call[0] === 'error').map((call) => call[1]),
    [...failures.values()],
  );
  assert.strictEqual(manager.started, false);
  assert.strictEqual(manager.active.size, 0);
  assert.strictEqual(manager.retryTimer, null);
  await manager.stop();
});

test('subscriptions - retain handles when stopping fails', async () => {
  const { manager } = createManager();
  const subscriber = createSubscriber();
  await manager.registerSubscriber(subscriber);
  await manager.start();
  const { handle } = manager.active.get(subscriber.subscriberName);
  const stop = handle.stop;
  handle.stop = async () => {
    throw new Error('Cannot drain');
  };
  await assert.rejects(manager.stop(), /Failed to stop/);
  assert.strictEqual(manager.active.size, 1);
  handle.stop = stop;
  await manager.stop();
  assert.strictEqual(manager.active.size, 0);
});

test('event loaders - keep contracts after invalid reload', async (t) => {
  const { manager } = createManager();
  const loader = new EventLoader(createApplication(), manager);
  const file = path.join(loader.path, 'chat.1', 'messageCreated.js');
  await loader.change(file);
  const original = manager.events['chat:1:message:created'];
  t.mock.method(fsp, 'readFile', async () => '({');
  await loader.change(file);
  assert.strictEqual(manager.events['chat:1:message:created'], original);
});

test('event loaders - reject an invalid initial snapshot', async (t) => {
  const { manager, calls } = createManager();
  const loader = new EventLoader(createApplication(), manager);
  t.mock.method(fsp, 'readFile', async () => '({');
  await assert.rejects(loader.load(), /Cannot load declaration/);
  assert.deepStrictEqual(calls, []);
});
