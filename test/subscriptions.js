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
  eventName: 'chat:1:message:created',
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
      eventName: contract.eventName,
      maxMessages: contract.maxMessages,
      retryLimit: contract.retryLimit,
      retryDelay: contract.retryDelay,
      timeout: contract.timeout,
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
  assert.strictEqual(subscriber.eventName, event.eventName);
  assert.strictEqual(subscriber.maxMessages, 5);
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

test('subscriptions - prepare stream without subscribers', async () => {
  const { manager, transport } = createManager();
  let prepared = false;
  transport.ensureStream = async () => {
    prepared = true;
    return true;
  };
  await manager.start();
  assert.strictEqual(prepared, true);
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
  const failure = new Error('Cannot create stream');
  transport.ensureStream = async () => {
    throw failure;
  };
  await manager.registerSubscriber(createSubscriber());
  await assert.rejects(manager.start(), (error) => error === failure);
  assert.strictEqual(manager.started, false);
  assert.deepStrictEqual(calls, []);
  await manager.stop();
});

test('subscriptions - consume without local declarations', async () => {
  const { manager } = createManager();
  let received;
  const subscriber = createSubscriber({
    method: (payload, event) => {
      received = { payload, event };
    },
  });
  await manager.registerSubscriber(subscriber);
  await manager.start();
  try {
    const record = {
      id: 'event-1',
      name: subscriber.eventName,
      createdAt: '2026-10-31T00:00:00.000Z',
      payload: null,
    };
    const signal = new AbortController().signal;
    const { handle } = manager.active.get(subscriber.subscriberName);
    await handle.dispatch(record, { signal });
    assert.deepStrictEqual(received, {
      payload: null,
      event: {
        id: record.id,
        name: record.name,
        createdAt: record.createdAt,
        signal,
      },
    });
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

test('subscriptions - rebind on event or options changes', async () => {
  const { manager, calls } = createManager();
  const subscriber = createSubscriber();
  await manager.registerSubscriber(subscriber);
  await manager.start();
  await manager.registerSubscriber({ ...subscriber, maxMessages: 10 });
  await manager.registerSubscriber({
    ...subscriber,
    eventName: 'profile:1:details:updated',
  });
  assert.deepStrictEqual(
    calls.map((call) => call[0]),
    ['bind', 'stop', 'bind', 'stop', 'bind'],
  );
  await manager.removeSubscriber(subscriber.subscriberName);
  assert.strictEqual(manager.active.size, 0);
  await manager.stop();
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
  transport.bind = async () => {
    throw new Error('JetStream unavailable');
  };
  await assert.rejects(manager.start(), /JetStream unavailable/);
  assert.strictEqual(manager.started, false);
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
