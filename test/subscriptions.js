'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const metavm = require('metavm');
const fsp = require('node:fs/promises');
const {
  EventLoader,
  SubscriberLoader,
  SubscriptionManager,
  PgbossSubscriptions,
  NatsSubscriptions,
} = require('../lib/events/index.js');
const { SUBSCRIBER_QUEUE_PREFIX } = require('../lib/events/transports.js');
const { contractNames } = require('../lib/events/declarations.js');

const root = process.cwd();

class PgbossClient {
  constructor() {
    this.calls = [];
    this.queues = new Map();
    this.handlers = new Map();
    this.bindings = new Map();
    this.lastWorkId = 0;
  }

  async getQueue(name) {
    this.calls.push(['getQueue', name]);
    return this.queues.get(name) || null;
  }

  async getQueues() {
    this.calls.push(['getQueues']);
    return [...this.queues.values()];
  }

  async createQueue(name, options) {
    this.calls.push(['createQueue', name, options]);
    this.queues.set(name, { name, ...options });
  }

  async updateQueue(name, options) {
    this.calls.push(['updateQueue', name, options]);
    const queue = this.queues.get(name);
    this.queues.set(name, { ...queue, ...options });
  }

  async subscribe(eventName, queueName) {
    this.calls.push(['subscribe', eventName, queueName]);
    let events = this.bindings.get(queueName);
    if (!events) {
      events = new Set();
      this.bindings.set(queueName, events);
    }
    events.add(eventName);
  }

  async unsubscribe(eventName, queueName) {
    this.calls.push(['unsubscribe', eventName, queueName]);
    this.bindings.get(queueName)?.delete(eventName);
  }

  async deleteQueue(queueName) {
    this.calls.push(['deleteQueue', queueName]);
    this.queues.delete(queueName);
  }

  async work(queueName, options, handler) {
    const workId = `worker-${++this.lastWorkId}`;
    this.calls.push(['work', queueName, options, workId]);
    this.handlers.set(queueName, handler);
    return workId;
  }

  async offWork(queueName, options) {
    this.calls.push(['offWork', queueName, options]);
    this.handlers.delete(queueName);
  }
}

const createApplication = () => ({
  path: path.join(root, 'test'),
  console,
  sandbox: metavm.createContext({}),
  watcher: { watch() {} },
  absolute(relative) {
    return path.join(this.path, relative);
  },
});

const createPgboss = () => {
  const client = new PgbossClient();
  return {
    client,
    enabled: true,
    config: { useListenNotify: true },
    replaceSubscription: async (event, name) => {
      client.bindings.set(name, new Set());
      await client.subscribe(event, name);
    },
    clearSubscription: async (name) => client.bindings.delete(name),
  };
};

const createManager = (
  emitter,
  pgboss = null,
  nats = null,
  logger = console,
  managesTopology = true,
) =>
  new SubscriptionManager(
    emitter,
    new PgbossSubscriptions(pgboss, managesTopology),
    new NatsSubscriptions(nats, logger),
    logger,
  );

const createNats = () => {
  const calls = [];
  const subscriptions = new Map();
  return {
    calls,
    subscriptions,
    connection: {
      subscribe: (subject, options) => {
        calls.push(['subscribe', subject, options]);
        const subscription = {
          drain: async () => {
            calls.push(['unsubscribe', subject]);
            subscriptions.delete(subject);
          },
        };
        subscriptions.set(subject, { ...options, subscription });
        return subscription;
      },
    },
  };
};

const createEmitter = () => {
  const calls = [];
  return {
    calls,
    registerEvent: (contract) => calls.push(['registerEvent', contract]),
    unregisterEvent: (eventName) => calls.push(['unregisterEvent', eventName]),
  };
};

const createSubscriptionSpy = () => {
  const calls = [];
  return {
    calls,
    registerEvent: async (contract) => calls.push(['registerEvent', contract]),
    unregisterEvent: async (eventName) =>
      calls.push(['unregisterEvent', eventName]),
    registerSubscriber: async (contract) =>
      calls.push(['registerSubscriber', contract]),
    removeSubscriber: async (subscriberName) =>
      calls.push(['removeSubscriber', subscriberName]),
  };
};

const createEvent = (overrides = {}) => ({
  eventName: 'chat:1:message:created',
  eventSubject: 'chat.v1.message-created',
  transports: ['local', 'nats'],
  ...overrides,
});

const createSubscriber = (overrides = {}) => ({
  subscriberName: 'feed:1:message:created',
  subscriberPath: 'feed/1/messageCreated',
  eventName: 'chat:1:message:created',
  eventSubject: 'chat.v1.message-created',
  queueName: `${SUBSCRIBER_QUEUE_PREFIX}feed/1/messageCreated`,
  queueGroup: 'feed.v1.message-created',
  concurrency: 2,
  retryLimit: 5,
  retryDelay: 1000,
  timeout: 30000,
  method: async () => {},
  ...overrides,
});

const countCalls = (calls, operation) =>
  calls.filter((call) => call[0] === operation).length;

test('subscriptions - build transport names from its path', () => {
  const directory = path.join(root, 'test', 'events');
  const versioned = path.join(directory, 'chat.2', 'messageCreated.js');
  const unversioned = path.join(directory, 'profile', 'avatarUpdated.js');

  assert.deepStrictEqual(contractNames(directory, versioned), {
    name: 'chat:2:message:created',
    subject: 'chat.2.message.created',
    path: 'chat/2/messageCreated',
  });
  assert.deepStrictEqual(contractNames(directory, unversioned), {
    name: 'profile:1:avatar:updated',
    subject: 'profile.1.avatar.updated',
    path: 'profile/1/avatarUpdated',
  });
});

test('subscriptions - load normalized event contracts', async () => {
  const pubsub = createSubscriptionSpy();
  const contracts = new EventLoader(createApplication(), pubsub);

  await contracts.load();

  assert.strictEqual(pubsub.calls.length, 1);
  const [operation, contract] = pubsub.calls[0];
  assert.strictEqual(operation, 'registerEvent');
  assert.strictEqual(contract.eventName, 'chat:1:message:created');
  assert.strictEqual(contract.eventSubject, 'chat.1.message.created');
  assert.strictEqual(contract.caption, 'Message created');
  assert.strictEqual(contract.description, 'A new chat message was created');
  assert.deepStrictEqual(structuredClone(contract.examples), [
    { messageId: 42 },
  ]);
  assert.deepStrictEqual(structuredClone(contract.transports), [
    'local',
    'nats',
  ]);
});

test('subscriptions - describe sorted NATS events', async () => {
  const pubsub = createManager(createEmitter());
  await pubsub.registerEvent(
    createEvent({
      eventName: 'profile:1:updated',
      eventSubject: 'profile.1.updated',
      caption: 'Profile updated',
      description: 'A profile was updated',
      examples: [{ profileId: 42 }],
      transports: ['nats'],
    }),
  );
  await pubsub.registerEvent(
    createEvent({
      eventName: 'chat:1:message:created',
      eventSubject: 'chat.1.message.created',
      transports: ['local'],
    }),
  );
  await pubsub.registerEvent(
    createEvent({
      eventName: 'account:1:created',
      eventSubject: 'account.1.created',
      transports: ['local', 'nats'],
    }),
  );

  assert.deepStrictEqual(pubsub.describeEvents(), [
    {
      name: 'account:1:created',
      subject: 'account.1.created',
      caption: '',
      description: '',
      examples: null,
      transports: ['local', 'nats'],
    },
    {
      name: 'profile:1:updated',
      subject: 'profile.1.updated',
      caption: 'Profile updated',
      description: 'A profile was updated',
      examples: [{ profileId: 42 }],
      transports: ['nats'],
    },
  ]);
});

test('subscriptions - announce event contract changes', async () => {
  const application = createApplication();
  let updates = 0;
  application.nats = {
    updateEventDiscovery: () => updates++,
  };
  const pubsub = createSubscriptionSpy();
  const contracts = new EventLoader(application, pubsub, () =>
    application.nats.updateEventDiscovery(),
  );
  const fileName = path.join(contracts.path, 'chat.1', 'messageCreated.js');

  await contracts.change(fileName);
  assert.strictEqual(updates, 1);
  await contracts.delete(fileName);
  assert.strictEqual(updates, 2);
});

test('subscriptions - load normalized subscriber contracts', async () => {
  const pubsub = createSubscriptionSpy();
  const subscribers = new SubscriberLoader(createApplication(), pubsub);

  await subscribers.load();

  assert.strictEqual(pubsub.calls.length, 1);
  const [operation, contract] = pubsub.calls[0];
  assert.strictEqual(operation, 'registerSubscriber');
  assert.strictEqual(contract.subscriberName, 'feed:1:message:created');
  assert.strictEqual(contract.subscriberPath, 'feed/1/messageCreated');
  assert.strictEqual(contract.eventName, 'chat:1:message:created');
  const local = new PgbossSubscriptions(null).createBinding(contract);
  const nats = new NatsSubscriptions(null).createBinding(contract);
  assert.strictEqual(local.queueName, 'subscribers/feed/1/messageCreated');
  assert.strictEqual(nats.eventSubject, 'chat.1.message.created');
  assert.strictEqual(nats.queueGroup, 'feed.1.message.created');
  assert.strictEqual(contract.concurrency, 2);
  assert.strictEqual(contract.retryLimit, 5);
  assert.strictEqual(contract.retryDelay, 1000);
  assert.strictEqual(contract.timeout, 30000);
  assert.strictEqual(typeof contract.method, 'function');
});

test('subscriptions - delete loaded contracts', async () => {
  const pubsub = createSubscriptionSpy();
  const application = createApplication();
  const events = new EventLoader(application, pubsub);
  const subscribers = new SubscriberLoader(application, pubsub);
  const eventFile = path.join(events.path, 'chat.1', 'messageCreated.js');
  const subscriberFile = path.join(
    subscribers.path,
    'feed.1',
    'messageCreated.js',
  );

  await events.change(eventFile);
  await subscribers.change(subscriberFile);
  await events.delete(eventFile);
  await subscribers.delete(subscriberFile);

  assert.deepStrictEqual(pubsub.calls.slice(-2), [
    ['unregisterEvent', 'chat:1:message:created'],
    ['removeSubscriber', 'feed:1:message:created'],
  ]);
});

test('subscriptions - bind declarations only after start', async () => {
  const emitter = createEmitter();
  const pgboss = createPgboss();
  const nats = createNats();
  const pubsub = createManager(emitter, pgboss, nats, console);

  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(createSubscriber());

  assert.strictEqual(pgboss.client.calls.length, 0);
  assert.strictEqual(nats.calls.length, 0);
  assert.strictEqual(pubsub.active.size, 0);

  await pubsub.start();

  assert.strictEqual(countCalls(pgboss.client.calls, 'subscribe'), 1);
  assert.strictEqual(nats.calls.length, 0);
  assert.strictEqual(pubsub.active.size, 1);
});

test('subscriptions - select transport from event contracts', async () => {
  const emitter = createEmitter();
  const pgboss = createPgboss();
  const nats = createNats();
  const pubsub = createManager(emitter, pgboss, nats, console);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerEvent(
    createEvent({
      eventName: 'billing:1:invoice:paid',
      eventSubject: 'billing.v1.invoice-paid',
      transports: ['nats'],
    }),
  );
  await pubsub.registerSubscriber(createSubscriber());
  await pubsub.registerSubscriber(
    createSubscriber({
      subscriberName: 'feed:1:invoice:paid',
      subscriberPath: 'feed/1/invoicePaid',
      eventName: 'billing:1:invoice:paid',
      eventSubject: 'billing.v1.invoice-paid',
      queueName: `${SUBSCRIBER_QUEUE_PREFIX}feed/1/invoicePaid`,
      queueGroup: 'feed.v1.invoice-paid',
    }),
  );
  await pubsub.registerSubscriber(
    createSubscriber({
      subscriberName: 'feed:1:profile:deleted',
      subscriberPath: 'feed/1/profileDeleted',
      eventName: 'profile:1:profile:deleted',
      eventSubject: 'profile.v1.profile-deleted',
      queueName: `${SUBSCRIBER_QUEUE_PREFIX}feed/1/profileDeleted`,
      queueGroup: 'feed.v1.profile-deleted',
    }),
  );

  await pubsub.start();

  const localSubscriptions = pgboss.client.calls.filter(
    (call) => call[0] === 'subscribe',
  );
  assert.deepStrictEqual(localSubscriptions, [
    [
      'subscribe',
      'chat:1:message:created',
      `${SUBSCRIBER_QUEUE_PREFIX}feed/1/messageCreated`,
    ],
  ]);
  const natsSubjects = nats.calls
    .filter((call) => call[0] === 'subscribe')
    .map((call) => call[1]);
  assert.deepStrictEqual(natsSubjects, [
    'billing.v1.invoice-paid',
    'profile.v1.profile-deleted',
  ]);
  assert.strictEqual(pubsub.active.size, 3);
});

test('subscriptions - use exact names and pgboss settings', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const calls = [];
  const subscriber = createSubscriber({
    method: async (...args) => calls.push(args),
  });
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);

  await pubsub.start();

  assert.deepStrictEqual(pgboss.client.calls, [
    ['getQueue', subscriber.queueName],
    [
      'createQueue',
      subscriber.queueName,
      {
        notify: true,
        retryLimit: 5,
        retryDelay: 1,
        expireInSeconds: 30,
      },
    ],
    ['subscribe', subscriber.eventName, subscriber.queueName],
    ['work', subscriber.queueName, { localConcurrency: 2 }, 'worker-1'],
    ['getQueues'],
  ]);
  const consume = pgboss.client.handlers.get(subscriber.queueName);
  const signal = new AbortController().signal;
  await consume([
    {
      signal,
      data: {
        id: 'event-1',
        name: subscriber.eventName,
        createdAt: '2026-09-04T00:00:00.000Z',
        data: { messageId: 42 },
      },
    },
  ]);
  assert.deepStrictEqual(calls, [
    [
      { messageId: 42 },
      {
        id: 'event-1',
        name: subscriber.eventName,
        createdAt: '2026-09-04T00:00:00.000Z',
        signal,
      },
    ],
  ]);
});

test('subscriptions - ignore worker settings for Core NATS', async () => {
  const nats = createNats();
  const calls = [];
  const pubsub = createManager(createEmitter(), null, nats, console);
  const subscriber = createSubscriber({
    eventSubject: 'custom.subject-kept-verbatim',
    queueGroup: 'custom.queue-group',
    method: async (...args) => calls.push(args),
  });
  await pubsub.registerSubscriber(subscriber);

  await pubsub.start();

  assert.strictEqual(nats.calls.length, 1);
  const [operation, subject, options] = nats.calls[0];
  assert.strictEqual(operation, 'subscribe');
  assert.strictEqual(subject, 'custom.subject-kept-verbatim');
  assert.strictEqual(options.queue, 'custom.queue-group');
  assert.deepStrictEqual(Object.keys(options).sort(), ['callback', 'queue']);
  await options.callback(null, {
    json: () => ({
      id: 'event-1',
      name: subscriber.eventName,
      createdAt: '2026-09-04T00:00:00.000Z',
      data: { messageId: 42 },
    }),
  });
  assert.deepStrictEqual(calls, [
    [
      { messageId: 42 },
      {
        id: 'event-1',
        name: subscriber.eventName,
        createdAt: '2026-09-04T00:00:00.000Z',
      },
    ],
  ]);
});

test('subscriptions - drain running NATS handlers', async () => {
  const nats = createNats();
  let releaseHandler;
  const handlerFinished = new Promise((resolve) => {
    releaseHandler = resolve;
  });
  const binding = {
    subscriberName: 'feed:1:message:created',
    eventSubject: 'chat.1.message.created',
    queueGroup: 'feed.1.message.created',
  };
  const adapter = new NatsSubscriptions(nats);
  const handle = adapter.bind(binding, () => handlerFinished);
  const subscription = nats.subscriptions.get(binding.eventSubject);
  subscription.callback(null, { json: () => ({ data: {} }) });

  let stopped = false;
  const stopping = handle.stop().then(() => {
    stopped = true;
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.strictEqual(stopped, false);
  releaseHandler();
  await stopping;
  assert.strictEqual(stopped, true);
});

test('subscriptions - update a method without rebinding', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const calls = [];
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(
    createSubscriber({ method: async () => calls.push('old') }),
  );
  await pubsub.start();

  const queueName = `${SUBSCRIBER_QUEUE_PREFIX}feed/1/messageCreated`;
  const consume = pgboss.client.handlers.get(queueName);
  await pubsub.registerSubscriber(
    createSubscriber({ method: async () => calls.push('new') }),
  );
  await pubsub.reconcile();
  await consume([
    {
      data: {
        id: 'event-1',
        name: 'chat:1:message:created',
        createdAt: '2026-09-04T00:00:00.000Z',
        data: {},
      },
    },
  ]);

  assert.deepStrictEqual(calls, ['new']);
  assert.strictEqual(countCalls(pgboss.client.calls, 'work'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 0);
  assert.strictEqual(countCalls(pgboss.client.calls, 'subscribe'), 1);
});

test('subscriptions - ignore NATS-only setting changes', async () => {
  const nats = createNats();
  const calls = [];
  const pubsub = createManager(createEmitter(), null, nats, console);
  const subscriber = createSubscriber({
    method: async () => calls.push('old'),
  });
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();
  const subscription = nats.subscriptions.get(subscriber.eventSubject);

  await pubsub.registerSubscriber(
    createSubscriber({
      concurrency: 20,
      retryLimit: 20,
      retryDelay: 20000,
      timeout: 60000,
      method: async () => calls.push('new'),
    }),
  );
  await pubsub.reconcile();
  await subscription.callback(null, {
    json: () => ({
      id: 'event-1',
      name: subscriber.eventName,
      createdAt: '2026-09-04T00:00:00.000Z',
      data: {},
    }),
  });

  assert.deepStrictEqual(calls, ['new']);
  assert.strictEqual(countCalls(nats.calls, 'subscribe'), 1);
  assert.strictEqual(countCalls(nats.calls, 'unsubscribe'), 0);
});

test('subscriptions - migrate on source change', async () => {
  const pgboss = createPgboss();
  const nats = createNats();
  const pubsub = createManager(createEmitter(), pgboss, nats, console);
  const subscriber = createSubscriber();
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();
  assert.strictEqual(countCalls(nats.calls, 'subscribe'), 1);

  await pubsub.registerEvent(createEvent());
  await pubsub.reconcile();

  assert.strictEqual(countCalls(nats.calls, 'unsubscribe'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'subscribe'), 1);
  assert.strictEqual(pubsub.active.size, 1);

  await pubsub.unregisterEvent('chat:1:message:created');
  await pubsub.reconcile();

  assert.strictEqual(countCalls(pgboss.client.calls, 'unsubscribe'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 0);
  assert.strictEqual(pgboss.client.queues.has(subscriber.queueName), true);
  assert.strictEqual(countCalls(nats.calls, 'subscribe'), 2);
  assert.strictEqual(pubsub.active.size, 1);
});

test('subscriptions - remove a deleted subscriber', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const subscriber = createSubscriber();
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();

  await pubsub.removeSubscriber(subscriber.subscriberName);
  await pubsub.reconcile();

  assert.strictEqual(countCalls(pgboss.client.calls, 'unsubscribe'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 1);
  assert.strictEqual(pubsub.active.size, 0);
});

test('subscriptions - stop workers without deleting bindings', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const subscriber = createSubscriber();
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();

  await pubsub.stop();
  await pubsub.stop();

  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'unsubscribe'), 0);
  assert.strictEqual(pubsub.active.size, 0);
});

test('subscriptions - serialize start and stop', async () => {
  const pgboss = createPgboss();
  const originalWork = pgboss.client.work.bind(pgboss.client);
  let workStarted;
  let startWork;
  const workStarting = new Promise((resolve) => {
    workStarted = resolve;
  });
  const workPending = new Promise((resolve) => {
    startWork = resolve;
  });
  pgboss.client.work = async (...args) => {
    workStarted();
    await workPending;
    return originalWork(...args);
  };
  const emitter = createEmitter();
  const pubsub = createManager(emitter, pgboss, null, console);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(createSubscriber());

  const starting = pubsub.start();
  await workStarting;
  const stopping = pubsub.stop();
  startWork();
  await starting;
  await stopping;

  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 1);
  assert.strictEqual(pubsub.active.size, 0);
  assert.strictEqual(pubsub.started, false);
  assert.deepStrictEqual(
    emitter.calls.map(([operation]) => operation),
    ['registerEvent', 'unregisterEvent'],
  );
});

test('subscriptions - keep a consumer after failed stop', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(createSubscriber());
  await pubsub.start();

  const originalOffWork = pgboss.client.offWork.bind(pgboss.client);
  pgboss.client.offWork = async () => {
    throw new Error('connection lost');
  };
  await assert.rejects(pubsub.stop(), /Failed to stop event subscribers/);
  assert.strictEqual(pubsub.active.size, 1);

  pgboss.client.offWork = originalOffWork;
  await pubsub.stop();
  assert.strictEqual(pubsub.active.size, 0);
});

test('subscriptions - retain stopped bindings until removal', async () => {
  const pgboss = createPgboss();
  const client = pgboss.client;
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const subscriber = createSubscriber();
  await pubsub.registerEvent(createEvent({ transports: ['local'] }));
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();

  pgboss.client = null;
  await pubsub.registerEvent(createEvent({ transports: ['local'] }));
  const stopped = pubsub.active.get('feed:1:message:created');
  assert.strictEqual(stopped.running, false);

  await pubsub.registerEvent(createEvent({ transports: [] }));
  assert.strictEqual(pubsub.active.size, 0);
  assert.strictEqual(countCalls(client.calls, 'unsubscribe'), 1);
  assert.strictEqual(countCalls(client.calls, 'deleteQueue'), 0);
  assert.strictEqual(client.queues.has(subscriber.queueName), true);
});

test('subscriptions - inactive instance should only consume', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console, false);
  const subscriber = createSubscriber();
  pgboss.client.queues.set(subscriber.queueName, {
    name: subscriber.queueName,
  });
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);

  await pubsub.start();

  assert.strictEqual(countCalls(pgboss.client.calls, 'work'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'getQueue'), 1);
  for (const operation of [
    'getQueues',
    'createQueue',
    'updateQueue',
    'subscribe',
    'unsubscribe',
    'deleteQueue',
  ]) {
    assert.strictEqual(countCalls(pgboss.client.calls, operation), 0);
  }

  await pubsub.removeSubscriber(subscriber.subscriberName);

  assert.strictEqual(countCalls(pgboss.client.calls, 'offWork'), 1);
  assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 0);
});

test('subscriptions - wait for queues from a later manager', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const pgboss = createPgboss();
  const client = pgboss.client;
  const pubsub = createManager(createEmitter(), pgboss, null, console, false);
  t.after(() => pubsub.stop());
  const subscriber = createSubscriber();
  const ready = createSubscriber({
    subscriberName: 'feed:1:ready',
    queueName: `${SUBSCRIBER_QUEUE_PREFIX}feed/1/ready`,
  });
  client.queues.set(ready.queueName, { name: ready.queueName });
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);
  await pubsub.registerSubscriber(ready);

  await pubsub.start();

  assert.strictEqual(client.handlers.has(subscriber.queueName), false);
  assert.strictEqual(client.handlers.has(ready.queueName), true);
  assert.strictEqual(pubsub.active.size, 1);
  assert.strictEqual(countCalls(client.calls, 'createQueue'), 0);

  t.mock.timers.tick(1000);
  await pubsub.operation;
  assert.strictEqual(countCalls(client.calls, 'getQueue'), 3);
  assert.strictEqual(countCalls(client.calls, 'work'), 1);

  const calls = [];
  await pubsub.registerSubscriber(
    createSubscriber({ method: async () => calls.push('reloaded') }),
  );
  const managingPgboss = createPgboss();
  managingPgboss.client.queues = client.queues;
  const manager = createManager(createEmitter(), managingPgboss);
  t.after(() => manager.stop());
  await manager.registerEvent(createEvent());
  await manager.registerSubscriber(subscriber);
  await manager.registerSubscriber(ready);
  await manager.start();

  t.mock.timers.tick(1000);
  await pubsub.operation;

  assert.strictEqual(pubsub.active.size, 2);
  assert.strictEqual(countCalls(client.calls, 'work'), 2);
  assert.strictEqual(countCalls(client.calls, 'createQueue'), 0);
  assert.strictEqual(countCalls(client.calls, 'updateQueue'), 0);
  assert.strictEqual(countCalls(client.calls, 'subscribe'), 0);
  assert.strictEqual(countCalls(managingPgboss.client.calls, 'createQueue'), 1);
  await client.handlers.get(subscriber.queueName)([
    { data: { name: subscriber.eventName, data: {} } },
  ]);
  assert.deepStrictEqual(calls, ['reloaded']);

  const checks = countCalls(client.calls, 'getQueue');
  t.mock.timers.tick(5000);
  await pubsub.operation;
  assert.strictEqual(countCalls(client.calls, 'getQueue'), checks);
  assert.strictEqual(countCalls(client.calls, 'work'), 2);
});

for (const action of ['stop', 'removeSubscriber']) {
  test(`subscriptions - cancel queue waiting on ${action}`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const pgboss = createPgboss();
    const client = pgboss.client;
    const pubsub = createManager(createEmitter(), pgboss, null, console, false);
    t.after(() => pubsub.stop());
    const subscriber = createSubscriber();
    await pubsub.registerEvent(createEvent());
    await pubsub.registerSubscriber(subscriber);
    await pubsub.start();

    await pubsub[action](subscriber.subscriberName);
    const checks = countCalls(client.calls, 'getQueue');
    client.queues.set(subscriber.queueName, { name: subscriber.queueName });
    t.mock.timers.tick(5000);
    await pubsub.operation;

    assert.strictEqual(countCalls(client.calls, 'getQueue'), checks);
    assert.strictEqual(countCalls(client.calls, 'work'), 0);
    assert.strictEqual(pubsub.active.size, 0);
  });
}

test('subscriptions - propagate queue lookup failures on start', async () => {
  const pgboss = createPgboss();
  const error = new Error('connection lost');
  pgboss.client.getQueue = async () => {
    throw error;
  };
  const pubsub = createManager(createEmitter(), pgboss, null, console, false);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(createSubscriber());

  await assert.rejects(pubsub.start(), { cause: error });

  assert.strictEqual(pubsub.started, false);
  assert.strictEqual(pubsub.active.size, 0);
  assert.strictEqual(countCalls(pgboss.client.calls, 'work'), 0);
});

test('subscriptions - active instance removes remote subscriber', async () => {
  const pgboss = createPgboss();
  const nats = createNats();
  const subscriber = createSubscriber({
    eventName: 'billing:1:invoice:paid',
    eventSubject: 'billing.1.invoice.paid',
  });
  pgboss.client.queues.set(subscriber.queueName, {
    name: subscriber.queueName,
  });
  const pubsub = createManager(createEmitter(), pgboss, nats, console);
  await pubsub.registerSubscriber(subscriber);

  await pubsub.start();
  await pubsub.removeSubscriber(subscriber.subscriberName);

  assert.strictEqual(pgboss.client.queues.has(subscriber.queueName), false);
  assert.strictEqual(countCalls(pgboss.client.calls, 'unsubscribe'), 0);
  assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 1);
  assert.strictEqual(countCalls(nats.calls, 'unsubscribe'), 1);
});

test('subscriptions - active instance removes stale queues', async () => {
  const pgboss = createPgboss();
  const client = pgboss.client;
  const subscriber = createSubscriber();
  const stale = `${SUBSCRIBER_QUEUE_PREFIX}feed/1/old-handler`;
  client.queues.set(stale, { name: stale });
  client.queues.set('tasks/cleanup', { name: 'tasks/cleanup' });
  client.queues.set('events/nats/publish', { name: 'events/nats/publish' });
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);

  await pubsub.start();

  assert.strictEqual(client.queues.has(subscriber.queueName), true);
  assert.strictEqual(client.queues.has(stale), false);
  assert.strictEqual(client.queues.has('tasks/cleanup'), true);
  assert.strictEqual(client.queues.has('events/nats/publish'), true);
  assert.strictEqual(countCalls(client.calls, 'deleteQueue'), 1);
});

test('subscriptions - active instance clears a stale catalog', async () => {
  const pgboss = createPgboss();
  const client = pgboss.client;
  const stale = `${SUBSCRIBER_QUEUE_PREFIX}feed/1/removed`;
  client.queues.set(stale, { name: stale });
  const pubsub = createManager(createEmitter(), pgboss, null, console);

  await pubsub.start();

  assert.strictEqual(client.queues.has(stale), false);
  assert.strictEqual(countCalls(client.calls, 'getQueues'), 1);
  assert.strictEqual(countCalls(client.calls, 'deleteQueue'), 1);
});

test('subscriptions - not clean queues after a bind failure', async () => {
  const pgboss = createPgboss();
  const client = pgboss.client;
  const stale = `${SUBSCRIBER_QUEUE_PREFIX}feed/1/removed`;
  client.queues.set(stale, { name: stale });
  client.work = async () => {
    throw new Error('work failed');
  };
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(createSubscriber());

  await assert.rejects(pubsub.start(), /Failed to bind event subscribers/);

  assert.strictEqual(pubsub.started, false);
  assert.strictEqual(client.queues.has(stale), true);
  assert.strictEqual(countCalls(client.calls, 'getQueues'), 0);
  assert.strictEqual(countCalls(client.calls, 'deleteQueue'), 0);
});

test('subscriptions - preserve a reloaded subscriber queue', async () => {
  const pgboss = createPgboss();
  const pubsub = createManager(createEmitter(), pgboss, null, console);
  const subscriber = createSubscriber();
  await pubsub.registerEvent(createEvent());
  await pubsub.registerSubscriber(subscriber);
  await pubsub.start();

  const removing = pubsub.removeSubscriber(subscriber.subscriberName);
  const registering = pubsub.registerSubscriber(createSubscriber());
  await Promise.all([removing, registering]);

  assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 0);
  assert.strictEqual(pgboss.client.queues.has(subscriber.queueName), true);
  assert.strictEqual(pubsub.active.size, 1);
});

test('subscriptions - replace bindings after restart', async () => {
  const pgboss = createPgboss();
  const subscriber = createSubscriber();
  const first = createManager(createEmitter(), pgboss);
  await first.registerEvent(createEvent());
  await first.registerSubscriber(subscriber);
  await first.start();
  await first.stop();

  const eventName = 'chat:1:message:updated';
  const second = createManager(createEmitter(), pgboss);
  await second.registerEvent(createEvent({ eventName }));
  await second.registerSubscriber(createSubscriber({ eventName }));
  await second.start();
  try {
    assert.deepStrictEqual(
      [...pgboss.client.bindings.get(subscriber.queueName)],
      [eventName],
    );
    assert.strictEqual(countCalls(pgboss.client.calls, 'deleteQueue'), 0);
  } finally {
    await second.stop();
  }
});

test('subscriptions - clear local bindings on remote rollout', async () => {
  const pgboss = createPgboss();
  const subscriber = createSubscriber();
  const first = createManager(createEmitter(), pgboss);
  await first.registerEvent(createEvent());
  await first.registerSubscriber(subscriber);
  await first.start();
  await first.stop();

  const second = createManager(createEmitter(), pgboss, createNats());
  await second.registerSubscriber(
    createSubscriber({
      eventName: 'remote:1:updated',
    }),
  );
  await second.start();
  try {
    assert.strictEqual(pgboss.client.bindings.has(subscriber.queueName), false);
    assert.strictEqual(pgboss.client.queues.has(subscriber.queueName), true);
  } finally {
    await second.stop();
  }
});

test('event loaders - reject an incomplete initial snapshot', async (t) => {
  const application = createApplication();
  const subscriptions = createManager(createEmitter());
  const loader = new SubscriberLoader(application, subscriptions);
  t.mock.method(fsp, 'readdir', async (directory) => {
    if (directory === loader.path) {
      return [{ name: 'feed.1', isDirectory: () => true }];
    }
    return ['first.js', 'second.js'].map((name) => ({
      name,
      isDirectory: () => false,
    }));
  });
  t.mock.method(fsp, 'readFile', async (file) => {
    if (file.endsWith('second.js')) return '({ method:';
    return `({
      event: 'chat:1:created',
      method: async () => {},
    })`;
  });

  await assert.rejects(loader.load(), /Cannot load declaration/);
  assert.deepStrictEqual(Object.keys(subscriptions.subscribers), []);
});

test('event loaders - preserve declarations on reload error', async (t) => {
  const application = createApplication();
  const errors = [];
  application.console = { error: (error) => errors.push(error) };
  const subscriptions = createManager(createEmitter());
  const events = new EventLoader(application, subscriptions);
  const subscribers = new SubscriberLoader(application, subscriptions);
  await events.load();
  await subscribers.load();
  const event = subscriptions.events['chat:1:message:created'];
  const subscriber = subscriptions.subscribers['feed:1:message:created'];

  t.mock.method(fsp, 'readFile', async () => {
    throw new Error('Read failed');
  });
  await events.change(path.join(events.path, 'chat.1', 'messageCreated.js'));
  await subscribers.change(
    path.join(subscribers.path, 'feed.1', 'messageCreated.js'),
  );

  assert.strictEqual(subscriptions.events[event.eventName], event);
  assert.strictEqual(
    subscriptions.subscribers[subscriber.subscriberName],
    subscriber,
  );
  assert.strictEqual(errors.length, 2);
});

test('subscriptions - retry cleanup after reconnect', async () => {
  const pgboss = createPgboss();
  const client = pgboss.client;
  const subscriber = createSubscriber();
  const subscriptions = createManager(createEmitter(), pgboss);
  await subscriptions.registerEvent(createEvent());
  await subscriptions.registerSubscriber(subscriber);
  await subscriptions.start();

  pgboss.client = null;
  await subscriptions.removeSubscriber(subscriber.subscriberName);
  assert.strictEqual(client.queues.has(subscriber.queueName), true);

  pgboss.client = client;
  await subscriptions.synchronize();
  assert.strictEqual(client.queues.has(subscriber.queueName), false);
  await subscriptions.stop();
});
