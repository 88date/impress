'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const { Error } = require('metautil');
const { npm } = require('../lib/deps.js');
const { Nats } = require('../lib/nats.js');

const matches = (pattern, subject) => {
  const expected = pattern.split('.');
  const actual = subject.split('.');
  if (expected.length !== actual.length) return false;
  return expected.every(
    (part, index) => part === '*' || part === actual[index],
  );
};

const createConnection = () => {
  const requests = [];
  const published = [];
  const subscriptions = new Map();
  const connection = {
    requests,
    published,
    subscriptions,
    subscribe(subject, options) {
      subscriptions.set(subject, options);
      return {
        subject,
        unsubscribe: () => subscriptions.delete(subject),
      };
    },
    request(subject, payload, options) {
      requests.push({ subject, payload, options });
      const subscription = subscriptions.get(subject);
      if (!subscription) {
        return Promise.reject(new npm.nats.NoRespondersError(subject));
      }
      return new Promise((resolve, reject) => {
        const message = {
          json: () => JSON.parse(payload),
          respond: (response) => {
            resolve({ json: () => JSON.parse(response) });
            return true;
          },
        };
        Promise.resolve(subscription.callback(null, message)).catch(reject);
      });
    },
    async requestMany(subject, payload, options) {
      requests.push({ subject, payload, options });
      const responses = [];
      const subscription = subscriptions.get(subject);
      if (!subscription) throw new npm.nats.NoRespondersError(subject);
      const message = {
        subject,
        json: () => JSON.parse(payload),
        respond: (response) => {
          responses.push(response);
          return true;
        },
      };
      await subscription.callback(null, message);
      return {
        async *[Symbol.asyncIterator]() {
          for (const response of responses) {
            yield { json: () => JSON.parse(response) };
          }
        },
      };
    },
    publish(subject, payload) {
      published.push({ subject, payload });
      for (const [pattern, subscription] of subscriptions) {
        if (!matches(pattern, subject)) continue;
        const message = {
          subject,
          json: () => JSON.parse(payload),
        };
        Promise.resolve(subscription.callback(null, message)).catch(() => {});
      }
    },
    async flush() {},
  };
  return connection;
};

test('lib/nats - should load NATS transport', () => {
  assert.strictEqual(typeof npm.nats.connect, 'function');
  assert.strictEqual(typeof npm.nats.credsAuthenticator, 'function');
});

test('lib/nats - should require configuration', async () => {
  const application = { config: { service: { enabled: true } } };
  const nats = new Nats(application);
  await assert.rejects(() => nats.start(), {
    message: 'NATS servers and credentials expected',
  });
});

test('lib/nats - should drain connection on close', async () => {
  let drained = false;
  const nats = new Nats({});
  nats.connection = {
    drain: async () => {
      drained = true;
    },
  };
  await nats.close();
  assert.strictEqual(drained, true);
  assert.strictEqual(nats.connection, null);
  assert.strictEqual(nats.discoveryCatalogSubscription, null);
});

test('lib/nats - should refresh discovery after reconnect', async () => {
  const calls = [];
  const nats = new Nats({ console: { error() {} } });
  nats.connection = {
    async *status() {
      yield { type: 'reconnect' };
    },
  };
  nats.announceServices = () => calls.push('announce');
  nats.discoverServices = async () => calls.push('discover');

  await nats.watchStatus();

  assert.deepStrictEqual(calls, ['announce', 'discover']);
});

test('lib/nats - should announce service catalog once', () => {
  const nats = new Nats({});
  nats.connection = createConnection();
  nats.discoverySubscriptions.set('city', {});
  nats.discoverySubscriptions.set('profile', {});

  nats.announceServices();

  assert.deepStrictEqual(nats.connection.published, [
    { subject: 'service.discovery.changed', payload: undefined },
  ]);
});

test('lib/nats - should request and respond', async () => {
  const contextStorage = new AsyncLocalStorage();
  const application = { console: { error() {} }, contextStorage };
  const nats = new Nats(application);
  nats.connection = createConnection();
  let receivedContext = null;
  nats.subscribe('example.1.add', async (context, { a, b }) => {
    receivedContext = context;
    assert.strictEqual(contextStorage.getStore(), context);
    return a + b;
  });

  const session = {
    token: 'session-token',
    state: { userId: 'user-1', language: 'ru' },
  };
  const context = {
    session,
    client: { ip: '127.0.0.1' },
    state: { internal: true },
  };

  const result = await contextStorage.run(context, () =>
    nats.request('example.1.add', { a: 4, b: 6 }, 5000),
  );

  assert.strictEqual(result, 10);
  assert.deepStrictEqual(receivedContext, { session, ip: '127.0.0.1' });
  const subscription = nats.connection.subscriptions.get('example.1.add');
  assert.strictEqual(subscription.queue, 'example.1.add');
  const request = nats.connection.requests[0];
  assert.strictEqual(request.subject, 'example.1.add');
  assert.strictEqual(request.options.timeout, 5000);
});

test('lib/nats - should return 404 without responders', async () => {
  const application = {
    contextStorage: new AsyncLocalStorage(),
  };
  const nats = new Nats(application);
  nats.connection = createConnection();

  await assert.rejects(nats.request('missing.1.action', {}, 5000), {
    message: 'Not Found',
    code: 404,
  });
});

test('lib/nats - should transfer service errors', async () => {
  const logged = [];
  const application = {
    console: { error: (error) => logged.push(error) },
    contextStorage: new AsyncLocalStorage(),
  };
  const nats = new Nats(application);
  nats.connection = createConnection();
  nats.subscribe('example.domain', async () => {
    throw new Error('Operation failed', { code: 'EFAIL' });
  });
  nats.subscribe('example.validation', async () => {
    throw new Error('Invalid parameters');
  });
  nats.subscribe('example.internal', async () => {
    throw new globalThis.Error('Sensitive details');
  });

  await assert.rejects(nats.request('example.domain', {}, 5000), {
    message: 'Operation failed',
    code: 'EFAIL',
  });
  await assert.rejects(nats.request('example.validation', {}, 5000), {
    message: 'Invalid parameters',
  });
  await assert.rejects(nats.request('example.internal', {}, 5000), {
    message: 'Service request failed',
  });
  assert.strictEqual(logged.length, 1);
  assert.strictEqual(logged[0].message, 'Sensitive details');
});

test('lib/nats - should subscribe local services', async () => {
  const calls = [];
  const localV1 = {
    method: async () => {},
    serviceName: 'example',
    version: 1,
    actionName: 'update',
    subject: 'example.1.update',
    invoke: async (context, args) => calls.push({ context, args }),
  };
  const localV2 = {
    method: async () => {},
    serviceName: 'example',
    version: 2,
    actionName: 'get',
    subject: 'example.2.get',
    invoke: async (context, args) => calls.push({ context, args }),
  };
  const remote = { method: null, subject: 'remote.get' };
  const application = {
    contextStorage: new AsyncLocalStorage(),
    service: {
      collection: {
        example: {
          default: 1,
          1: { update: localV1 },
          2: { get: localV2 },
        },
        remote: { default: 1, 1: { get: remote } },
      },
    },
  };
  const nats = new Nats(application);
  const subscriptions = [];
  nats.subscribe = (subject, handler) => {
    const subscription = {
      subject,
      handler,
      closed: false,
      unsubscribe() {
        this.closed = true;
      },
    };
    subscriptions.push(subscription);
    return subscription;
  };

  nats.subscribeServices();
  nats.subscribeServices();

  assert.strictEqual(subscriptions.length, 2);
  assert.strictEqual(subscriptions[0].subject, 'example.1.update');
  assert.strictEqual(subscriptions[1].subject, 'example.2.get');
  const context = { session: { token: 'token', state: { userId: 'user-1' } } };
  await subscriptions[0].handler(context, { a: 4, b: 6 });
  assert.deepStrictEqual(calls, [{ context, args: { a: 4, b: 6 } }]);

  const updated = {
    ...localV1,
    invoke: async (context, args) =>
      calls.push({ context, args, updated: true }),
  };
  application.service.collection.example['1'].update = updated;
  await subscriptions[0].handler(context, { a: 5, b: 7 });
  assert.deepStrictEqual(calls[1], {
    context,
    args: { a: 5, b: 7 },
    updated: true,
  });

  nats.unsubscribeService('example.1.update');
  assert.strictEqual(subscriptions[0].closed, true);
  assert.strictEqual(nats.serviceSubscriptions.has('example.1.update'), false);
});

test('lib/nats - should discover remote services', async () => {
  const connection = createConnection();
  const actions = [
    {
      name: 'sendMessage',
      version: 1,
      access: 'public',
      parameters: null,
      returns: null,
      errors: null,
      caption: '',
      description: '',
      deprecated: false,
      examples: null,
    },
  ];
  const provider = new Nats({
    console: { error() {} },
    config: { service: { discovery: { maxWait: 250 } } },
    service: {
      collection: { supportChat: { default: 1 } },
      isRemote: () => false,
      describe: (name) => ({ name, actions, events: [] }),
    },
  });
  const loaded = [];
  const consumerService = {
    collection: {},
    loadRemote(name, contracts) {
      loaded.push({ name, contracts });
      this.collection[name] = { default: 1 };
    },
  };
  const consumer = new Nats({
    console: { error() {} },
    config: { service: { discovery: { maxWait: 250 } } },
    service: consumerService,
  });
  provider.connection = connection;
  consumer.connection = connection;

  provider.subscribeDiscovery();
  provider.subscribeDiscoveryCatalog();
  consumer.subscribeDiscoveryChanges();
  await consumer.discoverServices();

  assert.deepStrictEqual(loaded, [{ name: 'supportChat', contracts: actions }]);
  const request = connection.requests.at(-1);
  assert.strictEqual(request.subject, 'service.discovery');
  assert.deepStrictEqual(request.options, {
    strategy: 'timer',
    maxWait: 250,
  });

  actions.push({ ...actions[0], name: 'createConversation' });
  const subscription = connection.subscriptions.get(
    'service.discovery.changed',
  );
  await subscription.callback(null, {
    subject: 'service.discovery.changed',
  });

  assert.deepStrictEqual(loaded.at(-1), {
    name: 'supportChat',
    contracts: actions,
  });

  actions.length = 0;
  provider.subscribeDiscovery();
  await subscription.callback(null, {
    subject: 'service.discovery.changed',
  });
  assert.strictEqual(loaded.length, 2);

  provider.updateDiscovery();
  assert.strictEqual(
    connection.published.at(-1).subject,
    'service.discovery.changed',
  );
});

test('lib/nats - should fail discovery without providers', async () => {
  const application = {
    config: { service: { discovery: { maxWait: 100 } } },
    service: {
      collection: { supportChat: { default: 1 } },
      isRemote: () => true,
    },
  };
  const nats = new Nats(application);
  nats.connection = createConnection();

  await assert.rejects(nats.discoverServices(), {
    message: 'Service discovery failed',
  });
});

test('lib/nats - should publish and subscribe events', async () => {
  const invocations = [];
  const supportChat = {
    name: 'supportChat',
    eventNames: () => ['user:created'],
    dispatch: (...args) => invocations.push(args),
  };
  const location = {
    name: 'location',
    eventNames: () => ['user:created'],
    dispatch: (...args) => invocations.push(args),
  };
  const application = {
    console: { error() {} },
    service: {
      events: { supportChat, location },
    },
  };
  const subscriptions = [];
  const published = [];
  const nats = new Nats(application);
  nats.connection = {
    subscribe(subject, options) {
      const subscription = {
        subject,
        options,
        closed: false,
        unsubscribe() {
          this.closed = true;
        },
      };
      subscriptions.push(subscription);
      return subscription;
    },
    publish: (subject, payload) => published.push({ subject, payload }),
  };

  nats.subscribeEvents();
  nats.subscribeEvents();

  assert.strictEqual(subscriptions.length, 2);
  assert.strictEqual(subscriptions[0].subject, 'user.created');
  assert.strictEqual(subscriptions[0].options.queue, 'supportChat');
  assert.strictEqual(subscriptions[1].options.queue, 'location');

  const payload = { userId: 'user-1' };
  nats.publishEvent('user:created', payload);
  assert.deepStrictEqual(published, [
    { subject: 'user.created', payload: JSON.stringify(payload) },
  ]);

  const message = { json: () => payload };
  const currentInvocations = [];
  application.service.events.supportChat = {
    ...supportChat,
    dispatch: (...args) => currentInvocations.push(args),
  };
  application.service.events.location = {
    ...location,
    dispatch: (...args) => currentInvocations.push(args),
  };
  await subscriptions[0].options.callback(null, message);
  await subscriptions[1].options.callback(null, message);

  assert.deepStrictEqual(invocations, []);
  assert.deepStrictEqual(currentInvocations, [
    ['user:created', payload],
    ['user:created', payload],
  ]);

  delete application.service.events.supportChat;
  delete application.service.events.location;
  nats.subscribeEvents();
  assert.strictEqual(subscriptions[0].closed, true);
  assert.strictEqual(subscriptions[1].closed, true);
  assert.strictEqual(nats.eventSubscriptions.size, 0);
});
