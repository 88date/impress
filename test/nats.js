'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const { DomainError } = require('metautil');
const { npm } = require('../lib/deps.js');
const { Nats } = require('../lib/nats.js');

const createConnection = () => {
  const requests = [];
  const subscriptions = new Map();
  const connection = {
    requests,
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
  const context = { session, state: { internal: true } };

  const result = await contextStorage.run(context, () =>
    nats.request('example.1.add', { a: 4, b: 6 }, 5000),
  );

  assert.strictEqual(result, 10);
  assert.deepStrictEqual(receivedContext, { session });
  const subscription = nats.connection.subscriptions.get('example.1.add');
  assert.strictEqual(subscription.queue, 'example.1.add');
  const request = nats.connection.requests[0];
  assert.strictEqual(request.subject, 'example.1.add');
  assert.strictEqual(request.options.timeout, 5000);
});

test('lib/nats - should transfer domain and internal errors', async () => {
  const logged = [];
  const application = {
    console: { error: (error) => logged.push(error) },
    contextStorage: new AsyncLocalStorage(),
  };
  const nats = new Nats(application);
  nats.connection = createConnection();
  nats.subscribe('example.domain', async () => {
    throw new DomainError('EFAIL');
  });
  nats.subscribe('example.internal', async () => {
    throw new globalThis.Error('Sensitive details');
  });

  await assert.rejects(nats.request('example.domain', {}, 5000), {
    message: 'Domain error',
    code: 'EFAIL',
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
    config: { location: 'local' },
    serviceName: 'example',
    version: 1,
    actionName: 'update',
    subject: 'example.1.update',
    invoke: async (context, args) => calls.push({ context, args }),
  };
  const localV2 = {
    config: { location: 'local' },
    serviceName: 'example',
    version: 2,
    actionName: 'get',
    subject: 'example.2.get',
    invoke: async (context, args) => calls.push({ context, args }),
  };
  const remote = { subject: 'remote.get' };
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
      configs: {
        'example.1': { location: 'local' },
        'remote.1': { location: 'remote' },
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

test('lib/nats - should publish and subscribe events', async () => {
  const invocations = [];
  const supportChat = {
    name: 'supportChat',
    eventNames: () => ['user:created'],
    invoke: (...args) => invocations.push(args),
  };
  const location = {
    name: 'location',
    eventNames: () => ['user:created'],
    invoke: (...args) => invocations.push(args),
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
    invoke: (...args) => currentInvocations.push(args),
  };
  application.service.events.location = {
    ...location,
    invoke: (...args) => currentInvocations.push(args),
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
