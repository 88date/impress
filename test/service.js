'use strict';

const { before, test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const path = require('node:path');
const metavm = require('metavm');
const { DomainError } = require('metautil');
const { Broker } = require('../lib/broker.js');
const { Service } = require('../lib/service.js');

const root = process.cwd();

const application = {
  path: path.join(root, 'test'),
  sandbox: metavm.createContext({ service: {} }),
  watcher: { watch() {} },
  console: { error() {} },
  contextStorage: new AsyncLocalStorage(),
  absolute(relative) {
    return path.join(this.path, relative);
  },
  config: { server: { timeouts: {} } },
};

let service = null;

before(async () => {
  service = new Service('service', application);
  application.service = service;
  await service.load();
});

test('lib/service load - should load service correctly', async () => {
  const { example } = service.collection;
  assert.strictEqual(example.default, 1);
  assert.strictEqual(example['1'].add.constructor.name, 'Broker');
  assert.strictEqual(example['1'].add.unitName, 'example.1');
  assert.strictEqual(example['1'].add.actionName, 'add');
  assert.strictEqual(example['1'].add.config.location, 'local');
  assert.deepStrictEqual(Object.keys(example['1']), ['add']);
  assert.deepStrictEqual(Object.keys(application.sandbox.service.example), [
    'emit',
    'on',
    'add',
  ]);

  const config = service.configs['example.1'];
  assert.strictEqual(config.location, 'local');
  assert.strictEqual(config.versions.default, 1);
  assert.strictEqual(config.request.timeout, 5000);
  assert.strictEqual(config.discovery.maxWait, 1000);

  const eventBroker = service.events.example;
  const complete = eventBroker.collection['calculation:complete'];
  assert.strictEqual(eventBroker.constructor.name, 'EventBroker');
  assert.strictEqual(complete.exports.parameters.result, 'number');
  assert.throws(
    () => eventBroker.validate('calculation:complete', { result: 'invalid' }),
    (error) => error.message.startsWith('Invalid event parameters'),
  );

  const result = await application.sandbox.service.example.add({ a: 4, b: 6 });
  assert.strictEqual(result, 10);

  const direct = await application.sandbox.service.example.add({
    a: '4',
    b: 6,
  });
  assert.strictEqual(direct, '46');

  await assert.rejects(example['1'].add.invoke({}, { a: '4', b: 6 }), (error) =>
    error.message.startsWith('Invalid parameters'),
  );
});

test('lib/service version - should derive version from directory', async () => {
  const { versioned } = service.collection;
  assert.strictEqual(versioned.default, 1);
  assert.strictEqual(versioned['2'].ping.constructor.name, 'Broker');

  const config = service.configs['versioned.2'];
  assert.strictEqual(config.location, 'local');
  assert.strictEqual(config.versions.default, 2);
  assert.strictEqual(config.request.timeout, 5000);

  const result = await application.sandbox.service.versioned.ping();
  assert.strictEqual(result, 'pong');
});

test('lib/service metadata - should reload config and events', async () => {
  const unitPath = path.join(root, 'test', 'service', 'example');
  const configPath = path.join(unitPath, '.service.js');
  const eventsPath = path.join(unitPath, '.events.js');

  service.delete(configPath);
  service.delete(eventsPath);
  assert.strictEqual(service.configs['example.1'], undefined);
  assert.deepStrictEqual(service.events.example.collection, {});

  await service.change(configPath);
  await service.change(eventsPath);
  assert.strictEqual(service.configs['example.1'].location, 'local');
  assert.strictEqual(
    service.events.example.collection['calculation:complete'].exports.parameters
      .result,
    'number',
  );
});

test('lib/service events - should select one handler per group', async () => {
  service.prepareUnit('supportChat.1');
  service.prepareUnit('location.1');
  const { example, supportChat, location } = application.sandbox.service;
  const calls = { first: 0, second: 0, location: 0 };
  const contexts = [];

  supportChat.on('example:calculation:complete', async () => {
    calls.first++;
    contexts.push(application.contextStorage.getStore());
  });
  supportChat.on('example:calculation:complete', async () => {
    calls.second++;
    contexts.push(application.contextStorage.getStore());
  });
  location.on('example:calculation:complete', async () => {
    calls.location++;
    contexts.push(application.contextStorage.getStore());
  });

  const context = { session: { state: { userId: 'user-1' } } };
  await application.contextStorage.run(context, () =>
    example.emit('calculation:complete', { result: 1 }),
  );
  await example.emit('calculation:complete', { result: 2 });

  assert.deepStrictEqual(calls, { first: 1, second: 1, location: 2 });
  assert.deepStrictEqual(contexts, [null, null, null, null]);

  const previous = service.events.example.collection['calculation:complete'];
  const eventBroker = service.events.supportChat;
  const eventsPath = path.join(
    root,
    'test',
    'service',
    'example',
    '.events.js',
  );
  await service.change(eventsPath);
  const current = service.events.example.collection['calculation:complete'];
  assert.notStrictEqual(current, previous);
  assert.strictEqual(service.events.supportChat, eventBroker);

  await example.emit('calculation:complete', { result: 3 });
  assert.deepStrictEqual(calls, { first: 2, second: 1, location: 3 });
});

test('lib/service events - should use NATS when connected', async () => {
  const calls = [];
  application.nats = {
    publishEvent: (...args) => calls.push(['emit', ...args]),
    subscribeEvent: (broker, eventName) =>
      calls.push(['on', broker.name, eventName]),
  };
  service.prepareUnit('notifications.1');
  const { example, notifications } = application.sandbox.service;
  const handler = async () => {};

  notifications.on('example:calculation:complete', handler);
  await example.emit('calculation:complete', { result: 3 });

  assert.deepStrictEqual(calls, [
    ['on', 'notifications', 'example:calculation:complete'],
    ['emit', 'example:calculation:complete', { result: 3 }],
  ]);
  application.nats = null;
});

test('lib/service delete - should use version 1 by default', async () => {
  const actionPath = path.join(root, 'test', 'service', 'example', 'add.js');

  service.delete(actionPath);

  assert.strictEqual(service.collection.example['1'].add, undefined);
  assert.strictEqual(application.sandbox.service.example.add, undefined);
  assert.strictEqual(service.signatures['example.1'].add, undefined);

  await service.change(actionPath);
});

test('lib/service reload - should update NATS subscriptions', () => {
  const subscribed = [];
  const unsubscribed = [];
  application.nats = {
    subscribeService: (broker) => subscribed.push(broker.subject),
    unsubscribeService: (subject) => unsubscribed.push(subject),
  };
  const script = () => ({
    access: 'public',
    method: async () => 'reloaded',
  });
  const broker = new Broker(script, 'method', 'example.1', application);

  service.changeUnit('example.1', 'reload', broker);

  assert.deepStrictEqual(subscribed, ['example.1.reload']);

  const actionPath = path.join(root, 'test', 'service', 'example', 'reload.js');
  service.delete(actionPath);

  assert.deepStrictEqual(unsubscribed, ['example.1.reload']);
  application.nats = null;
});

test('lib/service delete - should preserve another version', async () => {
  const v1 = new Broker(
    () => ({ access: 'public', method: async () => 1 }),
    'method',
    'example.1',
    application,
  );
  const v2 = new Broker(
    () => ({ access: 'public', method: async () => 2 }),
    'method',
    'example.2',
    application,
  );
  service.changeUnit('example.1', 'shared', v1);
  service.changeUnit('example.2', 'shared', v2);

  const actionPath = path.join(
    root,
    'test',
    'service',
    'example.2',
    'shared.js',
  );
  service.delete(actionPath);

  const result = await application.sandbox.service.example.shared();
  assert.strictEqual(result, 1);

  const v1Path = path.join(root, 'test', 'service', 'example', 'shared.js');
  service.delete(v1Path);
});

test('lib/broker access - should require session for logged', async () => {
  const script = () => ({
    access: 'logged',
    method: async () => 'allowed',
  });
  const broker = new Broker(script, 'method', 'example.1', application);
  service.changeUnit('example.1', 'private', broker);

  assert.throws(() => application.sandbox.service.example.private(), {
    message: 'Authentication required',
  });
  await assert.rejects(broker.invoke({ session: null }), {
    message: 'Authentication required',
  });

  const context = { session: { token: 'token', state: { userId: 'user-1' } } };
  const result = await application.contextStorage.run(context, () =>
    application.sandbox.service.example.private(),
  );
  assert.strictEqual(result, 'allowed');
  assert.strictEqual(await broker.invoke(context), 'allowed');

  const actionPath = path.join(
    root,
    'test',
    'service',
    'example',
    'private.js',
  );
  service.delete(actionPath);
});

test('lib/broker - should map domain error message', async () => {
  const script = () => ({
    access: 'public',
    errors: { EFAIL: 'Operation failed' },
    method: async () => {
      throw new DomainError('EFAIL');
    },
  });
  const broker = new Broker(script, 'method', 'example.1', application);
  service.changeUnit('example.1', 'fail', broker);

  await assert.rejects(broker.invoke({}, {}), {
    message: 'Operation failed',
    code: 'EFAIL',
  });
  await assert.rejects(application.sandbox.service.example.fail(), {
    message: 'Operation failed',
    code: 'EFAIL',
  });

  const actionPath = path.join(root, 'test', 'service', 'example', 'fail.js');
  service.delete(actionPath);
});

test('lib/broker - should route remote calls through NATS', async () => {
  const calls = [];
  const remoteApplication = {
    contextStorage: new AsyncLocalStorage(),
    schemas: null,
    service: {
      configs: {
        'example.1': {
          location: 'remote',
          versions: { default: 1, add: 2 },
          request: { timeout: 5000 },
        },
      },
    },
    nats: {
      request: async (...args) => {
        calls.push(args);
        return 10;
      },
    },
  };
  const script = () => ({
    access: 'public',
    errors: { EFAIL: 'Operation failed' },
    method: async () => 0,
  });
  const broker = new Broker(script, 'method', 'example.1', remoteApplication);
  broker.actionName = 'add';

  const result = await broker.call({ a: 4, b: 6 });

  assert.strictEqual(result, 10);
  assert.strictEqual(broker.subject, 'example.1.add');
  assert.strictEqual(broker.requestSubject, 'example.2.add');
  assert.deepStrictEqual(calls, [['example.2.add', { a: 4, b: 6 }, 5000]]);

  remoteApplication.nats.request = async () => {
    throw new DomainError('EFAIL').toError({ EFAIL: 'Operation failed' });
  };
  await assert.rejects(broker.call(), {
    message: 'Operation failed',
    code: 'EFAIL',
  });

  broker.actionName = 'remove';
  assert.strictEqual(broker.requestSubject, 'example.1.remove');
});

test('lib/broker - should use configured local version', async () => {
  const localApplication = {
    contextStorage: new AsyncLocalStorage(),
    schemas: null,
    service: {
      configs: {
        'example.1': {
          location: 'local',
          versions: { default: 1, add: 2 },
        },
      },
      collection: null,
    },
  };
  const v1 = new Broker(
    () => ({ access: 'public', method: async () => 1 }),
    'method',
    'example.1',
    localApplication,
  );
  const v2 = new Broker(
    () => ({ access: 'public', method: async () => 2 }),
    'method',
    'example.2',
    localApplication,
  );
  v1.actionName = 'add';
  v2.actionName = 'add';
  localApplication.service.collection = {
    example: { default: 1, 1: { add: v1 }, 2: { add: v2 } },
  };

  const result = await v1.call();

  assert.strictEqual(result, 2);
});
