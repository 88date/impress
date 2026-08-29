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
const logs = [];

const application = {
  path: path.join(root, 'test'),
  sandbox: metavm.createContext({ service: {} }),
  watcher: { watch() {} },
  console: {
    log: (message) => logs.push(['log', message]),
    error: (message) => logs.push(['error', message]),
  },
  contextStorage: new AsyncLocalStorage(),
  absolute(relative) {
    return path.join(this.path, relative);
  },
  config: {
    server: { timeouts: {} },
    service: { discovery: { maxWait: 1000 } },
  },
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
  assert.strictEqual(example['1'].add.timeout, 5000);
  assert.deepStrictEqual(Object.keys(example['1']), ['add']);
  assert.deepStrictEqual(Object.keys(application.sandbox.service.example), [
    'emit',
    'on',
    'add',
  ]);

  const config = service.configs['example.1'];
  assert.strictEqual(config.location, 'local');
  assert.strictEqual(application.config.service.discovery.maxWait, 1000);
  assert.strictEqual(example['1'].skipped, undefined);
  assert.strictEqual(example['1'].disabled, undefined);

  const { remote } = service.collection;
  assert.strictEqual(remote.default, 2);
  assert.strictEqual(remote['2'].sendMessage.method, undefined);
  assert.strictEqual(service.isRemote('remote'), true);
  assert.strictEqual(
    typeof application.sandbox.service.remote.sendMessage,
    'function',
  );

  const metadata = service.describe('example');
  assert.strictEqual(metadata.name, 'example');
  assert.strictEqual(metadata.actions[0].name, 'add');
  assert.strictEqual(metadata.actions[0].version, 1);
  assert.strictEqual(metadata.actions[0].timeout, 5000);
  assert.strictEqual('method' in metadata.actions[0], false);
  assert.deepStrictEqual(metadata.events, []);

  const eventBroker = service.events.example;
  assert.strictEqual(eventBroker.constructor.name, 'EventBroker');
  assert.deepStrictEqual(eventBroker.collection, {});

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
  assert.strictEqual(versioned.default, 2);
  assert.strictEqual(versioned['2'].ping.constructor.name, 'Broker');
  assert.strictEqual(versioned['2'].ping.timeout, 5000);

  const config = service.configs['versioned.2'];
  assert.strictEqual(config.location, 'local');

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
  assert.deepStrictEqual(service.events.example.collection, {});
});

test('lib/service discovery - should load remote contracts', async () => {
  const name = 'remoteChat';
  const declaration = new Broker(
    () => ({ service: true, access: 'public', timeout: 5000 }),
    'method',
    `${name}.1`,
    application,
  );
  service.changeUnit(`${name}.1`, 'sendMessage', declaration);
  assert.strictEqual(service.isRemote(name), true);
  assert.strictEqual(service.isRemote('example'), false);
  const contract = {
    name: 'sendMessage',
    version: 1,
    access: 'public',
    parameters: { text: 'string' },
    returns: 'string',
    errors: null,
    caption: 'Send message',
    description: '',
    timeout: 5000,
    deprecated: false,
    examples: null,
  };
  const event = {
    name: 'message:created',
    parameters: { conversationId: 'string' },
    caption: 'Message created',
    description: '',
    deprecated: false,
    examples: null,
  };

  service.loadRemote(name, [contract], [event]);

  const broker = service.collection.remoteChat['1'].sendMessage;
  assert.strictEqual(broker.discovered, true);
  assert.strictEqual(broker.script, null);
  assert.strictEqual(broker.caption, 'Send message');
  assert.deepStrictEqual(service.events.remoteChat.collection, {});
  assert.strictEqual(
    typeof application.sandbox.service.remoteChat.sendMessage,
    'function',
  );

  const requests = [];
  application.nats = {
    request: async (...args) => {
      requests.push(args);
      return 'sent';
    },
    subscribeService() {},
  };
  const result = await application.sandbox.service.remoteChat.sendMessage({
    text: 'Hello',
  });

  assert.strictEqual(result, 'sent');
  assert.deepStrictEqual(requests, [
    ['remoteChat.1.sendMessage', { text: 'Hello' }, 5000],
  ]);

  const updatedEvent = { ...event, name: 'conversation:created' };
  service.loadRemote(
    name,
    [{ ...contract, name: 'createConversation' }],
    [updatedEvent],
  );
  assert.strictEqual(
    application.sandbox.service.remoteChat.sendMessage,
    undefined,
  );
  assert.strictEqual(
    typeof application.sandbox.service.remoteChat.createConversation,
    'function',
  );
  assert.deepStrictEqual(service.events.remoteChat.collection, {});

  application.nats = null;
  delete service.collection[name];
  delete service.events[name];
  delete application.sandbox.service[name];
});

test('lib/service events - should select one handler per group', async () => {
  service.prepareUnit('supportChat.1');
  service.prepareUnit('location.1');
  service.events.example.load({
    'calculation:complete': {
      parameters: { result: 'number' },
    },
  });
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
  assert.strictEqual(current, previous);
  assert.strictEqual(service.events.supportChat, eventBroker);

  await example.emit('calculation:complete', { result: 3 });
  assert.deepStrictEqual(calls, { first: 2, second: 1, location: 3 });
});

test('lib/service events - should use NATS when connected', async () => {
  const calls = [];
  service.events.example.load({
    'calculation:complete': {
      parameters: { result: 'number' },
    },
  });
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

  assert.strictEqual(service.collection.example['1'], undefined);
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
    updateDiscovery() {},
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

test('lib/service delete - should remove event subscriptions', () => {
  const name = 'subscriber';
  const unitName = `${name}.1`;
  service.configs[unitName] = { location: 'local' };
  const { eventBroker } = service.prepareUnit(unitName);
  eventBroker.on('example:calculation:complete', () => {});
  eventBroker.indexes.set('example:calculation:complete', 1);
  const calls = [];
  application.nats = {
    subscribeServices: () => calls.push('services'),
    subscribeEvents: () => calls.push('events'),
    updateDiscovery: () => calls.push('discovery'),
  };

  const configPath = path.join(root, 'test', 'service', name, '.service.js');
  service.delete(configPath);

  assert.strictEqual(
    eventBroker.listenerCount('example:calculation:complete'),
    0,
  );
  assert.strictEqual(eventBroker.indexes.size, 0);
  assert.deepStrictEqual(calls, ['services', 'discovery']);

  application.nats = null;
  delete service.collection[name];
  delete service.events[name];
  delete application.sandbox.service[name];
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

  await assert.rejects(application.sandbox.service.example.private(), {
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
  const [level, message] = logs.at(-1);
  assert.strictEqual(level, 'error');
  assert.match(
    message,
    /^-\tservice\texample\.1\/fail\t200\tEFAIL\tError: Operation failed/,
  );

  const actionPath = path.join(root, 'test', 'service', 'example', 'fail.js');
  service.delete(actionPath);
});

test('lib/broker - should log service calls', async () => {
  const context = { client: { ip: '127.0.0.1' } };
  await application.contextStorage.run(context, () =>
    application.sandbox.service.example.add({ a: 4, b: 6 }),
  );

  assert.deepStrictEqual(logs.at(-1), [
    'log',
    '127.0.0.1\tservice\texample.1/add',
  ]);
});

test('lib/broker - should route remote calls through NATS', async () => {
  const calls = [];
  const logs = [];
  const remoteApplication = {
    console: {
      log: (...args) => logs.push(['log', ...args]),
      error: (...args) => logs.push(['error', ...args]),
    },
    contextStorage: new AsyncLocalStorage(),
    config: { server: { timeouts: {} } },
    schemas: null,
    nats: {
      request: async (...args) => {
        calls.push(args);
        return 10;
      },
    },
  };
  const contract = {
    name: 'add',
    access: 'public',
    errors: { EFAIL: 'Operation failed' },
    timeout: 5000,
  };
  const broker = Broker.fromContract(contract, 'example.2', remoteApplication);

  const result = await broker.call({ a: 4, b: 6 });

  assert.strictEqual(result, 10);
  assert.strictEqual(broker.subject, 'example.2.add');
  assert.deepStrictEqual(calls, [['example.2.add', { a: 4, b: 6 }, 5000]]);
  assert.deepStrictEqual(logs, []);

  remoteApplication.nats.request = async () => {
    throw new DomainError('EFAIL').toError({ EFAIL: 'Operation failed' });
  };
  await assert.rejects(broker.call(), {
    message: 'Operation failed',
    code: 'EFAIL',
  });

  broker.actionName = 'remove';
  assert.strictEqual(broker.subject, 'example.2.remove');
});

test('lib/service - should select maximum version', async () => {
  const namespaceMethod = application.sandbox.service.example.add;
  const v2 = new Broker(
    () => ({ access: 'public', timeout: 5000, method: async () => 2 }),
    'method',
    'example.2',
    application,
  );
  service.changeUnit('example.2', 'add', v2);

  const localResult = await application.sandbox.service.example.add();

  assert.strictEqual(localResult, 2);
  assert.strictEqual(application.sandbox.service.example.add, namespaceMethod);

  const calls = [];
  const v3 = Broker.fromContract(
    { name: 'add', access: 'public', timeout: 5000 },
    'example.3',
    application,
  );
  service.changeUnit('example.3', 'add', v3);
  application.nats = {
    request: async (...args) => {
      calls.push(args);
      return 3;
    },
  };
  const remoteResult = await application.sandbox.service.example.add();

  assert.strictEqual(remoteResult, 3);
  assert.deepStrictEqual(calls, [['example.3.add', {}, 5000]]);

  application.nats = null;
  const v3Path = path.join(root, 'test', 'service', 'example.3', 'add.js');
  const v2Path = path.join(root, 'test', 'service', 'example.2', 'add.js');
  service.delete(v3Path);
  service.delete(v2Path);
});
