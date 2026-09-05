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
    server: {
      timeouts: {},
      nats: { discovery: { maxWait: 1000 } },
    },
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
  assert.deepStrictEqual(Array.from(example['1'].add.transports), ['nats']);
  assert.strictEqual(example['1'].add.timeout, 5000);
  assert.deepStrictEqual(Object.keys(example['1']), ['add']);
  assert.deepStrictEqual(Object.keys(application.sandbox.service.example), [
    'add',
  ]);

  assert.strictEqual(application.config.server.nats.discovery.maxWait, 1000);
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
  assert.deepStrictEqual(Array.from(metadata.actions[0].transports), ['nats']);
  assert.strictEqual('method' in metadata.actions[0], false);

  assert.strictEqual(application.sandbox.service.example.emit, undefined);
  assert.strictEqual(application.sandbox.service.example.on, undefined);
});

test('lib/service version - should derive version from directory', async () => {
  const { versioned } = service.collection;
  assert.strictEqual(versioned.default, 2);
  assert.strictEqual(versioned['2'].ping.constructor.name, 'Broker');
  assert.strictEqual(versioned['2'].ping.timeout, 5000);

  const result = await application.sandbox.service.versioned.ping();
  assert.strictEqual(result, 'pong');
});

test('lib/service discovery - should load remote contracts', async () => {
  const name = 'remoteChat';
  const declaration = new Broker(
    () => ({ transports: ['nats'], access: 'public', timeout: 5000 }),
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
    transports: ['nats'],
    deprecated: false,
    examples: null,
  };
  service.loadRemote(name, [contract]);

  const broker = service.collection.remoteChat['1'].sendMessage;
  assert.strictEqual(broker.discovered, true);
  assert.strictEqual(broker.script, null);
  assert.strictEqual(broker.caption, 'Send message');
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

  service.loadRemote(name, [{ ...contract, name: 'createConversation' }]);
  assert.strictEqual(
    typeof application.sandbox.service.remoteChat.sendMessage,
    'function',
  );
  assert.strictEqual(
    typeof application.sandbox.service.remoteChat.createConversation,
    'function',
  );

  application.nats = null;
  delete service.collection[name];
  delete application.sandbox.service[name];
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
    transports: ['nats'],
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
    () => ({ transports: ['nats'], access: 'public', method: async () => 1 }),
    'method',
    'example.1',
    application,
  );
  const v2 = new Broker(
    () => ({ transports: ['nats'], access: 'public', method: async () => 2 }),
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

test('lib/broker - should default transports to an empty array', () => {
  const broker = new Broker(
    () => ({ method: async () => {} }),
    'method',
    'example.1',
    application,
  );

  assert.deepStrictEqual(broker.transports, []);
});

test('lib/broker access - should require session for logged', async () => {
  const script = () => ({
    transports: ['nats'],
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
    transports: ['nats'],
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
    transports: ['nats'],
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
    () => ({
      transports: ['nats'],
      access: 'public',
      timeout: 5000,
      method: async () => 2,
    }),
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
    {
      name: 'add',
      transports: ['nats'],
      access: 'public',
      timeout: 5000,
    },
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

test('lib/service - should select an explicit local version', async () => {
  const localApplication = {
    ...application,
    sandbox: { service: {} },
    nats: null,
  };
  const localService = new Service('service', localApplication);
  localApplication.service = localService;
  for (const version of [1, 2]) {
    const unitName = `example.${version}`;
    const script = () => ({
      transports: ['nats'],
      access: 'public',
      method: async (args) => ({ version, args }),
    });
    const broker = new Broker(script, 'method', unitName, localApplication);
    localService.changeUnit(unitName, 'echo', broker);
    if (version === 1) {
      const legacy = new Broker(script, 'method', unitName, localApplication);
      localService.changeUnit(unitName, 'legacy', legacy);
    }
  }
  const { echo, legacy } = localApplication.sandbox.service.example;
  const args = { value: 42 };

  assert.deepStrictEqual(await echo(args, { version: 1 }), {
    version: 1,
    args,
  });
  assert.deepStrictEqual(await echo(args), { version: 2, args });
  assert.deepStrictEqual(await echo(args, {}), { version: 2, args });
  assert.deepStrictEqual(await legacy(args, { version: 1 }), {
    version: 1,
    args,
  });
  await assert.rejects(legacy(args, { version: 2 }), {
    message: 'Service action is not available: example.2.legacy',
  });
  await assert.rejects(echo(args, { version: 3 }), {
    message: 'Service action is not available: example.3.echo',
  });
});

test('lib/service - should pin remote versions across updates', async () => {
  const calls = [];
  const remoteApplication = {
    ...application,
    sandbox: { service: {} },
    nats: {
      subscribeService() {},
      request: async (...args) => {
        calls.push(args);
        return 'sent';
      },
    },
  };
  const remoteService = new Service('service', remoteApplication);
  remoteApplication.service = remoteService;
  const contract = {
    name: 'echo',
    version: 1,
    transports: ['nats'],
    access: 'public',
    timeout: 5000,
  };
  remoteService.loadRemote('example', [contract]);
  const { echo } = remoteApplication.sandbox.service.example;
  const args = { value: 42 };
  await echo(args, { version: 1 });

  remoteService.loadRemote('example', [contract, { ...contract, version: 2 }]);
  await echo(args, { version: 1 });
  await echo(args, { version: 2 });
  await echo(args);
  await echo(args, {});
  await assert.rejects(echo(args, { version: 3 }), {
    message: 'Service action is not available: example.3.echo',
  });

  assert.deepStrictEqual(calls, [
    ['example.1.echo', args, 5000],
    ['example.1.echo', args, 5000],
    ['example.2.echo', args, 5000],
    ['example.2.echo', args, 5000],
    ['example.2.echo', args, 5000],
  ]);
});
