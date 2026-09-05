'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const { MessageChannel } = require('node:worker_threads');
const { Error } = require('metautil');
const { npm } = require('../lib/deps.js');
const { Broker } = require('../lib/broker.js');
const { ServiceCatalog, DiscoveryWorker } = require('../lib/catalog.js');
const {
  Nats,
  EVENT_DISCOVERY_SUBJECT,
  EVENT_DISCOVERY_CHANGED_SUBJECT,
} = require('../lib/nats.js');
const { Service } = require('../lib/service.js');

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
        Promise.resolve()
          .then(() => subscription.callback(null, message))
          .catch(() => {});
      }
    },
    async flush() {},
    async drain() {},
    async *status() {},
  };
  return connection;
};

const createApplication = (kind) => {
  const application = {
    kind,
    console: { log() {}, error() {} },
    contextStorage: new AsyncLocalStorage(),
    sandbox: { service: {} },
    config: {
      server: {
        nats: { discovery: { maxWait: 100 } },
        timeouts: { start: 1000, request: 1000 },
      },
    },
    absolute: (name) => name,
  };
  application.service = new Service('api', application);
  return application;
};

const addAction = (application, name, method) => {
  const script = () => ({ transports: ['nats'], access: 'public', method });
  const broker = new Broker(script, 'method', 'example.1', application);
  application.service.changeUnit('example.1', name, broker);
};

test('lib/nats - should load NATS transport', () => {
  assert.strictEqual(typeof npm.nats.connect, 'function');
  assert.strictEqual(typeof npm.nats.credsAuthenticator, 'function');
});

test('lib/nats - should require configuration', async () => {
  const application = { config: { server: { nats: { enabled: true } } } };
  const nats = new Nats(application);
  await assert.rejects(() => nats.start(), {
    message: 'NATS servers and credentials expected',
  });
});

test('lib/nats - should register server RPC on start and reload', async () => {
  const application = createApplication('server');
  addAction(application, 'echo', async ({ value }) => value);
  const nats = new Nats(application);
  application.nats = nats;
  const connection = createConnection();
  nats.connect = async () => {
    nats.connection = connection;
  };

  await nats.start();

  assert.deepStrictEqual(
    [...nats.serviceSubscriptions.keys()],
    ['example.1.echo'],
  );
  assert.deepStrictEqual([...nats.discoverySubscriptions.keys()], ['example']);
  assert.ok(nats.discoveryCatalogSubscription);
  assert.ok(nats.discoveryChangeSubscription);
  assert.strictEqual(
    await nats.request('example.1.echo', { value: 42 }, 1000),
    42,
  );
  assert.strictEqual(connection.published.length, 1);
  assert.strictEqual(
    connection.requests.filter(({ subject }) => subject === 'service.discovery')
      .length,
    1,
  );

  addAction(application, 'echo', async () => 'updated');
  addAction(application, 'created', async () => 'created');

  assert.strictEqual(await nats.request('example.1.echo', {}, 1000), 'updated');
  assert.strictEqual(
    await nats.request('example.1.created', {}, 1000),
    'created',
  );
  assert.strictEqual(nats.serviceSubscriptions.size, 2);
  assert.strictEqual(connection.published.length, 3);
});

test('lib/nats - should keep other workers as RPC clients', async () => {
  for (const kind of ['worker', 'balancer']) {
    const provider = createApplication('server');
    addAction(provider, 'remote', async ({ value }) => value);
    const server = new Nats(provider);
    const connection = createConnection();
    server.connection = connection;
    const requestMany = connection.requestMany.bind(connection);
    let attempts = 0;
    connection.requestMany = async (...args) => {
      attempts++;
      if (attempts === 1) {
        throw new npm.nats.NoRespondersError('service.discovery');
      }
      server.subscribeServices();
      server.subscribeDiscovery();
      server.subscribeDiscoveryCatalog();
      return requestMany(...args);
    };
    const application = createApplication(kind);
    addAction(application, 'local', async () => 'local');
    const client = new Nats(application);
    application.nats = client;
    client.connect = async () => {
      client.connection = connection;
    };

    await client.start();

    assert.strictEqual(attempts, 2);
    assert.strictEqual(client.serviceSubscriptions.size, 0);
    assert.strictEqual(client.discoverySubscriptions.size, 0);
    assert.strictEqual(client.discoveryCatalogSubscription, null);
    assert.ok(client.discoveryChangeSubscription);
    assert.deepStrictEqual(connection.published, []);
    const { example } = application.sandbox.service;
    assert.strictEqual(await example.remote({ value: 42 }), 42);
    assert.strictEqual(await example.local(), 'local');

    addAction(application, 'local', async () => 'updated');
    addAction(application, 'created', async () => 'created');

    assert.strictEqual(await example.local(), 'updated');
    assert.strictEqual(await example.created(), 'created');
    assert.strictEqual(client.serviceSubscriptions.size, 0);
    assert.strictEqual(client.discoverySubscriptions.size, 0);
    assert.deepStrictEqual(connection.published, []);
    await assert.rejects(client.request('example.1.local', {}, 1000), {
      message: 'Not Found',
      code: 404,
    });
    await assert.rejects(client.request('example.1.created', {}, 1000), {
      message: 'Not Found',
      code: 404,
    });
  }
});

test('lib/nats - should bound discovery wait during startup', async () => {
  const application = createApplication('worker');
  application.config.server.timeouts.start = 20;
  const nats = new Nats(application);
  nats.connect = async () => {
    nats.connection = createConnection();
  };

  await assert.rejects(nats.start(), { message: 'Service discovery failed' });
});

test(
  'lib/nats - should share catalog across workers',
  { timeout: 5000 },
  async (t) => {
    const threads = new Map();
    const discoveryWorker = new DiscoveryWorker(threads, {
      ports: [8000, 8001],
    });
    const master = new ServiceCatalog(threads, discoveryWorker);
    let gate = Promise.withResolvers();
    const services = [
      {
        name: 'example',
        actions: [
          { name: 'remote', version: 1, access: 'public', caption: 'first' },
        ],
      },
    ];
    const workers = [];
    const createWorker = (id, kind) => {
      const { port1, port2 } = new MessageChannel();
      threads.set(id, port1);
      const application = createApplication(kind);
      addAction(application, 'local', async () => kind);
      const discovery = {
        loader: discoveryWorker.register(id, kind),
        request: () => master.send(port1),
        publish: async (services) =>
          structuredClone(master.publish(port1, structuredClone(services))),
      };
      const nats = new Nats(application, discovery);
      application.nats = nats;
      const connection = createConnection();
      const started = Promise.withResolvers();
      let queries = 0;
      connection.requestMany = async () => {
        queries++;
        assert.strictEqual(discovery.loader, true);
        const data = structuredClone(services);
        started.resolve();
        await gate.promise;
        return {
          async *[Symbol.asyncIterator]() {
            yield { json: () => data };
          },
        };
      };
      connection.request = async (subject, payload) => {
        connection.requests.push({ subject, payload });
        return { json: () => ({ result: JSON.parse(payload).args.value }) };
      };
      nats.connect = async () => {
        nats.connection = connection;
      };
      port2.on('message', ({ snapshot }) => nats.applyCatalog(snapshot));
      const waitForRevision = (revision) => {
        if (nats.catalogRevision >= revision) return Promise.resolve();
        return new Promise((resolve) => {
          const listener = () => {
            if (nats.catalogRevision < revision) return;
            port2.off('message', listener);
            resolve();
          };
          port2.on('message', listener);
        });
      };
      const worker = {
        nats,
        application,
        connection,
        started,
        waitForRevision,
        queries: () => queries,
      };
      workers.push(worker);
      t.after(async () => {
        port1.close();
        port2.close();
        await nats.close();
      });
      return worker;
    };
    const owner = createWorker(1, 'server');
    const followers = [
      createWorker(2, 'worker'),
      createWorker(3, 'balancer'),
      createWorker(4, 'server'),
    ];
    const starts = followers.map(({ nats }) => nats.start());
    starts.push(owner.nats.start());
    await owner.started.promise;
    for (const follower of followers) assert.strictEqual(follower.queries(), 0);
    gate.resolve();
    await Promise.all(starts);
    await Promise.all(workers.map((worker) => worker.waitForRevision(1)));

    assert.strictEqual(owner.queries(), 1);
    for (const worker of workers) {
      const { nats, application, connection } = worker;
      assert.strictEqual(
        await application.sandbox.service.example.remote({ value: 42 }),
        42,
      );
      assert.strictEqual(connection.requests.length, 1);
      assert.strictEqual(
        await application.sandbox.service.example.local(),
        application.kind,
      );
      if (worker === owner) continue;
      assert.strictEqual(nats.discoveryChangeSubscription, null);
      await nats.discoverServices();
      assert.strictEqual(worker.queries(), 0);
    }

    const firstSnapshot = structuredClone(master.snapshot);
    services[0].actions[0].caption = 'updated';
    await owner.connection.subscriptions
      .get('service.discovery.changed')
      .callback(null);
    await Promise.all(workers.map((worker) => worker.waitForRevision(2)));
    for (const { application } of workers) {
      assert.strictEqual(
        application.service.collection.example['1'].remote.caption,
        'updated',
      );
    }

    const late = createWorker(5, 'worker');
    await late.nats.start();
    assert.strictEqual(late.nats.catalogRevision, 2);
    assert.strictEqual(late.queries(), 0);
    late.nats.applyCatalog(firstSnapshot);
    assert.strictEqual(
      late.application.service.collection.example['1'].remote.caption,
      'updated',
    );

    await owner.nats.close();
    gate = Promise.withResolvers();
    services[0].actions[0].caption = 'restarted';
    const replacement = createWorker(1, 'server');
    const restarting = replacement.nats.start();
    await replacement.started.promise;
    assert.strictEqual(
      (await late.nats.getCatalog()).get('example').get('1.remote').caption,
      'updated',
    );
    gate.resolve();
    await restarting;
    await late.waitForRevision(3);
    assert.strictEqual(
      late.application.service.collection.example['1'].remote.caption,
      'restarted',
    );
    assert.strictEqual(replacement.queries(), 1);
  },
);

test('lib/nats - should refresh changes during discovery', async () => {
  const application = createApplication('server');
  const nats = new Nats(application);
  nats.connection = createConnection();
  nats.subscribeDiscoveryChanges();
  const gate = Promise.withResolvers();
  let caption = 'first';
  let queries = 0;
  nats.connection.requestMany = async () => {
    const data = [
      { name: 'example', actions: [{ name: 'remote', version: 1, caption }] },
    ];
    queries++;
    if (queries === 1) await gate.promise;
    return {
      async *[Symbol.asyncIterator]() {
        yield { json: () => data };
      },
    };
  };

  const initial = nats.discoverServices();
  caption = 'updated';
  const update = nats.connection.subscriptions
    .get('service.discovery.changed')
    .callback(null);
  gate.resolve();
  await Promise.all([initial, update]);

  assert.strictEqual(queries, 2);
  assert.strictEqual(nats.catalogRevision, 2);
  assert.strictEqual(
    application.service.collection.example['1'].remote.caption,
    'updated',
  );
});

test('lib/nats - should reject catalog wait when closed', async () => {
  const application = createApplication('worker');
  const nats = new Nats(application, { loader: false, request() {} });
  const pending = nats.getCatalog();
  const rejected = assert.rejects(pending, {
    message: 'Service discovery stopped',
  });
  await nats.close();
  await rejected;
});

test('lib/nats - should ignore catalog updates after stop begins', async () => {
  const application = createApplication('worker');
  let requested = false;
  const nats = new Nats(application, {
    loader: false,
    request: () => {
      requested = true;
    },
  });
  const initial = nats.applyCatalog({ revision: 1, services: [] });
  const snapshot = {
    revision: 2,
    services: [{ name: 'example', actions: [{ name: 'remote', version: 1 }] }],
  };
  const gate = Promise.withResolvers();
  nats.connection = createConnection();
  nats.connection.drain = () => gate.promise;
  const closing = nats.close();
  try {
    assert.strictEqual(nats.applyCatalog(snapshot), initial);
    assert.strictEqual(nats.catalogRevision, 1);
    await assert.rejects(nats.getCatalog(), {
      message: 'Service discovery stopped',
    });
    await assert.rejects(nats.discoverServices(), {
      message: 'Service discovery stopped',
    });
    assert.strictEqual(requested, false);
  } finally {
    gate.resolve();
    await closing;
  }

  assert.strictEqual(nats.applyCatalog(snapshot), null);
  assert.strictEqual(nats.catalogRevision, 0);
  assert.deepStrictEqual(application.service.collection, {});
});

test('lib/nats - should stop pending catalog refresh on close', async () => {
  for (const phase of ['fetch', 'publish']) {
    const application = createApplication('server');
    const gate = Promise.withResolvers();
    const started = Promise.withResolvers();
    const services = [
      { name: 'example', actions: [{ name: 'remote', version: 1 }] },
    ];
    let queries = 0;
    let publications = 0;
    const nats = new Nats(application, {
      loader: true,
      publish: async () => {
        publications++;
        if (phase === 'publish') {
          started.resolve();
          await gate.promise;
        }
        return { revision: 1, services };
      },
    });
    const connection = createConnection();
    nats.connection = connection;
    nats.subscribeDiscoveryChanges();
    nats.fetchServices = async () => {
      queries++;
      if (phase === 'fetch') {
        started.resolve();
        await gate.promise;
      }
      return services;
    };
    const refreshing = nats.discoverServices();
    const rejected = assert.rejects(refreshing, {
      message: 'Service discovery stopped',
    });
    await started.promise;
    const { callback } = connection.subscriptions.get(
      'service.discovery.changed',
    );
    const changed = callback(null);
    await nats.close();
    gate.resolve();
    await Promise.all([rejected, changed]);
    await callback(null);
    await assert.rejects(nats.discoverServices(), {
      message: 'Service discovery stopped',
    });

    assert.strictEqual(queries, 1);
    assert.strictEqual(publications, phase === 'publish' ? 1 : 0);
    assert.strictEqual(nats.catalog, null);
    assert.strictEqual(nats.catalogRevision, 0);
    assert.strictEqual(nats.discoveryPromise, null);
    assert.deepStrictEqual(application.service.collection, {});
  }
});

test('lib/nats - should time out waiting for the shared catalog', async () => {
  const application = createApplication('worker');
  application.config.server.timeouts.start = 20;
  const nats = new Nats(application, { loader: false, request() {} });
  const connection = createConnection();
  nats.connect = async () => {
    nats.connection = connection;
  };

  await assert.rejects(nats.start(), { code: 'ETIMEOUT' });
  assert.deepStrictEqual(connection.requests, []);
  await nats.close();
});

test('lib/nats - should not retry invalid discovery responses', async () => {
  const application = createApplication('worker');
  const nats = new Nats(application);
  const connection = createConnection();
  nats.connection = connection;
  connection.subscribe('service.discovery', {
    callback: (error, message) => message.respond('invalid JSON'),
  });

  await assert.rejects(nats.discoverServices(1000), (error) => {
    assert.strictEqual(error.message, 'Service discovery failed');
    assert.ok(error.cause instanceof SyntaxError);
    return true;
  });
  assert.strictEqual(connection.requests.length, 1);
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

test('lib/nats - should preserve roles after reconnect', async () => {
  const cases = [
    ['server', true],
    ['server', false],
    ['worker', true],
    ['worker', false],
    ['balancer', true],
    ['balancer', false],
  ];
  for (const [kind, loader] of cases) {
    const nats = new Nats({ kind, console: { error() {} } }, { loader });
    nats.connection = createConnection();
    nats.connection.status = async function* () {
      yield { type: 'reconnect' };
    };
    let discovered = false;
    nats.discoverServices = async () => {
      discovered = true;
    };

    await nats.watchStatus();

    assert.strictEqual(discovered, loader);
    const published =
      kind === 'server'
        ? [{ subject: 'service.discovery.changed', payload: undefined }]
        : [];
    assert.deepStrictEqual(nats.connection.published, published);
  }
});

test('lib/nats - should announce service catalog once', () => {
  const nats = new Nats({ kind: 'server' });
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
    kind: 'server',
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
  nats.connection = createConnection();
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
    kind: 'server',
    console: { error() {} },
    config: { server: { nats: { discovery: { maxWait: 250 } } } },
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
    kind: 'worker',
    console: { error() {} },
    config: { server: { nats: { discovery: { maxWait: 250 } } } },
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
    config: { server: { nats: { discovery: { maxWait: 100 } } } },
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

test('lib/nats - should discover events from the selected server', async () => {
  const events = [
    {
      name: 'chat:1:message:created',
      subject: 'chat.1.message.created',
      caption: 'Message created',
      description: '',
      examples: null,
    },
  ];
  const application = createApplication('server');
  application.subscriptions = { describeEvents: () => events };
  const requests = [];
  const publications = [];
  const eventDiscovery = {
    loader: true,
    request: () => requests.push('request'),
    publish: async (discovered) => {
      publications.push(structuredClone(discovered));
      return { revision: publications.length, events: discovered };
    },
  };
  const nats = new Nats(application, null, eventDiscovery);
  const connection = createConnection();
  nats.connect = async () => {
    nats.connection = connection;
  };

  await nats.start();

  assert.deepStrictEqual(requests, ['request']);
  assert.deepStrictEqual(publications, [events]);
  assert.deepStrictEqual(
    connection.requests.map(({ subject }) => subject),
    ['service.discovery', EVENT_DISCOVERY_SUBJECT],
  );
  assert.deepStrictEqual(
    nats.eventCatalog,
    new Map([[events[0].name, events[0]]]),
  );
  assert.strictEqual(nats.eventCatalogRevision, 1);
  assert.ok(nats.eventDiscoveryTimer);
  assert.ok(nats.eventCatalogSubscription);
  assert.ok(nats.eventDiscoveryChangeSubscription);
  assert.ok(
    connection.requests.some(
      ({ subject }) => subject === EVENT_DISCOVERY_SUBJECT,
    ),
  );
  assert.ok(
    connection.published.some(
      ({ subject }) => subject === EVENT_DISCOVERY_CHANGED_SUBJECT,
    ),
  );
  await nats.close();
  assert.strictEqual(nats.eventDiscoveryTimer, null);
});

test('lib/nats - should select one server for event discovery', async () => {
  const snapshot = {
    revision: 2,
    events: [
      {
        name: 'profile:1:created',
        subject: 'profile.1.created',
      },
    ],
  };
  for (const [kind, loader, expected] of [
    ['server', true, true],
    ['server', false, false],
    ['worker', true, false],
    ['balancer', true, false],
  ]) {
    const application = createApplication(kind);
    application.subscriptions = { describeEvents: () => snapshot.events };
    const eventDiscovery = {
      loader,
      publish: async (events) => ({ revision: 1, events }),
    };
    const nats = new Nats(application, null, eventDiscovery);
    nats.connection = createConnection();

    nats.subscribeEventCatalog();
    nats.subscribeEventDiscoveryChanges();
    nats.updateEventDiscovery();

    assert.strictEqual(Boolean(nats.eventCatalogSubscription), expected);
    assert.strictEqual(
      Boolean(nats.eventDiscoveryChangeSubscription),
      expected,
    );
    assert.strictEqual(nats.connection.published.length, expected ? 1 : 0);
    const discovered = await nats.discoverEvents();
    assert.strictEqual(discovered instanceof Map, expected);
    nats.applyEventCatalog(snapshot);
    nats.applyEventCatalog({ revision: 1, events: [] });
    assert.strictEqual(nats.eventCatalogRevision, 2);
    assert.deepStrictEqual(
      nats.eventCatalog.get('profile:1:created'),
      snapshot.events[0],
    );
    await nats.close();
  }
});

test('lib/nats - should reject conflicting event contracts', async () => {
  const application = createApplication('server');
  const nats = new Nats(application);
  nats.connection = createConnection();
  const variants = [
    [{ name: 'chat:1:created', subject: 'chat.1.created', caption: 'first' }],
    [{ name: 'chat:1:created', subject: 'chat.1.changed', caption: 'second' }],
  ];
  nats.connection.requestMany = async () => ({
    async *[Symbol.asyncIterator]() {
      for (const events of variants) yield { json: () => events };
    },
  });

  await assert.rejects(nats.fetchEvents(), (error) => {
    assert.strictEqual(
      error.message,
      'Conflicting event contract: chat:1:created',
    );
    assert.strictEqual(error instanceof Error, true);
    return true;
  });
});

test('lib/nats - should merge event metadata during rollout', async () => {
  const application = createApplication('server');
  const nats = new Nats(application);
  nats.connection = createConnection();
  const variants = [
    [{ name: 'chat:1:created', subject: 'chat.1.created', caption: 'first' }],
    [{ name: 'chat:1:created', subject: 'chat.1.created', caption: 'second' }],
  ];
  nats.connection.requestMany = async () => ({
    async *[Symbol.asyncIterator]() {
      for (const events of variants) yield { json: () => events };
    },
  });

  const events = await nats.fetchEvents();

  assert.deepStrictEqual(events, variants[1]);
});

test('lib/nats - should publish an event on the supplied subject', () => {
  const nats = new Nats(createApplication('server'));
  const connection = createConnection();
  nats.connection = connection;
  const payload = { id: 'event-1', data: { value: 42 } };

  nats.publishEvent('chat.1.message.created', payload);

  assert.deepStrictEqual(connection.published, [
    {
      subject: 'chat.1.message.created',
      payload: JSON.stringify(payload),
    },
  ]);
});

test('lib/nats - should defer RPC subscriptions until connected', async () => {
  const application = createApplication('server');
  const nats = new Nats(application);
  application.nats = nats;
  addAction(application, 'first', async () => 1);
  addAction(application, 'second', async () => 2);
  assert.strictEqual(nats.serviceSubscriptions.size, 0);

  const connection = createConnection();
  nats.connect = async () => {
    nats.connection = connection;
  };
  await nats.start();
  try {
    assert.strictEqual(nats.serviceSubscriptions.size, 2);
    assert.strictEqual(connection.subscriptions.has('example.1.first'), true);
    assert.strictEqual(connection.subscriptions.has('example.1.second'), true);
  } finally {
    await nats.close();
  }
});
