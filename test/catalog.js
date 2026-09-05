'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const {
  ServiceCatalog,
  EventCatalog,
  DiscoveryWorker,
} = require('../lib/catalog.js');

const createCatalog = (threads, config) =>
  new ServiceCatalog(threads, new DiscoveryWorker(threads, config));

const createThread = () => ({
  messages: [],
  postMessage(message) {
    this.messages.push(structuredClone(message));
  },
});

test('lib/catalog - should select one discovery worker', () => {
  const cases = [
    [{ ports: [8000, 8001], workers: { pool: 2 } }, 2],
    [{ ports: [], workers: { pool: 2 } }, 4],
    [{ ports: [] }, 1],
  ];
  for (const [config, expected] of cases) {
    const catalog = createCatalog(new Map(), config);
    const kinds = ['balancer', 'server', 'server', 'worker', 'worker'];
    for (const [index, kind] of kinds.entries()) {
      const id = index + 1;
      assert.strictEqual(
        catalog.discoveryWorker.register(id, kind),
        id === expected,
      );
    }
    assert.strictEqual(catalog.discoveryWorker.id, expected);
    assert.strictEqual(
      catalog.discoveryWorker.register(expected, kinds[expected - 1]),
      true,
    );
  }
});

test('lib/catalog - should cache snapshots for late workers', () => {
  const loader = createThread();
  const follower = createThread();
  const threads = new Map([
    [1, loader],
    [2, follower],
  ]);
  const catalog = createCatalog(threads, { ports: [8000] });
  catalog.discoveryWorker.register(1, 'server');
  catalog.send(follower);
  assert.deepStrictEqual(follower.messages, []);

  const services = [
    { name: 'example', actions: [{ name: 'echo', version: 1 }] },
  ];
  const snapshot = catalog.publish(loader, services);
  const message = { name: 'catalog', snapshot: { revision: 1, services } };
  assert.deepStrictEqual(snapshot, message.snapshot);
  assert.deepStrictEqual(loader.messages, [message]);
  assert.deepStrictEqual(follower.messages, [message]);

  const late = createThread();
  threads.set(3, late);
  catalog.send(late);
  assert.deepStrictEqual(late.messages, [message]);

  assert.throws(() => catalog.publish(follower, []), {
    message: 'Only the discovery worker can publish the catalog',
  });
  assert.strictEqual(catalog.snapshot.revision, 1);
});

test('lib/catalog - should preserve catalog across loader restart', () => {
  const previous = createThread();
  const threads = new Map([[1, previous]]);
  const catalog = createCatalog(threads, { ports: [8000] });
  catalog.discoveryWorker.register(1, 'server');
  catalog.publish(previous, [{ name: 'example', actions: [] }]);

  const replacement = createThread();
  threads.set(1, replacement);
  assert.strictEqual(catalog.discoveryWorker.register(1, 'server'), true);
  catalog.send(replacement);
  assert.strictEqual(replacement.messages[0].snapshot.revision, 1);
  const cached = catalog.publish(replacement, [
    { name: 'example', actions: [] },
  ]);
  assert.strictEqual(cached.revision, 1);
  assert.strictEqual(replacement.messages.length, 1);
  assert.throws(() => catalog.publish(previous, []), {
    message: 'Only the discovery worker can publish the catalog',
  });

  catalog.publish(replacement, []);
  assert.deepStrictEqual(catalog.snapshot, { revision: 2, services: [] });
  assert.strictEqual(replacement.messages.at(-1).snapshot.revision, 2);
  assert.strictEqual(previous.messages.length, 1);
});

test('lib/catalog - should skip unchanged catalogs in any order', () => {
  const loader = createThread();
  const follower = createThread();
  const threads = new Map([
    [1, loader],
    [2, follower],
  ]);
  const catalog = createCatalog(threads, { ports: [8000] });
  catalog.discoveryWorker.register(1, 'server');
  const services = [
    {
      name: 'example',
      actions: [
        { name: 'echo', version: 1, parameters: { value: 'string' } },
        { name: 'echo', version: 2, parameters: { value: 'number' } },
      ],
    },
    { name: 'other', actions: [{ name: 'ping', version: 1 }] },
  ];
  const snapshot = catalog.publish(loader, services);
  const identical = structuredClone(services);
  assert.strictEqual(catalog.publish(loader, identical), snapshot);

  const reordered = identical.toReversed().map(({ name, actions }) => ({
    actions: actions.toReversed().map(({ name, ...action }) => ({
      ...action,
      name,
    })),
    name,
  }));
  assert.strictEqual(catalog.publish(loader, reordered), snapshot);
  assert.strictEqual(loader.messages.length, 1);
  assert.strictEqual(follower.messages.length, 1);
  assert.strictEqual(catalog.snapshot.revision, 1);
  assert.throws(() => catalog.publish(follower, identical), {
    message: 'Only the discovery worker can publish the catalog',
  });

  const late = createThread();
  threads.set(3, late);
  catalog.send(late);
  assert.deepStrictEqual(late.messages, [{ name: 'catalog', snapshot }]);
});

test('lib/catalog - should broadcast changed contracts and removals', () => {
  const loader = createThread();
  const catalog = createCatalog(new Map([[1, loader]]), {
    ports: [8000],
  });
  catalog.discoveryWorker.register(1, 'server');
  const services = [
    {
      name: 'example',
      actions: [
        { name: 'echo', version: 1, parameters: { value: 'string' } },
        { name: 'echo', version: 2, parameters: { value: 'number' } },
      ],
    },
    { name: 'other', actions: [{ name: 'ping', version: 1 }] },
  ];
  catalog.publish(loader, structuredClone(services));
  services[0].actions[0].parameters.value = 'boolean';
  catalog.publish(loader, structuredClone(services));
  services[0].actions.pop();
  catalog.publish(loader, structuredClone(services));
  services.pop();
  catalog.publish(loader, structuredClone(services));
  const empty = catalog.publish(loader, []);
  assert.strictEqual(catalog.publish(loader, []), empty);

  assert.deepStrictEqual(
    loader.messages.map(({ snapshot }) => snapshot.revision),
    [1, 2, 3, 4, 5],
  );
  const updated = loader.messages[1].snapshot.services[0].actions[0];
  assert.deepStrictEqual(updated.parameters, { value: 'boolean' });
  assert.strictEqual(loader.messages[2].snapshot.services[0].actions.length, 1);
  assert.strictEqual(loader.messages[3].snapshot.services.length, 1);
  assert.deepStrictEqual(empty.services, []);
});

test('lib/catalog - should isolate application catalogs', () => {
  const first = createThread();
  const second = createThread();
  const firstCatalog = createCatalog(new Map([[1, first]]), {
    ports: [8000],
  });
  const secondCatalog = createCatalog(new Map([[2, second]]), {
    ports: [8001],
  });
  firstCatalog.discoveryWorker.register(1, 'server');
  secondCatalog.discoveryWorker.register(2, 'server');

  firstCatalog.publish(first, []);

  assert.strictEqual(first.messages.length, 1);
  assert.deepStrictEqual(second.messages, []);
  assert.strictEqual(secondCatalog.snapshot, null);
});

test('lib/catalog - event catalog should share discovery loader', () => {
  const follower = createThread();
  const loader = createThread();
  const threads = new Map([
    [1, follower],
    [2, loader],
  ]);
  const services = createCatalog(threads, { ports: [8000] });
  const events = new EventCatalog(threads, services.discoveryWorker);

  assert.strictEqual(services.discoveryWorker.register(1, 'balancer'), false);
  assert.strictEqual(services.discoveryWorker.register(2, 'server'), true);
  assert.throws(() => events.publish(follower, []), {
    message: 'Only the discovery worker can publish the event catalog',
  });

  const declarations = [
    {
      name: 'profile:1:created',
      subject: 'profile.1.created',
      caption: 'Profile created',
    },
    {
      name: 'chat:1:message:created',
      subject: 'chat.1.message.created',
      caption: 'Message created',
    },
  ];
  const snapshot = events.publish(loader, structuredClone(declarations));
  const message = {
    name: 'eventCatalog',
    snapshot: { revision: 1, events: declarations },
  };
  assert.deepStrictEqual(snapshot, message.snapshot);
  assert.deepStrictEqual(loader.messages, [message]);
  assert.deepStrictEqual(follower.messages, [message]);

  const reordered = structuredClone(declarations).toReversed();
  assert.strictEqual(events.publish(loader, reordered), snapshot);
  assert.strictEqual(loader.messages.length, 1);
  assert.strictEqual(follower.messages.length, 1);

  declarations[0].caption = 'Created profile';
  const updated = events.publish(loader, structuredClone(declarations));
  assert.strictEqual(updated.revision, 2);

  const late = createThread();
  threads.set(3, late);
  events.send(late);
  assert.deepStrictEqual(late.messages, [
    { name: 'eventCatalog', snapshot: updated },
  ]);
});

test('lib/catalog - event broadcast should skip disconnected workers', () => {
  const loader = createThread();
  const disconnected = {
    postMessage() {
      throw new Error('Worker is closed');
    },
  };
  const follower = createThread();
  const threads = new Map([
    [1, loader],
    [2, disconnected],
    [3, follower],
  ]);
  const services = createCatalog(threads, { ports: [8000] });
  const events = new EventCatalog(threads, services.discoveryWorker);
  services.discoveryWorker.register(1, 'server');

  const snapshot = events.publish(loader, [
    { name: 'chat:1:created', subject: 'chat.1.created' },
  ]);

  assert.deepStrictEqual(follower.messages, [
    { name: 'eventCatalog', snapshot },
  ]);
});
