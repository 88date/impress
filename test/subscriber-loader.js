'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const fsp = require('node:fs/promises');
const metavm = require('metavm');
const {
  EventPublisher,
  SubscriberLoader,
  SubscriptionManager,
  JetStreamSubscriptions,
} = require('../lib/events/index.js');

const createLoader = (t) => {
  const errors = [];
  const application = {
    absolute: (name) => path.join(__dirname, name),
    sandbox: metavm.createContext({}),
    watcher: { watch() {} },
    console: { error: (error) => errors.push(error) },
    config: { server: { nats: {} } },
  };
  const nats = { application, connection: {} };
  const transport = new JetStreamSubscriptions(nats, application.console, true);
  t.mock.method(transport, 'ensureStream', async () => true);
  t.mock.method(transport, 'bind', async (binding, dispatch) => {
    const handle = {
      running: true,
      dispatch,
      stop: async () => {
        handle.running = false;
      },
    };
    return handle;
  });
  const manager = new SubscriptionManager(
    new EventPublisher(),
    transport,
    application.console,
  );
  const loader = new SubscriberLoader(application, manager);
  t.after(() => manager.stop());
  return { loader, manager, errors };
};

const invalidDeclarations = [
  ['null', 'Subscriber event name expected'],
  ['({ method: async () => {} })', 'Subscriber event name expected'],
  ['({ event: "", method: async () => {} })', 'Subscriber event name expected'],
  ['({ event: 42, method: async () => {} })', 'Subscriber event name expected'],
  ['({ event: "chat:1:message:created" })', 'Subscriber method expected'],
  [
    '({ event: "chat:1:message:created", method: 42 })',
    'Subscriber method expected',
  ],
];

test('subscriber loader - invalid declarations reject startup', async (t) => {
  for (const [source, message] of invalidDeclarations) {
    await t.test(source, async (t) => {
      const { loader, manager } = createLoader(t);
      t.mock.method(fsp, 'readFile', async () => source);

      await assert.rejects(loader.load(), (error) => {
        assert.match(error.message, /Cannot load declaration/);
        assert.strictEqual(error.cause.message, message);
        return true;
      });

      assert.deepStrictEqual(Object.keys(manager.subscribers), []);
      assert.strictEqual(manager.active.size, 0);
    });
  }
});

test('subscriber loader - invalid reload preserves the handler', async (t) => {
  const { loader, manager, errors } = createLoader(t);
  let source = '({ event: "chat:1:message:created", method: () => "working" })';
  t.mock.method(fsp, 'readFile', async () => source);
  await loader.load();
  await manager.start();
  const name = 'feed:1:message:created';
  const contract = manager.subscribers[name];
  const { handle } = manager.active.get(name);
  const record = { name: contract.eventName, payload: {} };
  const file = path.join(loader.path, 'feed.1', 'messageCreated.js');

  for (const [invalid] of invalidDeclarations) {
    source = invalid;
    await loader.change(file);
    assert.strictEqual(manager.subscribers[name], contract);
    assert.strictEqual(manager.active.get(name).handle, handle);
    assert.strictEqual(await handle.dispatch(record, {}), 'working');
  }
  assert.strictEqual(errors.length, invalidDeclarations.length);

  source = '({ event: "chat:1:message:created", method: () => "updated" })';
  await loader.change(file);

  assert.notStrictEqual(manager.subscribers[name], contract);
  assert.strictEqual(manager.active.get(name).handle, handle);
  assert.strictEqual(await handle.dispatch(record, {}), 'updated');
  assert.strictEqual(errors.length, invalidDeclarations.length);
});
