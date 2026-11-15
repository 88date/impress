'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const fs = require('node:fs/promises');
const { tmpdir } = require('node:os');
const metavm = require('metavm');
const {
  EventLoader,
  SubscriberLoader,
  EventPublisher,
  SubscriptionManager,
} = require('../lib/events/index.js');

const eventSource =
  '({ caption: shared.caption, internalType: typeof internal })';
const subscriberSource = `({
  stream: 'EVENTS',
  subject: 'events.chat.1.messageCreated',
  method: () => ({ shared, internalType: typeof internal }),
})`;

const createLoaders = async (t) => {
  const directory = await fs.mkdtemp(
    path.join(tmpdir(), 'impress-declarations-'),
  );
  const errors = [];
  const shared = { caption: 'Message created' };
  const application = {
    absolute: (name) => path.join(directory, name),
    sandbox: metavm.createContext({ shared }),
    watcher: { watch() {} },
    console: { error: (error) => errors.push(error) },
  };
  const manager = new SubscriptionManager(
    new EventPublisher(),
    { enabled: false },
    application.console,
  );
  const events = new EventLoader(application, manager);
  const subscribers = new SubscriberLoader(application, manager);
  t.after(async () => {
    await manager.stop();
    assert.strictEqual(path.dirname(directory), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('impress-declarations-'));
    await fs.rm(directory, { recursive: true, force: true });
  });
  const write = async (name, source) => {
    const file = path.join(directory, name);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, source);
    return file;
  };
  const remove = async (name) => {
    const target = path.resolve(directory, name);
    assert.ok(target.startsWith(directory + path.sep));
    await fs.rm(target, { recursive: true, force: true });
    return target;
  };
  return {
    directory,
    errors,
    shared,
    manager,
    events,
    subscribers,
    write,
    remove,
  };
};

test('declaration loader - loads versions in the shared sandbox', async (t) => {
  const { events, subscribers, manager, shared, write, errors } =
    await createLoaders(t);
  await write('modules/chat/events/messageCreated.js', eventSource);
  await write('modules/chat/events/messageCreated.2.js', eventSource);
  await write('modules/chat/events/message/created.3.js', eventSource);
  await write('modules/chat/subscribers/messageCreated.js', subscriberSource);
  await write('modules/chat/subscribers/messageCreated.2.js', subscriberSource);

  await events.load();
  assert.deepStrictEqual(Object.keys(manager.subscribers), []);
  await subscribers.load();

  assert.deepStrictEqual(Object.keys(manager.events).sort(), [
    'chat:1:messageCreated',
    'chat:2:messageCreated',
    'chat:3:message:created',
  ]);
  assert.deepStrictEqual(Object.keys(manager.subscribers).sort(), [
    'chat:1:messageCreated',
    'chat:2:messageCreated',
  ]);
  assert.strictEqual(
    manager.events['chat:2:messageCreated'].eventSubject,
    'events.chat.2.messageCreated',
  );
  assert.strictEqual(
    manager.events['chat:3:message:created'].eventSubject,
    'events.chat.3.message.created',
  );
  const event = manager.events['chat:1:messageCreated'];
  assert.strictEqual(event.caption, shared.caption);
  assert.strictEqual(event.internalType, 'undefined');
  const subscriber = manager.subscribers['chat:2:messageCreated'];
  assert.strictEqual(subscriber.subject, 'events.chat.1.messageCreated');
  assert.strictEqual(subscriber.subscriberPath, 'chat/2/messageCreated');
  const result = subscriber.method();
  assert.strictEqual(result.shared, shared);
  assert.strictEqual(result.internalType, 'undefined');
  assert.deepStrictEqual(errors, []);
});

test('declaration loader - ignores non-declaration paths', async (t) => {
  const { directory, events, subscribers, manager, write, errors } =
    await createLoaders(t);
  await events.load();
  await subscribers.load();
  assert.deepStrictEqual(await fs.readdir(directory), ['modules']);

  const ignored = [
    'events/chat.1/messageCreated.js',
    'subscribers/feed.1/messageCreated.js',
    'modules/chat/start.js',
    'modules/chat/events.js',
    'modules/chat/helpers/events/messageCreated.js',
    'modules/chat/helpers/subscribers/messageCreated.js',
    'modules/chat/events/.hidden.js',
    'modules/chat/events/messageCreated.ts',
  ];
  for (const name of ignored) await write(name, 'not valid JavaScript');
  await write('modules/chat/events/messageCreated.js', eventSource);
  await write('modules/feed/subscribers/messageCreated.js', subscriberSource);

  await events.load();
  await subscribers.load();

  assert.deepStrictEqual(Object.keys(manager.events), [
    'chat:1:messageCreated',
  ]);
  assert.deepStrictEqual(Object.keys(manager.subscribers), [
    'feed:1:messageCreated',
  ]);
  assert.deepStrictEqual(errors, []);
});

const declarations = [
  { kind: 'events', source: eventSource },
  { kind: 'subscribers', source: subscriberSource },
];

test('declaration loader - rejects duplicate initial snapshots', async (t) => {
  for (const { kind, source } of declarations) {
    await t.test(kind, async (t) => {
      const state = await createLoaders(t);
      const { write, manager } = state;
      await write(`modules/chat/${kind}/other.2.js`, source);
      await write(`modules/chat/${kind}/messageCreated.js`, source);
      await write(`modules/chat/${kind}/messageCreated.1.js`, source);

      await assert.rejects(
        state[kind].load(),
        /Duplicate declaration: chat:1:messageCreated/,
      );
      assert.deepStrictEqual(Object.keys(manager[kind]), []);
    });
  }
});

test('declaration loader - retains sources after reload errors', async (t) => {
  for (const { kind, source } of declarations) {
    await t.test(kind, async (t) => {
      const state = await createLoaders(t);
      const { write, remove, manager, errors } = state;
      const loader = state[kind];
      const name = 'chat:1:messageCreated';
      const originalPath = `modules/chat/${kind}/messageCreated.js`;
      const aliasPath = `modules/chat/${kind}/messageCreated.1.js`;
      const originalFile = await write(originalPath, source);
      await loader.load();
      const original = manager[kind][name];

      await write(originalPath, '({');
      await loader.change(originalFile);
      assert.strictEqual(manager[kind][name], original);
      assert.match(errors[0].message, /Cannot load declaration/);

      const aliasFile = await write(aliasPath, source);
      await loader.change(aliasFile);
      assert.strictEqual(manager[kind][name], original);
      assert.match(errors[1].message, /Duplicate declaration/);
      await remove(aliasPath);
      await loader.delete(aliasFile);
      assert.strictEqual(manager[kind][name], original);

      await remove(originalPath);
      await loader.delete(originalFile);
      assert.strictEqual(manager[kind][name], undefined);
      await write(aliasPath, source);
      await loader.change(aliasFile);
      assert.strictEqual(Object.keys(manager[kind]).length, 1);
      assert.notStrictEqual(manager[kind][name], original);
      assert.strictEqual(errors.length, 2);
    });
  }
});

test('declaration loader - removes and reloads module subtrees', async (t) => {
  const { events, subscribers, manager, write, remove, errors } =
    await createLoaders(t);
  await write('modules/chat/events/message/created.js', eventSource);
  await write('modules/chat/events/message/created.2.js', eventSource);
  await write('modules/chat/events/connected.js', eventSource);
  await write('modules/chat/subscribers/messageCreated.js', subscriberSource);
  await write('modules/chatExtra/events/connected.js', eventSource);
  await events.load();
  await subscribers.load();

  const messages = await remove('modules/chat/events/message');
  await events.delete(messages);
  assert.deepStrictEqual(Object.keys(manager.events).sort(), [
    'chat:1:connected',
    'chatExtra:1:connected',
  ]);
  assert.ok(manager.subscribers['chat:1:messageCreated']);

  await write('modules/chat/events/message/created.2.js', eventSource);
  await events.load(messages);
  assert.ok(manager.events['chat:2:message:created']);

  const modulePath = await remove('modules/chat');
  await events.delete(modulePath);
  await subscribers.delete(modulePath);
  assert.deepStrictEqual(Object.keys(manager.events), [
    'chatExtra:1:connected',
  ]);
  assert.deepStrictEqual(Object.keys(manager.subscribers), []);

  await write('modules/chat/events/messageCreated.js', eventSource);
  await write('modules/chat/subscribers/messageCreated.2.js', subscriberSource);
  await events.load(modulePath);
  await subscribers.load(modulePath);
  assert.ok(manager.events['chat:1:messageCreated']);
  assert.ok(manager.subscribers['chat:2:messageCreated']);
  assert.deepStrictEqual(errors, []);
});
