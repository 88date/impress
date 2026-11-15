'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs/promises');
const { tmpdir } = require('node:os');
const path = require('node:path');
const metavm = require('metavm');
const { Modules } = require('../lib/modules.js');
const {
  EventLoader,
  SubscriberLoader,
  EventPublisher,
  SubscriptionManager,
} = require('../lib/events/index.js');

const eventSource = '({ caption: record("event") })';
const subscriberSource = `({
  stream: 'EVENTS',
  subject: 'events.chat.1.messageCreated',
  method: () => "original",
  loaded: record('subscriber'),
})`;

const createModules = async (t) => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), 'impress-modules-'));
  const evaluated = [];
  const errors = [];
  const application = {
    initialization: true,
    starts: [],
    absolute: (name) => path.join(directory, name),
    sandbox: metavm.createContext({
      record: (name) => {
        evaluated.push(name);
        return name;
      },
    }),
    watcher: { watch() {} },
    console: { error: (error) => errors.push(error) },
  };
  const manager = new SubscriptionManager(
    new EventPublisher(),
    { enabled: false },
    application.console,
  );
  application.events = new EventLoader(application, manager);
  application.subscribers = new SubscriberLoader(application, manager);
  const modules = new Modules(application);
  application.sandbox.modules = modules.tree;
  t.after(async () => {
    await manager.stop();
    assert.strictEqual(path.dirname(directory), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('impress-modules-'));
    await fs.rm(directory, { recursive: true, force: true });
  });
  const write = async (name, source) => {
    const file = path.join(modules.path, name);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, source);
    return file;
  };
  return { application, modules, manager, evaluated, errors, write };
};

test('modules - loads declarations once during initialization', async (t) => {
  const { application, modules, manager, evaluated, errors, write } =
    await createModules(t);
  await write('chat/events/messageCreated.js', eventSource);
  await write('feed/subscribers/messageCreated.js', subscriberSource);
  const ordinary = [
    'chat/run.ts',
    'chat/events.js',
    'chat/helpers/events/value.js',
  ];
  for (const name of ordinary) {
    await write(name, `({ value: record('${name}') })`);
  }

  await application.events.load();
  await application.subscribers.load();
  await modules.load();

  assert.deepStrictEqual(
    evaluated.sort(),
    [...ordinary, 'event', 'subscriber'].sort(),
  );
  assert.strictEqual(modules.tree.chat.run.value, 'chat/run.ts');
  assert.strictEqual(modules.tree.chat.events.value, 'chat/events.js');
  assert.strictEqual(
    modules.tree.chat.helpers.events.value.value,
    'chat/helpers/events/value.js',
  );
  assert.strictEqual(modules.tree.feed, undefined);
  assert.strictEqual(manager.events['chat:1:messageCreated'].caption, 'event');
  assert.strictEqual(
    manager.subscribers['feed:1:messageCreated'].method(),
    'original',
  );
  assert.deepStrictEqual(errors, []);
});

test('modules - reloads and removes module declarations', async (t) => {
  const { application, modules, manager, evaluated, errors, write } =
    await createModules(t);
  application.initialization = false;
  await write('chat/run.js', '({ value: 42 })');
  await write('chat/work.js', '() => "original"');
  await write('chat/events/messageCreated.js', eventSource);
  const versionFile = await write(
    'chat/events/messageCreated.2.js',
    eventSource,
  );
  const subscriberFile = await write(
    'chat/subscribers/messageCreated.js',
    subscriberSource.replace('() => "original"', 'modules.chat.work'),
  );
  const modulePath = path.join(modules.path, 'chat');

  await modules.load(modulePath);

  assert.strictEqual(modules.tree.chat.run.value, 42);
  assert.strictEqual(modules.tree.chat.events, undefined);
  assert.strictEqual(modules.tree.chat.subscribers, undefined);
  assert.deepStrictEqual(Object.keys(manager.events).sort(), [
    'chat:1:messageCreated',
    'chat:2:messageCreated',
  ]);
  assert.strictEqual(
    manager.subscribers['chat:1:messageCreated'].method,
    modules.tree.chat.work,
  );
  assert.strictEqual(
    evaluated.filter((name) => name === 'subscriber').length,
    1,
  );
  await write(
    'chat/subscribers/messageCreated.js',
    subscriberSource.replace('original', 'updated'),
  );
  await modules.change(subscriberFile);
  assert.strictEqual(
    manager.subscribers['chat:1:messageCreated'].method(),
    'updated',
  );

  await fs.unlink(versionFile);
  await modules.delete(versionFile);
  assert.deepStrictEqual(Object.keys(manager.events), [
    'chat:1:messageCreated',
  ]);
  assert.strictEqual(path.dirname(modulePath), modules.path);
  await fs.rm(modulePath, { recursive: true });
  await modules.delete(modulePath);
  assert.deepStrictEqual(Object.keys(manager.events), []);
  assert.deepStrictEqual(Object.keys(manager.subscribers), []);
  assert.deepStrictEqual(errors, []);
});

test('modules - retains declarations on directory reload errors', async (t) => {
  const { application, modules, manager, errors, write } =
    await createModules(t);
  application.initialization = false;
  await write('feed/subscribers/messageCreated.js', subscriberSource);
  const directory = path.join(modules.path, 'feed', 'subscribers');
  await modules.load(directory);
  const original = manager.subscribers['feed:1:messageCreated'];

  await write('feed/subscribers/messageCreated.js', '({');
  await modules.load(directory);

  assert.strictEqual(manager.subscribers['feed:1:messageCreated'], original);
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /Cannot load declaration/);
});
