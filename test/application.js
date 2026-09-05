'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const wt = require('node:worker_threads');
const cwd = process.cwd();

wt.workerData = { id: 0, kind: 'server', root: cwd, path: cwd, port: 8000 };
const application = require('../lib/application.js');

test('lib/application - should have correct application properties', () => {
  assert.strictEqual(application instanceof EventEmitter, true);
  assert.strictEqual(application.constructor.name, 'Application');
  assert.strictEqual(application.kind, 'server');
  assert.strictEqual(application.initialization, true);
  assert.strictEqual(application.finalization, false);
  assert.strictEqual(
    application.contextStorage.constructor.name,
    'AsyncLocalStorage',
  );
  assert.strictEqual(typeof application.root, 'string');
  assert.strictEqual(typeof application.path, 'string');
  assert.strictEqual(application.schemas.constructor.name, 'Schemas');
  assert.strictEqual(application.static.constructor.name, 'Static');
  assert.strictEqual(application.cert.constructor.name, 'Cert');
  assert.strictEqual(application.resources.constructor.name, 'Static');
  assert.strictEqual(application.api.constructor.name, 'Api');
  assert.strictEqual(application.service.constructor.name, 'Service');
  assert.strictEqual(application.service.path, application.api.path);
  assert.strictEqual(application.lib.constructor.name, 'Code');
  assert.strictEqual(application.db.constructor.name, 'Code');
  assert.strictEqual(application.bus.constructor.name, 'Code');
  assert.strictEqual(application.tasks.constructor.name, 'Tasks');
  assert.strictEqual(application.eventPublisher, null);
  assert.strictEqual(application.subscriptions, null);
  assert.strictEqual(application.events, null);
  assert.strictEqual(application.subscribers, null);
  assert.strictEqual(application.nats, null);
  assert.strictEqual(application.pgboss, null);
  assert.deepStrictEqual(application.starts, []);
  assert.strictEqual(application.config, null);
  assert.strictEqual(application.logger, null);
  assert.strictEqual(application.console, null);
  assert.strictEqual(application.auth, null);
  assert.strictEqual(application.watcher, null);
  assert.strictEqual(typeof application.getDocumentation, 'function');
});

test('lib/application - should expose documentation to sandbox', async () => {
  application.config = { server: {} };
  application.console = console;
  application.createSandbox();

  const sandboxApplication = application.sandbox.application;
  const { getDocumentation } = sandboxApplication;
  assert.strictEqual(typeof getDocumentation, 'function');
  assert.strictEqual(sandboxApplication.scheduler, undefined);
  assert.strictEqual(typeof application.sandbox.tasks, 'object');
  let catalogReads = 0;
  application.nats = {
    getCatalog: async () => {
      catalogReads++;
      return new Map();
    },
  };
  const documentation = await getDocumentation();
  await getDocumentation();
  application.nats = null;

  assert.strictEqual(catalogReads, 2);
  assert.deepStrictEqual(documentation.api, {});
  assert.deepStrictEqual(documentation.services, {});
  assert.strictEqual(typeof documentation.schemas, 'object');
  assert.deepStrictEqual(documentation.queues, {});
  assert.deepStrictEqual(documentation.events, []);
});

test('lib/application - should restrict methods by transport', () => {
  const proc = { transports: ['centrifugo'] };
  const hidden = { transports: [] };
  application.api.collection.transport = {
    default: 1,
    1: { hidden, restricted: proc },
  };

  assert.strictEqual(
    application.getMethod('transport', '*', 'restricted', 'centrifugo'),
    proc,
  );
  assert.strictEqual(
    application.getMethod('transport', '*', 'restricted', 'http'),
    null,
  );
  assert.strictEqual(
    application.getMethod('transport', '*', 'restricted', 'ws'),
    null,
  );
  assert.strictEqual(
    application.getMethod('transport', '*', 'hidden', 'http'),
    null,
  );

  delete application.api.collection.transport;
});

test('lib/application - documentation includes event catalogs', async (t) => {
  const remote = {
    name: 'chat:1:created',
    subject: 'chat.1.created',
    caption: 'Remote event',
    description: '',
    examples: null,
  };
  const local = {
    ...remote,
    caption: 'Local event',
    transports: ['local', 'nats'],
  };
  const another = {
    ...remote,
    name: 'profile:1:updated',
    subject: 'profile.1.updated',
  };
  application.nats = {
    getCatalog: async () => new Map(),
    eventCatalog: new Map([
      [remote.name, remote],
      [another.name, another],
    ]),
  };
  application.subscriptions = {
    describeEvents: ({ natsOnly }) => {
      assert.strictEqual(natsOnly, false);
      return [local];
    },
  };
  t.after(() => {
    application.nats = null;
    application.subscriptions = null;
  });

  const documentation = await application.getDocumentation();
  assert.deepStrictEqual(documentation.events, [
    { ...local, origin: 'local' },
    { ...another, transports: ['nats'], origin: 'remote' },
  ]);
});
