'use strict';

const { before, test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const metavm = require('metavm');
const { Api } = require('../lib/api.js');

const root = process.cwd();

const application = {
  path: path.join(root, 'test'),
  sandbox: metavm.createContext({ api: {} }),
  watcher: { watch() {} },
  absolute(relative) {
    return path.join(this.path, relative);
  },
  config: { server: { timeouts: {} } },
};

let api = null;

before(async () => {
  api = new Api('api', application);
  await api.load();
});

test('lib/api load - should load API correctly', () => {
  const { example } = api.collection;

  assert.strictEqual(example.default, 1);
  const { add } = example['1'];
  assert.strictEqual(add.constructor.name, 'Procedure');
  assert.strictEqual(typeof add.method, 'function');
  assert.strictEqual(add.method.constructor.name, 'AsyncFunction');

  const exportsKeys = ['transports', 'parameters', 'method', 'returns'];
  assert.deepStrictEqual(Object.keys(add.exports), exportsKeys);
  assert.strictEqual(typeof add.script, 'function');
  assert.strictEqual(add.methodName, 'method');
  assert.strictEqual(typeof add.application, 'object');
  assert.strictEqual(add.parameters.constructor.name, 'Schema');
  assert.strictEqual(add.returns.constructor.name, 'Schema');
  assert.strictEqual(add.errors, null);
  assert.ok(add.locks instanceof Map);
  assert.strictEqual(add.locks.size, 0);
  assert.strictEqual(add.caption, '');
  assert.strictEqual(add.description, '');
  assert.strictEqual(add.access, '');
  assert.strictEqual(add.validate, null);
  assert.strictEqual(add.timeout, 0);
  assert.strictEqual(add.serializer, null);
  assert.strictEqual(add.protocols, null);
  assert.strictEqual(add.deprecated, false);
  assert.strictEqual(add.assert, null);
  assert.strictEqual(add.examples, null);
});

test('lib/api load - should load transports', () => {
  const example = Array.from(api.collection.example['1'].add.transports);
  assert.deepStrictEqual(example, ['http', 'ws']);
  const transports = api.collection.transports['1'];
  assert.deepStrictEqual(Array.from(transports.centrifugo.transports), [
    'centrifugo',
  ]);
  assert.deepStrictEqual(Array.from(transports.nats.transports), ['nats']);
  assert.strictEqual(
    typeof application.sandbox.api.transports.nats,
    'function',
  );
});

test('lib/api load - should load router without transports', () => {
  const router = api.collection.health['1'].router;

  assert.strictEqual(typeof router.method, 'function');
  assert.deepStrictEqual(router.transports, []);
});
