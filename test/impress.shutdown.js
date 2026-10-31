'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');

test('impress - cleanup failures do not prevent worker shutdown', async () => {
  const entry = path.join(__dirname, '../impress.js');
  const source = fs
    .readFileSync(entry, 'utf8')
    .replace(
      /loadApplications\(\)\.catch\(logError\('Initialization'\)\);/,
      'globalThis.review = { impress, stop };',
    );
  const errors = [];
  const exited = Promise.withResolvers();
  const context = {
    require: createRequire(entry),
    __dirname: path.dirname(entry),
    setTimeout,
    clearTimeout,
    console: { info() {}, error: (...args) => errors.push(args) },
    process: {
      cwd: () => process.cwd(),
      env: {},
      stdin: { isTTY: false },
      on() {},
      removeAllListeners() {},
      exit: exited.resolve,
    },
  };
  vm.runInNewContext(source, context);
  const { impress, stop } = context.review;
  const config = { server: { timeouts: { stop: 50 } } };
  const calls = [];
  const worker = {
    postMessage: ({ name }) => {
      calls.push(name);
      impress.applications.delete('test');
      impress.close();
    },
  };
  const app = {
    config,
    threads: new Map([[1, worker]]),
    scheduler: {
      stop: async () => {
        calls.push('scheduler');
        throw new Error('Scheduler failed');
      },
    },
    nats: {
      close: async () => {
        calls.push('nats');
        throw new Error('NATS failed');
      },
    },
  };
  impress.config = config;
  impress.applications.set('test', app);

  await stop();

  assert.strictEqual(await exited.promise, 0);
  assert.deepStrictEqual(calls, ['scheduler', 'nats', 'stop']);
  assert.deepStrictEqual(
    errors.map((args) => args[1].message),
    ['Scheduler failed', 'NATS failed'],
  );
});
