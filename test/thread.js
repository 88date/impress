'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { request } = require('../lib/thread.js');

test('lib/thread - should return thread response', async () => {
  const thread = {
    postMessage({ value, port }) {
      port.postMessage({ result: value * 2 });
      port.close();
    },
  };

  const result = await request(thread, { value: 21 });

  assert.strictEqual(result, 42);
});

test('lib/thread - should reject thread error', async () => {
  const thread = {
    postMessage({ port }) {
      port.postMessage({ error: { message: 'Request failed' } });
      port.close();
    },
  };

  await assert.rejects(request(thread, {}), { message: 'Request failed' });
});

test('lib/thread - should reject thread disconnect', async () => {
  const thread = {
    postMessage({ port }) {
      port.close();
    },
  };

  await assert.rejects(request(thread, {}), {
    message: 'Thread disconnected',
  });
});
