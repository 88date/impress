'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { withTimeout } = require('../lib/timeout.js');

test('timeout - returns result and clears the timer', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const result = { value: 42 };
  let signal;
  const operation = withTimeout(
    (value) => {
      signal = value;
      return result;
    },
    100,
    'Handler timed out',
  );
  assert.strictEqual(signal, undefined);
  assert.strictEqual(await operation, result);
  t.mock.timers.tick(100);
  assert.strictEqual(signal.aborted, false);
});

for (const mode of ['throw', 'reject']) {
  test(`timeout - preserves ${mode} error and clears timer`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const failure = new Error('Handler failed');
    let signal;
    const operation = withTimeout(
      (value) => {
        signal = value;
        if (mode === 'throw') throw failure;
        return Promise.reject(failure);
      },
      100,
      'Handler timed out',
    );
    await assert.rejects(operation, (error) => error === failure);
    t.mock.timers.tick(100);
    assert.strictEqual(signal.aborted, false);
  });
}

for (const mode of ['resolve', 'reject']) {
  test(`timeout - aborts and ignores late ${mode}`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const finished = Promise.withResolvers();
    let signal;
    const operation = withTimeout(
      (value) => {
        signal = value;
        return finished.promise;
      },
      100,
      'Handler timed out',
    );
    await Promise.resolve();
    assert.strictEqual(signal.aborted, false);
    const rejected = assert.rejects(operation, (error) => {
      assert.strictEqual(error, signal.reason);
      assert.strictEqual(error.message, 'Handler timed out');
      return true;
    });
    t.mock.timers.tick(100);
    await rejected;
    assert.strictEqual(signal.aborted, true);
    if (mode === 'resolve') finished.resolve('Late result');
    else finished.reject(new Error('Late failure'));
    await Promise.resolve();
    await assert.rejects(operation, (error) => error === signal.reason);
  });
}
