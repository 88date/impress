'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { Pgboss, getPgbossConfig } = require('../lib/pgboss.js');

test('lib/pgboss - should configure listener from scheduler mode', () => {
  const config = Object.freeze({
    enabled: true,
    useListenNotify: false,
    max: 5,
  });
  const scheduler = { enabled: true, active: false, notify: true };
  const options = getPgbossConfig(config, scheduler);
  const boss = new Pgboss(options);

  assert.strictEqual(boss.enabled, true);
  assert.strictEqual(boss.config.useListenNotify, true);
  assert.strictEqual(boss.config.max, 5);
  assert.strictEqual(config.useListenNotify, false);

  for (const notify of [false, undefined]) {
    const polling = getPgbossConfig(options, { enabled: true, notify });
    assert.strictEqual(new Pgboss(polling).config.useListenNotify, false);
  }
});

test('lib/pgboss - should preserve listener config when tasks are off', () => {
  const config = { enabled: true, useListenNotify: true };

  assert.deepStrictEqual(getPgbossConfig(config, { enabled: false }), config);
  assert.deepStrictEqual(getPgbossConfig(config), config);
});

test('lib/pgboss - should be disabled by default', async () => {
  const boss = new Pgboss();

  assert.strictEqual(await boss.start(), null);
  assert.strictEqual(boss.enabled, false);
  assert.deepStrictEqual(boss.config, {});
});

test('lib/pgboss - should stay idle when disabled', async () => {
  const boss = new Pgboss({
    enabled: false,
    connectionString: 'postgres://localhost/example',
  });

  const client = await boss.start();

  assert.strictEqual(client, null);
  assert.strictEqual(boss.client, null);
  assert.strictEqual(boss.state, 'idle');
  assert.strictEqual(boss.config.enabled, undefined);
});

test('lib/pgboss - should stop gracefully', async () => {
  let stopOptions = null;
  const boss = new Pgboss({ enabled: true });
  boss.client = {
    stop: async (options) => {
      stopOptions = options;
    },
  };
  boss.state = 'running';

  await boss.stop(5000);

  assert.deepStrictEqual(stopOptions, { graceful: true, timeout: 5000 });
  assert.strictEqual(boss.client, null);
  assert.strictEqual(boss.state, 'idle');
});

test('lib/pgboss - should log stop failure', async () => {
  const errors = [];
  const logger = { error: (...args) => errors.push(args) };
  const boss = new Pgboss({ enabled: true }, logger);
  boss.client = {
    stop: async () => {
      throw new Error('Connection lost');
    },
  };
  boss.state = 'running';

  await boss.stop(5000);

  assert.strictEqual(boss.state, 'running');
  assert.strictEqual(errors.length, 1);
  assert.strictEqual(errors[0][0], 'Can not stop pgboss');
  assert.match(errors[0][1], /Connection lost/);
});
