'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { loadSchema } = require('metaschema');
const { createContext } = require('metavm');
const { Config } = require('metaconfiguration');

test('schemas/config - should validate config schemas correctly', async () => {
  const context = createContext({ process });
  const config = await new Config('./test/config', { context });

  const log = await loadSchema('./schemas/config/log.js');
  assert.strictEqual(log.check(config.log).valid, true);

  const scale = await loadSchema('./schemas/config/scale.js');
  assert.strictEqual(scale.check(config.scale).valid, true);

  const server = await loadSchema('./schemas/config/server.js');
  assert.strictEqual(server.check(config.server).valid, true);
  const scheduler = { enabled: true, active: false };
  assert.strictEqual(server.check({ ...config.server, scheduler }).valid, true);
  for (const notify of [true, false]) {
    const options = { ...config.server, scheduler: { ...scheduler, notify } };
    assert.strictEqual(server.check(options).valid, true);
  }
  const invalid = {
    ...config.server,
    scheduler: { ...scheduler, notify: 'true' },
  };
  assert.strictEqual(server.check(invalid).valid, false);

  const invalidNats = {
    ...config.server,
    nats: { ...config.server.nats, enabled: 'true' },
  };
  assert.strictEqual(server.check(invalidNats).valid, false);

  const invalidPubsub = {
    ...config.server,
    pubsub: { active: 'true' },
  };
  assert.strictEqual(server.check(invalidPubsub).valid, false);

  const invalidPgboss = { ...config.server, pgboss: 'enabled' };
  assert.strictEqual(server.check(invalidPgboss).valid, false);

  const invalidCentrifugo = {
    ...config.server,
    centrifugo: { secret: true },
  };
  assert.strictEqual(server.check(invalidCentrifugo).valid, false);

  const sessions = await loadSchema('./schemas/config/sessions.js');
  assert.strictEqual(sessions.check(config.sessions).valid, true);
});

test('schemas/contracts - should load procedure contract', async () => {
  const proc = await loadSchema('./schemas/contracts/procedure.js');
  assert.strictEqual(Object.keys(proc.fields).length, 17);
  assert.strictEqual(proc.fields.transports.required, false);
});
