'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('node:events');
const { Pgboss, getPgbossConfig, watchEvents } = require('../lib/pgboss.js');

const createLogger = () => {
  const calls = [];
  return {
    calls,
    error: (...args) => calls.push(['error', ...args]),
    warn: (...args) => calls.push(['warn', ...args]),
    info: (...args) => calls.push(['info', ...args]),
    debug: (...args) => calls.push(['debug', ...args]),
  };
};

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
  assert.deepStrictEqual(boss.logEvents, ['error', 'warning']);
  assert.deepStrictEqual(boss.config, {});
});

test('lib/pgboss - should log default events', () => {
  const client = new EventEmitter();
  const logger = createLogger();
  const boss = new Pgboss();
  watchEvents(client, logger, boss.logEvents);

  client.emit('error', new Error('connection lost'));
  client.emit('warning', { message: 'queue is large', data: { size: 10 } });
  client.emit('wip', [{ name: 'task' }]);
  client.emit('stopped');
  client.emit('bam', { name: 'migration', status: 'completed' });
  client.emit('flow', { resolved: 1 });

  assert.deepStrictEqual(
    logger.calls.map(([level]) => level),
    ['error', 'warn'],
  );
});

test('lib/pgboss - should allow disabling event logs', () => {
  const client = new EventEmitter();
  const logger = createLogger();
  const boss = new Pgboss({ logEvents: [] });
  watchEvents(client, logger, boss.logEvents);

  assert.doesNotThrow(() => client.emit('error', new Error('ignored')));
  assert.strictEqual(boss.config.logEvents, undefined);
  assert.deepStrictEqual(logger.calls, []);
});

test('lib/pgboss - should select detailed event logs', () => {
  const client = new EventEmitter();
  const logger = createLogger();
  const boss = new Pgboss({
    logEvents: ['wip', 'flow', 'stopped', 'bam', 'wip'],
  });
  watchEvents(client, logger, boss.logEvents);

  client.emit('error', new Error('ignored'));
  client.emit('warning', { message: 'ignored' });
  client.emit('wip', [{ name: 'task' }]);
  client.emit('flow', { resolved: 1 });
  client.emit('stopped');
  client.emit('bam', { name: 'migration', status: 'completed' });

  assert.deepStrictEqual(boss.logEvents, ['wip', 'flow', 'stopped', 'bam']);
  assert.strictEqual(client.listenerCount('wip'), 1);
  assert.deepStrictEqual(
    logger.calls.map(([level]) => level),
    ['debug', 'debug', 'info', 'info'],
  );
});

test('lib/pgboss - should reject unknown log events', () => {
  assert.throws(
    () => new Pgboss({ logEvents: 'error' }),
    (error) =>
      error.constructor === Error &&
      error.message === 'pgboss logEvents must be an array',
  );
  assert.throws(
    () => new Pgboss({ logEvents: ['completed'] }),
    (error) =>
      error.constructor === Error &&
      error.message === 'Unknown pgboss log event: completed',
  );
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

test('lib/pgboss - should send through the existing connection', async () => {
  const calls = [];
  const boss = new Pgboss({ enabled: true });
  boss.client = {
    send: async (...args) => {
      calls.push(args);
      return 'job-1';
    },
  };

  const id = await boss.send('events/local', { id: 1 }, { priority: 2 });

  assert.strictEqual(id, 'job-1');
  assert.deepStrictEqual(calls, [['events/local', { id: 1 }, { priority: 2 }]]);
});

test('lib/pgboss - should send through a supplied transaction', async () => {
  const calls = [];
  const transaction = {
    query: async () => ({ rows: [] }),
  };
  const boss = new Pgboss({ enabled: true });
  boss.client = {
    send: async (...args) => {
      calls.push(args);
      return 'job-1';
    },
  };

  await boss.send('events/local', { id: 1 }, { transaction, priority: 2 });

  assert.strictEqual(calls.length, 1);
  assert.strictEqual(calls[0][0], 'events/local');
  assert.deepStrictEqual(calls[0][1], { id: 1 });
  assert.strictEqual(calls[0][2].priority, 2);
  assert.notStrictEqual(calls[0][2].db, transaction);
});

test('lib/pgboss - should adapt a pg transaction', async () => {
  const queries = [];
  const transaction = {
    query: async (...args) => {
      queries.push(args);
      return { rows: [{ id: 'job-1' }] };
    },
  };
  const boss = new Pgboss({ enabled: true });
  let database = null;
  boss.client = {
    publish: async (name, data, options) => {
      database = options.db;
    },
  };

  await boss.publish('chat:message:create', {}, { transaction });
  const result = await database.executeSql('select $1', [1]);

  assert.deepStrictEqual(queries, [['select $1', [1]]]);
  assert.deepStrictEqual(result, { rows: [{ id: 'job-1' }] });
});
