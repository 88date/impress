'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { MQConsumer } = require('../lib/mq-consumer.js');

const createMessage = () => {
  const calls = [];
  return {
    calls,
    subject: 'mq.test.run',
    info: {
      streamSequence: 42,
      timestampNanos: 1_700_000_000_000_000_000,
      deliveryCount: 1,
    },
    json: () => ({ value: 42 }),
    ack: () => calls.push(['ack']),
    nak: (delay) => calls.push(['nak', delay]),
  };
};

const createConsumer = (method) =>
  new MQConsumer(
    null,
    'test',
    { run: { method } },
    { timeout: 100, retryDelay: 25 },
    { error() {} },
  );

test('mq consumer - rejects unknown jobs and malformed JSON', async () => {
  const unknown = createMessage();
  unknown.subject = 'mq.test.missing';
  const malformed = createMessage();
  malformed.json = () => {
    throw new Error('Invalid JSON');
  };
  let handled = 0;
  const consumer = createConsumer(() => handled++);
  for (const message of [unknown, malformed]) {
    await consumer.handle(message);
    assert.deepStrictEqual(message.calls, [['nak', 25]]);
  }
  assert.strictEqual(handled, 0);
});

test('mq consumer - timeout aborts and ignores late completion', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const finished = Promise.withResolvers();
  let signal;
  const consumer = createConsumer((data, job) => {
    signal = job.signal;
    return finished.promise;
  });
  const message = createMessage();
  const handling = consumer.handle(message);
  await Promise.resolve();
  assert.strictEqual(signal.aborted, false);
  t.mock.timers.tick(100);
  await handling;
  assert.strictEqual(signal.aborted, true);
  assert.strictEqual(signal.reason.message, 'MQ job timed out');
  assert.deepStrictEqual(message.calls, [['nak', 25]]);
  finished.resolve();
  await finished.promise;
  await Promise.resolve();
  assert.deepStrictEqual(message.calls, [['nak', 25]]);
});

test('mq consumer - creates the requested delivery limit', async () => {
  const cases = [
    [{}, -1],
    [{ retryLimit: 0 }, 1],
    [{ retryLimit: 2 }, 3],
    [{ retryLimit: -1 }, -1],
  ];
  for (const [options, expected] of cases) {
    const created = [];
    const manager = {
      consumers: {
        info: async () => {
          throw Object.assign(new Error('Consumer not found'), { code: 10014 });
        },
        add: async (stream, config) => created.push([stream, config]),
        update: () => assert.fail('Unexpected consumer update'),
      },
    };
    const client = { consumers: { get: async () => ({}) } };
    const nats = { getJetStream: async () => ({ client, manager }) };
    await MQConsumer.create(nats, 'test', {}, options);
    assert.strictEqual(created.length, 1);
    assert.strictEqual(created[0][0], 'MQ');
    assert.strictEqual(created[0][1].max_deliver, expected);
  }
});

test('mq consumer - creates missing MQ before its consumer', async (t) => {
  for (const [jetstream, replicas] of [
    [undefined, 1],
    [{ replicas: 3 }, 3],
  ]) {
    const calls = [];
    const nativeConsumer = {};
    const addStream = t.mock.fn(async () => calls.push('stream'));
    const addConsumer = t.mock.fn(async () => calls.push('consumer'));
    const manager = {
      streams: { add: addStream },
      consumers: {
        info: async () => {
          throw Object.assign(new Error('Stream not found'), { code: 10059 });
        },
        add: addConsumer,
      },
    };
    const client = { consumers: { get: async () => nativeConsumer } };
    const nats = {
      application: { config: { server: { nats: { jetstream } } } },
      getJetStream: async () => ({ client, manager }),
    };
    const consumer = await MQConsumer.create(nats, 'test', {});
    assert.strictEqual(consumer.consumer, nativeConsumer);
    assert.strictEqual(consumer.running, false);
    assert.deepStrictEqual(calls, ['stream', 'consumer']);
    assert.deepStrictEqual(addStream.mock.calls[0].arguments, [
      {
        name: 'MQ',
        subjects: ['mq.>'],
        storage: 'file',
        retention: 'workqueue',
        num_replicas: replicas,
        allow_msg_schedules: true,
        allow_atomic: true,
      },
    ]);
    assert.strictEqual(addConsumer.mock.calls[0].arguments[0], 'MQ');
    assert.strictEqual(
      addConsumer.mock.calls[0].arguments[1].durable_name,
      'mq-test',
    );
  }
});

test('mq consumer - preserves lookup and creation errors', async (t) => {
  const missing = Object.assign(new Error('Stream not found'), { code: 10059 });
  const permission = new Error('Permission denied');
  const cases = [
    ['CUSTOM', 'lookup', missing],
    ['MQ', 'lookup', permission],
    ['MQ', 'create', permission],
  ];
  for (const [stream, phase, error] of cases) {
    const addStream = t.mock.fn(async () => {
      throw error;
    });
    const addConsumer = t.mock.fn();
    const manager = {
      streams: { add: addStream },
      consumers: {
        info: async () => {
          throw phase === 'lookup' ? error : missing;
        },
        add: addConsumer,
      },
    };
    const nats = {
      application: { config: { server: { nats: {} } } },
      getJetStream: async () => ({ manager }),
    };
    await assert.rejects(
      MQConsumer.create(nats, 'test', {}, { stream }),
      (reason) => reason === error,
    );
    assert.strictEqual(addStream.mock.callCount(), phase === 'create' ? 1 : 0);
    assert.strictEqual(addConsumer.mock.callCount(), 0);
  }
});

test('mq consumer - updates only explicitly changed limits', async () => {
  const original = {
    filter_subject: 'mq.test.*',
    ack_policy: 'explicit',
    max_ack_pending: 9,
    ack_wait: 2_000_000_000,
    max_deliver: 7,
    backoff: [2_000_000_000, 4_000_000_000],
    deliver_policy: 'new',
    replay_policy: 'original',
  };
  const cases = [
    [{}, {}],
    [{ maxAckPending: 9, retryLimit: 6 }, {}],
    [{ maxAckPending: 1 }, { max_ack_pending: 1 }],
    [{ retryLimit: 2 }, { max_deliver: 3 }],
    [{ retryLimit: 0 }, { max_deliver: 1 }],
    [{ retryLimit: -1 }, { max_deliver: -1 }],
    [
      { maxAckPending: 1, retryLimit: 2 },
      { max_ack_pending: 1, max_deliver: 3 },
    ],
  ];
  for (const [options, expected] of cases) {
    const config = structuredClone(original);
    const updates = [];
    const nativeConsumer = {};
    const manager = {
      consumers: {
        info: async () => ({ config }),
        add: () => assert.fail('Unexpected consumer creation'),
        update: async (stream, name, options) => {
          updates.push([stream, name, options]);
          Object.assign(config, options);
        },
      },
    };
    const client = { consumers: { get: async () => nativeConsumer } };
    const nats = { getJetStream: async () => ({ client, manager }) };
    const consumer = await MQConsumer.create(nats, 'test', {}, options);
    assert.strictEqual(consumer.consumer, nativeConsumer);
    assert.strictEqual(consumer.running, false);
    assert.deepStrictEqual(
      updates,
      Object.keys(expected).length ? [['MQ', 'mq-test', expected]] : [],
    );
    assert.deepStrictEqual(config, {
      ...original,
      ...expected,
    });
  }
});
