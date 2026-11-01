'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { JetStreamSubscriptions } = require('../lib/events/transports.js');
const { JetStreamConsumer } = require('../lib/events/consumer.js');

const createTransport = (managesTopology = true, jetstream = {}) => {
  const nats = {
    connection: {},
    isStopping: false,
    application: { config: { server: { nats: { jetstream } } } },
  };
  return new JetStreamSubscriptions(nats, console, managesTopology);
};

const createContract = (options = {}) => ({
  subscriberName: 'feed:1:message:created',
  stream: 'EVENTS',
  subject: 'events.chat.1.message.created',
  ...options,
});

const createRecord = () => ({
  id: 'event-1',
  name: 'chat:1:message:created',
  createdAt: '2026-10-31T00:00:00.000Z',
  payload: { messageId: 42 },
});

const createMessage = (record = createRecord()) => {
  const calls = [];
  return {
    calls,
    json: () => record,
    ack: () => calls.push(['ack']),
    nak: (delay) => calls.push(['nak', delay]),
  };
};

const createConsumer = (dispatch, options = {}) => {
  const errors = [];
  const logger = { error: (...args) => errors.push(args) };
  const binding = createTransport().createBinding(createContract(options));
  const consumer = new JetStreamConsumer(null, binding, dispatch, logger);
  return { consumer, errors };
};

test('JetStream - instances share readable durable names', () => {
  const transport = createTransport();
  const first = transport.createBinding(createContract());
  const same = createTransport().createBinding(
    createContract({ maxMessages: 10 }),
  );
  const other = transport.createBinding(
    createContract({ subscriberName: 'audit:1:message:created' }),
  );
  const moved = transport.createBinding(
    createContract({ subject: 'events.chat.1.message.updated' }),
  );
  const prefixed = createTransport(true, { consumerPrefix: 'backend' });
  const separate = prefixed.createBinding(createContract());
  assert.strictEqual(
    first.consumerName,
    'impress|feed:1:message:created|events:chat:1:message:created',
  );
  assert.strictEqual(first.options.durable_name, first.consumerName);
  assert.strictEqual(first.consumerName, same.consumerName);
  assert.notStrictEqual(first.consumerName, other.consumerName);
  assert.notStrictEqual(first.consumerName, moved.consumerName);
  assert.notStrictEqual(first.consumerName, separate.consumerName);
  assert.strictEqual(first.subject, 'events.chat.1.message.created');
  assert.strictEqual(first.stream, 'EVENTS');
});

test('JetStream - uses explicit streams and subject filters', () => {
  const transport = createTransport();
  const subjects = [
    'cdc.public.*',
    'cdc.public.>',
    'cdc.public.%2A',
    'cdc.public.table',
    'cdc.public:table',
  ];
  const names = new Set();
  for (const subject of subjects) {
    const binding = transport.createBinding(
      createContract({ stream: 'CDC', subject }),
    );
    assert.strictEqual(binding.stream, 'CDC');
    assert.strictEqual(binding.subject, subject);
    assert.strictEqual(binding.options.filter_subject, subject);
    assert.doesNotMatch(binding.consumerName, /[.*>]/);
    names.add(binding.consumerName);
  }
  assert.strictEqual(names.size, subjects.length);
});

test('JetStream - waits for externally provisioned streams', async () => {
  for (const stream of ['EVENTS', 'CDC']) {
    const owner = createTransport(true);
    const passive = createTransport(false);
    const manager = {
      streams: {
        info: async (name) => {
          assert.strictEqual(name, stream);
          throw Object.assign(new Error('Stream not found'), { code: 10059 });
        },
        add: () => assert.fail('Unexpected stream creation'),
        update: () => assert.fail('Unexpected stream update'),
      },
    };
    const getJetStream = async () => ({ manager });
    owner.nats.getJetStream = getJetStream;
    passive.nats.getJetStream = getJetStream;
    assert.strictEqual(await passive.ensureStream(stream), false);
    assert.strictEqual(await owner.ensureStream(stream), false);
  }
});

test('JetStream - reuses existing stream settings and messages', async () => {
  for (const managesTopology of [true, false]) {
    const config = {};
    const transport = createTransport(managesTopology, config);
    const info = {
      config: { name: 'EVENTS', num_replicas: 3, max_bytes: 1000000 },
      state: { messages: 42 },
    };
    transport.nats.getJetStream = async () => ({
      manager: {
        streams: {
          info: async () => structuredClone(info),
          add: () => assert.fail('Unexpected stream creation'),
          update: () => assert.fail('Unexpected stream update'),
          delete: () => assert.fail('Unexpected stream deletion'),
          purge: () => assert.fail('Unexpected stream purge'),
        },
      },
    });
    assert.strictEqual(await transport.ensureStream('EVENTS'), true);
    config.replicas = 1;
    assert.strictEqual(await transport.ensureStream('EVENTS'), true);
  }
});

test('JetStream - propagates stream lookup errors', async () => {
  for (const managesTopology of [true, false]) {
    const transport = createTransport(managesTopology);
    const failure = new Error('Permission denied');
    const manager = {
      streams: {
        info: async () => {
          throw failure;
        },
        add: () => assert.fail('Unexpected stream creation'),
      },
    };
    transport.nats.getJetStream = async () => ({ manager });
    await assert.rejects(
      transport.ensureStream('EVENTS'),
      (error) => error === failure,
    );
  }
});

test('JetStream - stream preparation waits for a connection', async () => {
  const transport = createTransport();
  transport.nats.getJetStream = () => assert.fail('Connection is unavailable');
  transport.nats.connection = null;
  assert.strictEqual(await transport.ensureStream('EVENTS'), false);
  transport.nats.connection = {};
  transport.nats.isStopping = true;
  assert.strictEqual(await transport.ensureStream('EVENTS'), false);
});

test('JetStream - only owner creates missing consumers', async () => {
  const owner = createTransport();
  const passive = createTransport(false);
  const binding = owner.createBinding(createContract());
  const calls = [];
  const manager = {
    consumers: {
      info: async () => {
        throw Object.assign(new Error('Consumer not found'), { code: 10014 });
      },
      add: async (...args) => calls.push(args),
    },
  };
  assert.strictEqual(await passive.ensureConsumer(manager, binding), false);
  assert.deepStrictEqual(calls, []);
  assert.strictEqual(await owner.ensureConsumer(manager, binding), true);
  assert.deepStrictEqual(calls, [[binding.stream, binding.options]]);
});

test('JetStream - sets the initial pending limit from the declaration', () => {
  const transport = createTransport();
  for (const maxAckPending of [undefined, -1, 1, 50]) {
    const binding = transport.createBinding(createContract({ maxAckPending }));
    assert.strictEqual(binding.maxAckPending, maxAckPending);
    assert.strictEqual(binding.options.max_ack_pending, maxAckPending ?? -1);
  }
});

test('JetStream - only owner applies an explicit pending limit', async () => {
  const owner = createTransport();
  const passive = createTransport(false);
  for (const maxAckPending of [-1, 1, 50]) {
    const binding = owner.createBinding(createContract({ maxAckPending }));
    const info = {
      config: {
        ...binding.options,
        max_ack_pending: maxAckPending === 1 ? 50 : 1,
        max_deliver: -1,
        ack_wait: 5_000_000_000,
        backoff: [5_000_000_000, 10_000_000_000],
      },
      delivered: { consumer_seq: 42, stream_seq: 42 },
      ack_floor: { consumer_seq: 41, stream_seq: 41 },
    };
    const original = structuredClone(info);
    const calls = [];
    const manager = {
      consumers: {
        info: async () => info,
        add: () => assert.fail('Unexpected consumer creation'),
        delete: () => assert.fail('Unexpected consumer deletion'),
        update: async (stream, name, options) => {
          calls.push([stream, name, options]);
          Object.assign(info.config, options);
        },
      },
    };
    assert.strictEqual(await passive.ensureConsumer(manager, binding), false);
    assert.deepStrictEqual(calls, []);
    assert.strictEqual(await owner.ensureConsumer(manager, binding), true);
    assert.strictEqual(await passive.ensureConsumer(manager, binding), true);
    assert.strictEqual(await owner.ensureConsumer(manager, binding), true);
    const updates = { max_ack_pending: maxAckPending };
    assert.deepStrictEqual(calls, [
      [binding.stream, binding.consumerName, updates],
    ]);
    original.config.max_ack_pending = maxAckPending;
    assert.deepStrictEqual(info, original);
  }
});

test('JetStream - preserves existing consumer delivery settings', async () => {
  for (const managesTopology of [true, false]) {
    const transport = createTransport(managesTopology);
    const binding = transport.createBinding(createContract({ retryLimit: 5 }));
    const info = {
      config: {
        ...binding.options,
        ack_wait: 5_000_000_000,
        max_deliver: -1,
        max_ack_pending: 1,
        backoff: [5_000_000_000, 10_000_000_000],
        replay_policy: 'original',
      },
      delivered: { consumer_seq: 42, stream_seq: 42 },
      ack_floor: { consumer_seq: 41, stream_seq: 41 },
    };
    const original = structuredClone(info);
    const manager = {
      consumers: {
        info: async () => info,
        add: () => assert.fail('Unexpected consumer creation'),
        update: () => assert.fail('Unexpected consumer update'),
        delete: () => assert.fail('Unexpected consumer deletion'),
      },
    };
    assert.strictEqual(await transport.ensureConsumer(manager, binding), true);
    assert.deepStrictEqual(info, original);
  }
});

test('JetStream - rejects a conflicting durable configuration', async () => {
  const transport = createTransport();
  const binding = transport.createBinding(createContract());
  const cases = [
    { filter_subject: 'events.>' },
    { ack_policy: 'none' },
    { ack_policy: 'all' },
    { deliver_subject: 'consumer.push' },
  ];
  for (const options of cases) {
    const manager = {
      consumers: {
        info: async () => ({ config: { ...binding.options, ...options } }),
        update: () => assert.fail('Unexpected consumer update'),
      },
    };
    await assert.rejects(
      transport.ensureConsumer(manager, binding),
      /Conflicting JetStream consumer/,
    );
  }
});

test('JetStream - propagate lookup permission errors', async () => {
  const transport = createTransport();
  const binding = transport.createBinding(createContract());
  const failure = new Error('Permission denied');
  const manager = {
    consumers: {
      info: async () => {
        throw failure;
      },
      add: () => assert.fail('Unexpected consumer creation'),
    },
  };
  await assert.rejects(
    transport.ensureConsumer(manager, binding),
    (error) => error === failure,
  );
});

test('JetStream - preserves existing initial delivery options', async () => {
  const transport = createTransport();
  const cases = [
    { deliverPolicy: 'new' },
    { deliverPolicy: 'by_start_sequence', startSequence: 100 },
    { deliverPolicy: 'by_start_time', startTime: '2026-10-31T00:00:00Z' },
  ];
  for (const options of cases) {
    const original = transport.createBinding(createContract(options));
    const changed = transport.createBinding(
      createContract({
        ...options,
        deliverPolicy:
          options.deliverPolicy === 'new' ? 'all' : options.deliverPolicy,
        startSequence: 200,
        startTime: '2026-11-01T00:00:00Z',
      }),
    );
    const manager = {
      consumers: {
        info: async () => ({ config: original.options }),
        add: () => assert.fail('Unexpected consumer creation'),
        update: () => assert.fail('Unexpected consumer update'),
        delete: () => assert.fail('Unexpected consumer deletion'),
      },
    };
    assert.strictEqual(await transport.ensureConsumer(manager, changed), true);
  }
});

test('JetStream - accepts equivalent start timestamp formats', async () => {
  const transport = createTransport();
  const binding = transport.createBinding(
    createContract({
      deliverPolicy: 'by_start_time',
      startTime: '2026-10-31T03:00:00.000+03:00',
    }),
  );
  const manager = {
    consumers: {
      info: async () => ({
        config: { ...binding.options, opt_start_time: '2026-10-31T00:00:00Z' },
      }),
      update: () => assert.fail('Unexpected consumer update'),
    },
  };
  assert.strictEqual(await transport.ensureConsumer(manager, binding), true);
});

test('JetStream - ACK only after handler completion', async () => {
  const finished = Promise.withResolvers();
  const entered = Promise.withResolvers();
  const message = createMessage();
  const { consumer } = createConsumer(async (record, { signal }) => {
    assert.deepStrictEqual(record, createRecord());
    assert.strictEqual(signal.aborted, false);
    entered.resolve();
    await finished.promise;
  });
  const handling = consumer.handle(message);
  await entered.promise;
  assert.deepStrictEqual(message.calls, []);
  finished.resolve();
  await handling;
  assert.deepStrictEqual(message.calls, [['ack']]);
});

test('JetStream - NAK with configured delay after failure', async () => {
  const failure = new Error('Handler failed');
  const { consumer, errors } = createConsumer(
    async () => {
      throw failure;
    },
    { retryDelay: 500 },
  );
  const message = createMessage();
  await consumer.handle(message);
  assert.deepStrictEqual(message.calls, [['nak', 500]]);
  assert.strictEqual(errors[0][1], failure);
});

test('JetStream - timeout aborts and ignores late completion', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const finished = Promise.withResolvers();
  let signal;
  const { consumer, errors } = createConsumer(
    (record, context) => {
      signal = context.signal;
      return finished.promise;
    },
    { timeout: 100, retryDelay: 100 },
  );
  const message = createMessage();
  const handling = consumer.handle(message);
  await Promise.resolve();
  assert.strictEqual(signal.aborted, false);
  t.mock.timers.tick(100);
  await handling;
  assert.strictEqual(signal.aborted, true);
  assert.strictEqual(signal.reason.message, 'Event subscriber timed out');
  assert.strictEqual(errors[0][1], signal.reason);
  assert.deepStrictEqual(message.calls, [['nak', 100]]);
  finished.resolve();
  await finished.promise;
  await Promise.resolve();
  assert.deepStrictEqual(message.calls, [['nak', 100]]);
});

test('JetStream - one reader starts handlers without waiting', async () => {
  const entered = Promise.withResolvers();
  const finished = Promise.withResolvers();
  const closed = Promise.withResolvers();
  let handlers = 0;
  const { consumer } = createConsumer(
    async () => {
      if (++handlers === 3) entered.resolve();
      await finished.promise;
    },
    { maxMessages: 2 },
  );
  const messages = [createMessage(), createMessage(), createMessage()];
  const calls = [];
  consumer.consumer = {
    consume: async (options) => {
      calls.push(options);
      return {
        async *[Symbol.asyncIterator]() {
          for (const message of messages) yield message;
          await closed.promise;
        },
        stop: () => closed.resolve(),
        close: async () => closed.resolve(),
      };
    },
  };
  consumer.start();
  await entered.promise;
  let stopped = false;
  const stopping = consumer.stop().then(() => {
    stopped = true;
  });
  await Promise.resolve();
  assert.strictEqual(stopped, false);
  finished.resolve();
  await stopping;
  assert.deepStrictEqual(calls, [{ max_messages: 2 }]);
  assert.strictEqual(consumer.pending.size, 0);
  for (const message of messages) {
    assert.deepStrictEqual(message.calls, [['ack']]);
  }
});

test('JetStream - stop while a reader is opening', async () => {
  const opening = Promise.withResolvers();
  const closed = Promise.withResolvers();
  let stopped = false;
  const { consumer } = createConsumer(() => assert.fail('Stopped consumer'));
  const message = createMessage();
  consumer.consumer = { consume: () => opening.promise };
  consumer.start();
  const stopping = consumer.stop();
  opening.resolve({
    async *[Symbol.asyncIterator]() {
      try {
        yield message;
      } finally {
        closed.resolve();
      }
    },
    stop: () => {
      stopped = true;
    },
    close: () => closed.promise,
  });
  await stopping;
  assert.strictEqual(stopped, true);
  assert.deepStrictEqual(message.calls, [['nak', undefined]]);
});

test('JetStream - closes an empty reader opened after stop', async () => {
  const opening = Promise.withResolvers();
  const stopped = Promise.withResolvers();
  const closed = Promise.withResolvers();
  const { consumer } = createConsumer(() => assert.fail('Stopped consumer'));
  consumer.consumer = { consume: () => opening.promise };
  consumer.start();
  const stopping = consumer.stop();
  opening.resolve({
    async *[Symbol.asyncIterator]() {
      try {
        await stopped.promise;
        yield* [];
      } finally {
        closed.resolve();
      }
    },
    stop: () => stopped.resolve(),
    close: () => closed.promise,
  });
  await stopping;
  assert.strictEqual(consumer.messages, null);
});

test('JetStream - drain after a read failure waits for handlers', async () => {
  const entered = Promise.withResolvers();
  const finished = Promise.withResolvers();
  const failed = Promise.withResolvers();
  const failure = new Error('Reader failed');
  const { consumer } = createConsumer(async () => {
    entered.resolve();
    await finished.promise;
  });
  const message = createMessage();
  let reads = 0;
  consumer.logger = { error: (...args) => failed.resolve(args) };
  consumer.consumer = {
    consume: async () => {
      reads++;
      return {
        async *[Symbol.asyncIterator]() {
          yield message;
          throw failure;
        },
        stop: () => {},
        close: async () => {},
      };
    },
  };
  consumer.start();
  await entered.promise;
  const errors = await failed.promise;
  assert.strictEqual(errors[1], failure);
  await consumer.consuming;
  assert.strictEqual(reads, 1);
  assert.strictEqual(consumer.running, false);
  assert.strictEqual(consumer.messages, null);
  assert.strictEqual(consumer.pending.size, 1);
  let stopped = false;
  const stopping = consumer.stop().then(() => {
    stopped = true;
  });
  await Promise.resolve();
  assert.strictEqual(stopped, false);
  finished.resolve();
  await stopping;
  assert.deepStrictEqual(message.calls, [['ack']]);
});

test('JetStream - opening failure stops without retrying', async () => {
  const failure = new Error('Cannot open consumer');
  const { consumer, errors } = createConsumer(() => {});
  let reads = 0;
  consumer.consumer = {
    consume: async () => {
      reads++;
      throw failure;
    },
  };
  consumer.start();
  await consumer.consuming;
  assert.strictEqual(reads, 1);
  assert.strictEqual(consumer.running, false);
  assert.strictEqual(consumer.messages, null);
  assert.strictEqual(errors[0][1], failure);
  await consumer.stop();
});
