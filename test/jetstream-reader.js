'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { MQConsumer } = require('../lib/mq-consumer.js');
const { TaskConsumer } = require('../lib/task-consumer.js');
const { JetStreamConsumer } = require('../lib/events/consumer.js');

const options = { maxMessages: 2, timeout: 1000, retryDelay: 25 };
const factories = {
  MQ: (consumer, method, logger) =>
    new MQConsumer(consumer, 'test', { run: { method } }, options, logger),
  tasks: (consumer, method, logger) =>
    new TaskConsumer(
      consumer,
      { stream: 'TASKS' },
      { name: 'tasks/test', ...options },
      (declaration, job) => method(job.data, job),
      logger,
    ),
  events: (consumer, method, logger) =>
    new JetStreamConsumer(
      consumer,
      { subscriberName: 'test', ...options },
      method,
      logger,
    ),
};

const createMessage = (sequence = 0) => {
  const calls = [];
  const acknowledged = Promise.withResolvers();
  return {
    calls,
    acknowledged: acknowledged.promise,
    subject: 'mq.test.run',
    info: {
      streamSequence: sequence + 1,
      timestampNanos: 1_700_000_000_000_000_000,
      deliveryCount: 1,
    },
    json: () => ({ sequence }),
    ack: () => {
      calls.push(['ack']);
      acknowledged.resolve();
    },
    nak: (delay) => calls.push(['nak', delay]),
  };
};

for (const name of Object.keys(factories)) {
  const create = factories[name];

  test(`${name} reader - parallel dispatch and idempotent drain`, async (t) => {
    const messages = [createMessage(0), createMessage(1), createMessage(2)];
    const gates = messages.map(() => Promise.withResolvers());
    const entered = Promise.withResolvers();
    const ended = Promise.withResolvers();
    const errors = [];
    const logger = { error: (...args) => errors.push(args) };
    let started = 0;
    let reads = 0;
    let closes = 0;
    const native = {
      consume: async (config) => {
        reads++;
        assert.deepStrictEqual(config, { max_messages: 2 });
        return {
          async *[Symbol.asyncIterator]() {
            yield* messages;
            await ended.promise;
          },
          stop: () => ended.resolve(),
          close: async () => closes++,
        };
      },
    };
    const consumer = create(
      native,
      async ({ sequence }) => {
        if (++started === messages.length) entered.resolve();
        await gates[sequence].promise;
      },
      logger,
    );
    t.after(async () => {
      for (const gate of gates) gate.resolve();
      await consumer.stop();
    });
    consumer.start();
    consumer.start();
    await entered.promise;
    const stopping = consumer.stop();
    assert.strictEqual(consumer.stop(), stopping);
    let stopped = false;
    const completion = stopping.then(() => {
      stopped = true;
    });
    consumer.start();
    await consumer.consuming;
    assert.strictEqual(stopped, false);
    gates[1].resolve();
    await messages[1].acknowledged;
    assert.deepStrictEqual(messages[0].calls, []);
    assert.deepStrictEqual(messages[2].calls, []);
    assert.strictEqual(stopped, false);
    gates[2].resolve();
    gates[0].resolve();
    await completion;
    consumer.start();
    assert.strictEqual(reads, 1);
    assert.strictEqual(closes, 1);
    assert.strictEqual(consumer.pending.size, 0);
    assert.strictEqual(consumer.running, false);
    assert.strictEqual(consumer.messages, null);
    assert.deepStrictEqual(errors, []);
    for (const message of messages) {
      assert.deepStrictEqual(message.calls, [['ack']]);
    }
  });

  test(`${name} reader - stop during opening returns message`, async () => {
    const opening = Promise.withResolvers();
    const message = createMessage();
    let stops = 0;
    let closes = 0;
    const consumer = create(
      { consume: () => opening.promise },
      () => assert.fail('Handler started after stop'),
      { error: assert.fail },
    );
    consumer.start();
    const stopping = consumer.stop();
    assert.strictEqual(consumer.stop(), stopping);
    opening.resolve({
      async *[Symbol.asyncIterator]() {
        yield message;
      },
      stop: () => stops++,
      close: async () => closes++,
    });
    await stopping;
    assert.deepStrictEqual(message.calls, [['nak', undefined]]);
    assert.strictEqual(stops, 1);
    assert.strictEqual(closes, 1);
    assert.strictEqual(consumer.messages, null);
  });

  test(`${name} reader - drains after read and close errors`, async (t) => {
    const entered = Promise.withResolvers();
    const finished = Promise.withResolvers();
    const message = createMessage();
    const readError = new Error('Reader failed');
    const closeError = new Error('Close failed');
    const errors = [];
    let reads = 0;
    const consumer = create(
      {
        consume: async () => {
          reads++;
          return {
            async *[Symbol.asyncIterator]() {
              yield message;
              await entered.promise;
              throw readError;
            },
            stop: () => {},
            close: async () => {
              throw closeError;
            },
          };
        },
      },
      async () => {
        entered.resolve();
        await finished.promise;
      },
      { error: (...args) => errors.push(args) },
    );
    t.after(async () => {
      finished.resolve();
      await consumer.stop();
    });
    consumer.start();
    await consumer.consuming;
    assert.deepStrictEqual(
      errors.map((args) => args[1]),
      [readError, closeError],
    );
    assert.strictEqual(consumer.running, false);
    assert.strictEqual(consumer.messages, null);
    let stopped = false;
    const stopping = consumer.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    assert.strictEqual(stopped, false);
    finished.resolve();
    await stopping;
    assert.strictEqual(reads, 1);
    assert.strictEqual(consumer.pending.size, 0);
    assert.deepStrictEqual(message.calls, [['ack']]);
  });

  test(`${name} reader - opening failure stops without retrying`, async () => {
    const failure = new Error('Cannot open reader');
    const errors = [];
    let reads = 0;
    const consumer = create(
      {
        consume: async () => {
          reads++;
          throw failure;
        },
      },
      () => assert.fail('Handler started without reader'),
      { error: (...args) => errors.push(args) },
    );
    consumer.start();
    await consumer.consuming;
    await consumer.stop();
    assert.strictEqual(reads, 1);
    assert.strictEqual(consumer.running, false);
    assert.strictEqual(consumer.messages, null);
    assert.strictEqual(consumer.pending.size, 0);
    assert.deepStrictEqual(
      errors.map((args) => args[1]),
      [failure],
    );
  });
}
