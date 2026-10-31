'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { TaskConsumer } = require('../lib/task-consumer.js');

const createMessage = (sequence = 42, deliveryCount = 1) => {
  const calls = [];
  return {
    calls,
    info: {
      streamSequence: sequence,
      timestampNanos: 1_700_000_000_000_000_000,
      deliveryCount,
    },
    json: () => ({ automatic: true }),
    ack: () => calls.push(['ack']),
    nak: (delay) => calls.push(['nak', delay]),
  };
};

const createConsumer = (execute, options = {}) => {
  const errors = [];
  const logger = { error: (...args) => errors.push(args) };
  const declaration = {
    name: 'tasks/reports/daily',
    path: 'reports/daily',
    timeout: 1000,
    retryDelay: 200,
    maxMessages: 5,
    ...options,
  };
  const consumer = new TaskConsumer(
    null,
    { stream: 'TASKS' },
    declaration,
    execute,
    logger,
  );
  return { consumer, errors };
};

test('task consumer - acknowledges after worker execution', async () => {
  const message = createMessage();
  let received;
  const { consumer } = createConsumer(async (declaration, job) => {
    assert.deepStrictEqual(message.calls, []);
    received = job;
    assert.strictEqual(declaration.path, 'reports/daily');
  });
  await consumer.handle(message);
  assert.deepStrictEqual(message.calls, [['ack']]);
  assert.deepStrictEqual(received, {
    id: 'TASKS:42',
    name: 'tasks/reports/daily',
    data: { automatic: true },
    createdAt: '2023-11-14T22:13:20.000Z',
    retryCount: 0,
  });
});

test('task consumer - preserves job identity on redelivery', async () => {
  const jobs = [];
  const { consumer } = createConsumer(async (declaration, job) => {
    jobs.push(job);
  });
  await consumer.handle(createMessage(42));
  await consumer.handle(createMessage(43));
  await consumer.handle(createMessage(42, 2));
  assert.notStrictEqual(jobs[0].id, jobs[1].id);
  assert.strictEqual(jobs[0].id, jobs[2].id);
  assert.strictEqual(jobs[2].retryCount, 1);
});

test('task consumer - requests redelivery after failure', async () => {
  const message = createMessage();
  const { consumer, errors } = createConsumer(async () => {
    throw new Error('Worker disconnected');
  });
  await consumer.handle(message);
  assert.deepStrictEqual(message.calls, [['nak', 200]]);
  assert.strictEqual(errors.length, 1);
});

test('task consumer - ignores late results after timeout', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let finish;
  const work = new Promise((resolve) => {
    finish = resolve;
  });
  const { consumer } = createConsumer(() => work, { timeout: 100 });
  const message = createMessage();
  const handling = consumer.handle(message);
  t.mock.timers.tick(100);
  await handling;
  assert.deepStrictEqual(message.calls, [['nak', 200]]);
  finish();
  await work;
  assert.deepStrictEqual(message.calls, [['nak', 200]]);
});

test('task consumer - consumes in parallel and drains on stop', async () => {
  const first = createMessage(1);
  const second = createMessage(2);
  const finishes = [];
  let allStarted;
  const started = new Promise((resolve) => {
    allStarted = resolve;
  });
  const { consumer } = createConsumer(
    async () => {
      const finished = new Promise((resolve) => finishes.push(resolve));
      if (finishes.length === 2) allStarted();
      await finished;
    },
    { maxMessages: 7 },
  );
  let finishMessages;
  const ended = new Promise((resolve) => {
    finishMessages = resolve;
  });
  let closeCount = 0;
  consumer.consumer = {
    consume: async (options) => {
      assert.deepStrictEqual(options, { max_messages: 7 });
      return {
        async *[Symbol.asyncIterator]() {
          yield first;
          yield second;
          await ended;
        },
        stop: () => finishMessages(),
        close: async () => closeCount++,
      };
    },
  };
  consumer.start();
  await started;
  assert.strictEqual(consumer.pending.size, 2);
  const closing = consumer.stop();
  finishes[1]();
  finishes[0]();
  await closing;
  assert.deepStrictEqual(first.calls, [['ack']]);
  assert.deepStrictEqual(second.calls, [['ack']]);
  assert.strictEqual(consumer.pending.size, 0);
  assert.strictEqual(closeCount, 1);
});
