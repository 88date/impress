'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const { EventPublisher } = require('../lib/events/publisher.js');

const eventName = 'chat:1:messageCreated';
const registerEvent = (events) => {
  events.registerEvent({
    eventName,
    eventSubject: 'events.chat.1.messageCreated',
  });
};

test('event-publisher - writes an outbox record without brokers', async () => {
  const events = new EventPublisher();
  const calls = [];
  events.configure({
    database: { query: async (...args) => calls.push(args) },
  });
  registerEvent(events);

  const id = await events.emit(eventName, { messageId: 42 });

  assert.strictEqual(calls.length, 1);
  const values = calls[0][1];
  assert.match(id, /^[0-9a-f-]{36}$/);
  assert.strictEqual(values[0], id);
  assert.strictEqual(values[1], eventName);
  assert.strictEqual(values[2], 'events.chat.1.messageCreated');
  assert.strictEqual(new Date(values[3]).toISOString(), values[3]);
  assert.deepStrictEqual(JSON.parse(values[4]), { messageId: 42 });
});

test('event-publisher - requires outbox configuration', async () => {
  const events = new EventPublisher();
  registerEvent(events);

  await assert.rejects(events.emit(eventName, {}), {
    name: 'Error',
    message: 'Event outbox is not configured: call events.configure()',
  });
});

test('event-publisher - rejects unknown and removed events', async () => {
  const events = new EventPublisher();
  const calls = [];
  events.configure({
    database: { query: async (...args) => calls.push(args) },
  });

  await assert.rejects(events.emit(eventName, {}), /Unknown event/);
  registerEvent(events);
  await events.emit(eventName, null);
  events.unregisterEvent(eventName);
  await assert.rejects(events.emit(eventName, {}), /Unknown event/);
  assert.strictEqual(calls.length, 1);
});

test('event-publisher - prefers an explicit transaction', async () => {
  const events = new EventPublisher();
  events.configure({
    database: { query: () => assert.fail('Query escaped its transaction') },
  });
  registerEvent(events);
  const calls = [];
  const transaction = { query: async (...args) => calls.push(args) };

  const id = await events.emit(eventName, {}, { transaction });

  assert.strictEqual(calls.length, 1);
  assert.strictEqual(calls[0][1][0], id);
  assert.ok(calls[0][0].startsWith('INSERT INTO'));
});

test('event-publisher - adapter preserves async context', async () => {
  const storage = new AsyncLocalStorage();
  const calls = [];
  const events = new EventPublisher();
  events.configure({
    database: {
      query: async (text, values) => {
        await Promise.resolve();
        calls.push([storage.getStore() ?? 'pool', values[0]]);
      },
    },
  });
  registerEvent(events);

  const publish = () => events.emit(eventName, {});
  const ids = await Promise.all([
    storage.run('transaction-1', publish),
    storage.run('transaction-2', publish),
    publish(),
  ]);

  assert.deepStrictEqual(calls, [
    ['transaction-1', ids[0]],
    ['transaction-2', ids[1]],
    ['pool', ids[2]],
  ]);
});

test('event-publisher - propagates a database failure', async () => {
  const events = new EventPublisher();
  const failure = new Error('Outbox unavailable');
  events.configure({
    database: {
      query: async () => {
        throw failure;
      },
    },
  });
  registerEvent(events);

  await assert.rejects(
    events.emit(eventName, {}),
    (error) => error === failure,
  );
});
