'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Outbox } = require('../lib/events/outbox.js');

const event = {
  id: '40484806-997c-43bc-8000-5ba599aa7aa4',
  name: 'chat:1:messageCreated',
  subject: 'events.chat.1.messageCreated',
  createdAt: '2026-10-31T12:00:00.000Z',
  payload: { messageId: 42 },
};

test('outbox - quotes identifiers and parameterizes event data', async () => {
  const calls = [];
  const database = {
    async query(text, values) {
      assert.strictEqual(this, database);
      calls.push({ text, values });
    },
  };
  const outbox = new Outbox({
    database,
    schema: 'eventSchema',
    table: 'event"Outbox',
  });
  const payload = { text: 'DROP TABLE "outbox"; --' };

  await outbox.write({ ...event, payload });

  assert.strictEqual(calls.length, 1);
  assert.strictEqual(
    calls[0].text,
    'INSERT INTO "eventSchema"."event""Outbox" ' +
      '("id", "name", "subject", "createdAt", "payload") ' +
      'VALUES ($1, $2, $3, $4, $5::text::jsonb)',
  );
  assert.deepStrictEqual(calls[0].values, [
    event.id,
    event.name,
    event.subject,
    event.createdAt,
    JSON.stringify(payload),
  ]);
});

test('outbox - preserves JSON payloads without an envelope', async () => {
  const payloads = [{ nested: [1, null] }, [1, 'two'], 'text', 42, true, null];
  const stored = [];
  const outbox = new Outbox({
    database: {
      query: async (text, values) => {
        assert.ok(text.startsWith('INSERT INTO "public"."outbox"'));
        stored.push(JSON.parse(values[4]));
      },
    },
  });

  for (const payload of payloads) await outbox.write({ ...event, payload });

  assert.deepStrictEqual(stored, payloads);
});

test('outbox - rejects unserializable payloads before querying', async () => {
  const outbox = new Outbox({
    database: { query: () => assert.fail('Unexpected query') },
  });
  const circular = {};
  circular.self = circular;
  for (const payload of [undefined, () => {}, Symbol('event'), 1n, circular]) {
    await assert.rejects(outbox.write({ ...event, payload }), {
      name: 'Error',
      message: 'Outbox payload must be JSON-serializable',
    });
  }
  const failure = new Error('Cannot serialize payload');
  const payload = {
    toJSON() {
      throw failure;
    },
  };
  await assert.rejects(outbox.write({ ...event, payload }), {
    name: 'Error',
    message: 'Outbox payload must be JSON-serializable',
    cause: failure,
  });
});

test('outbox - uses the transaction and waits for INSERT', async () => {
  const pending = Promise.withResolvers();
  const calls = [];
  const transaction = {
    query(text, values) {
      assert.strictEqual(this, transaction);
      calls.push({ text, values });
      return pending.promise;
    },
  };
  const outbox = new Outbox({
    database: { query: () => assert.fail('Query escaped its transaction') },
  });
  let finished = false;
  const writing = outbox.write(event, transaction).then(() => {
    finished = true;
  });

  await Promise.resolve();
  assert.strictEqual(finished, false);
  assert.strictEqual(calls.length, 1);
  assert.ok(calls[0].text.startsWith('INSERT INTO'));
  pending.resolve();
  await writing;
  assert.strictEqual(finished, true);
});

test('outbox - never falls back after a transaction error', async () => {
  const failure = new Error('Transaction aborted');
  const outbox = new Outbox({
    database: { query: () => assert.fail('Unexpected fallback') },
  });

  await assert.rejects(
    outbox.write(event, {
      query: async () => {
        throw failure;
      },
    }),
    (error) => error === failure,
  );
});
