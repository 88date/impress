'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomBytes } = require('node:crypto');
const { Pgboss } = require('../lib/pgboss.js');
const { PgbossSubscriptions } = require('../lib/events/transports.js');
const {
  EventPublisher,
  NATS_EVENT_QUEUE,
} = require('../lib/events/publisher.js');

const connectionString = process.env.PGBOSS_TEST_CONNECTION_STRING;

test(
  'pgboss integration - replaces persisted subscriber bindings',
  {
    skip: !connectionString,
    timeout: 30000,
  },
  async () => {
    const schema = 'impress_test_' + randomBytes(8).toString('hex');
    const boss = new Pgboss({
      enabled: true,
      connectionString,
      schema,
      supervise: false,
      schedule: false,
    });
    await boss.start();
    try {
      const name = 'subscribers/feed/1/updated';
      await boss.client.createQueue(name);
      await boss.client.createQueue('unrelated');
      await boss.client.subscribe('old:1:event', name);
      await boss.client.subscribe('old:1:event', 'unrelated');
      await boss.stop();
      await boss.start();

      const adapter = new PgbossSubscriptions(boss, true);
      const contract = {
        subscriberName: 'feed:1:updated',
        subscriberPath: 'feed/1/updated',
        eventName: 'new:1:event',
      };
      const handle = await adapter.bind(
        adapter.createBinding(contract),
        async () => {},
      );
      await handle.stop();
      const db = boss.client.getDb();
      const sql = `
        SELECT event, name FROM "${schema}".subscription ORDER BY name
      `;
      const { rows } = await db.executeSql(sql);
      assert.deepStrictEqual(rows, [
        { event: 'new:1:event', name },
        { event: 'old:1:event', name: 'unrelated' },
      ]);

      await adapter.removeBinding(contract);
      const cleared = await db.executeSql(sql);
      assert.deepStrictEqual(cleared.rows, [
        { event: 'old:1:event', name: 'unrelated' },
      ]);
      assert.ok(await boss.client.getQueue(name));
    } finally {
      try {
        await boss.client.getDb().executeSql(`DROP SCHEMA "${schema}" CASCADE`);
      } finally {
        await boss.stop();
      }
    }
  },
);

test(
  'pgboss integration - commits event delivery atomically',
  {
    skip: !connectionString,
    timeout: 30000,
  },
  async () => {
    const schema = 'impress_test_' + randomBytes(8).toString('hex');
    const boss = new Pgboss({
      enabled: true,
      connectionString,
      schema,
      max: 1,
      supervise: false,
      schedule: false,
    });
    await boss.start();
    try {
      const eventName = 'chat:1:message:created';
      const subscribers = ['subscribers/feed', 'subscribers/audit'];
      for (const name of subscribers) {
        await boss.client.createQueue(name);
        await boss.client.subscribe(eventName, name);
      }
      const nats = {
        connection: {},
        publishEvent: () => assert.fail('Unexpected direct NATS publication'),
      };
      const publisher = new EventPublisher(boss, nats);
      await publisher.ensureQueue();
      publisher.registerEvent({
        eventName,
        eventSubject: 'chat.1.message.created',
        transports: ['local', 'nats'],
      });
      const database = boss.client.getDb();
      const readJobs = () =>
        database.executeSql(`SELECT name, data FROM "${schema}".job`);

      const id = await publisher.emit(eventName, { messageId: 42 });
      const { rows } = await readJobs();
      assert.strictEqual(rows.length, 3);
      assert.deepStrictEqual(
        rows.map(({ name }) => name).sort(),
        [...subscribers, NATS_EVENT_QUEUE].sort(),
      );
      for (const { name, data } of rows) {
        const event = name === NATS_EVENT_QUEUE ? data.event : data;
        assert.strictEqual(event.id, id);
      }

      await database.executeSql(`
        ALTER TABLE "${schema}".job ADD CONSTRAINT reject_test_outbox
        CHECK (
          name <> '${NATS_EVENT_QUEUE}' OR
          data->'event'->'data'->>'fail' IS DISTINCT FROM 'true'
        )
      `);
      await assert.rejects(
        publisher.emit(eventName, { fail: true }),
        /reject_test_outbox/,
      );
      assert.strictEqual((await readJobs()).rows.length, 3);

      const failure = new Error('Business operation failed');
      await assert.rejects(
        database.withTransaction(async (db) => {
          const transaction = {
            query: (text, values) => db.executeSql(text, values),
          };
          await publisher.emit(eventName, {}, { transaction });
          throw failure;
        }),
        (error) => error === failure,
      );
      assert.strictEqual((await readJobs()).rows.length, 3);

      await Promise.all([
        publisher.emit(eventName, { messageId: 43 }),
        publisher.emit(eventName, { messageId: 44 }),
      ]);
      assert.strictEqual((await readJobs()).rows.length, 9);
    } finally {
      try {
        await boss.client.getDb().executeSql(`DROP SCHEMA "${schema}" CASCADE`);
      } finally {
        await boss.stop();
      }
    }
  },
);
