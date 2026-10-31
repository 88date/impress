'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomBytes } = require('node:crypto');
const { Pool } = require('pg');
const { EventPublisher } = require('../lib/events/publisher.js');

const connectionString = process.env.OUTBOX_TEST_CONNECTION_STRING;

const withTransaction = async (pool, action) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await action(client);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
};

test(
  'outbox integration - autocommit, transaction commit and rollback',
  { skip: !connectionString, timeout: 30000 },
  async (t) => {
    const pool = new Pool({
      connectionString,
      max: 3,
      connectionTimeoutMillis: 5000,
    });
    const schema = 'outboxTest' + randomBytes(8).toString('hex');
    const table = 'eventOutbox';
    const target = `"${schema}"."${table}"`;
    const business = `"${schema}"."business"`;
    let created = false;
    t.after(async () => {
      try {
        if (created) await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
      } finally {
        await pool.end();
      }
    });

    await pool.query(`CREATE SCHEMA "${schema}"`);
    created = true;
    await pool.query(`
      CREATE TABLE ${target} (
        "id" uuid PRIMARY KEY,
        "name" text NOT NULL,
        "subject" text NOT NULL,
        "createdAt" timestamptz NOT NULL,
        "payload" jsonb NOT NULL
      )
    `);
    await pool.query(`CREATE TABLE ${business} ("id" integer PRIMARY KEY)`);
    const publisher = new EventPublisher();
    publisher.configure({ database: pool, schema, table });
    const eventName = 'chat:1:message:created';
    publisher.registerEvent({
      eventName,
      eventSubject: 'events.chat.1.message.created',
    });
    const readOutbox = async () => {
      const { rows } = await pool.query(`SELECT * FROM ${target}`);
      return rows;
    };
    const readBusiness = async () => {
      const { rows } = await pool.query(`SELECT * FROM ${business}`);
      return rows;
    };

    const id = await publisher.emit(eventName, { messageId: 42 });
    const initial = await readOutbox();
    assert.strictEqual(initial.length, 1);
    assert.strictEqual(initial[0].id, id);
    assert.strictEqual(initial[0].name, eventName);
    assert.strictEqual(initial[0].subject, 'events.chat.1.message.created');
    assert.ok(initial[0].createdAt instanceof Date);
    assert.deepStrictEqual(initial[0].payload, { messageId: 42 });

    let committedId;
    await withTransaction(pool, async (client) => {
      await client.query(`INSERT INTO ${business} VALUES (1)`);
      committedId = await publisher.emit(eventName, ['committed', null], {
        transaction: client,
      });
      assert.strictEqual((await readOutbox()).length, 1);
      assert.strictEqual((await readBusiness()).length, 0);
    });
    const committed = await readOutbox();
    assert.strictEqual(committed.length, 2);
    assert.deepStrictEqual(
      committed.find((row) => row.id === committedId).payload,
      ['committed', null],
    );
    assert.strictEqual((await readBusiness()).length, 1);

    const failure = new Error('Business operation failed');
    await assert.rejects(
      withTransaction(pool, async (client) => {
        await client.query(`INSERT INTO ${business} VALUES (2)`);
        await publisher.emit(eventName, null, {
          transaction: client,
        });
        throw failure;
      }),
      (error) => error === failure,
    );
    assert.strictEqual((await readOutbox()).length, 2);
    assert.strictEqual((await readBusiness()).length, 1);

    await pool.query(`
      ALTER TABLE ${target} ADD CONSTRAINT "validPayload"
      CHECK ("payload"->>'reject' IS DISTINCT FROM 'true')
    `);
    await assert.rejects(
      withTransaction(pool, async (client) => {
        await client.query(`INSERT INTO ${business} VALUES (3)`);
        await publisher.emit(
          eventName,
          { reject: true },
          { transaction: client },
        );
      }),
      /validPayload/,
    );
    assert.strictEqual((await readOutbox()).length, 2);
    assert.strictEqual((await readBusiness()).length, 1);

    const nullId = await publisher.emit(eventName, null);
    const { rows: nullRows } = await pool.query(
      `SELECT "payload", "payload" IS NULL AS "isSqlNull" ` +
        `FROM ${target} WHERE "id" = $1`,
      [nullId],
    );
    assert.strictEqual(nullRows[0].payload, null);
    assert.strictEqual(nullRows[0].isSqlNull, false);

    for (const payload of ['text', 42, true]) {
      const scalarId = await publisher.emit(eventName, payload);
      const { rows } = await pool.query(
        `SELECT "payload" FROM ${target} WHERE "id" = $1`,
        [scalarId],
      );
      assert.strictEqual(rows[0].payload, payload);
    }
  },
);
