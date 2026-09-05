'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { Pgboss } = require('../lib/pgboss.js');
const {
  EventPublisher,
  NATS_EVENT_QUEUE,
} = require('../lib/events/publisher.js');

const directory = path.dirname(require.resolve('pg-boss'));
const { default: Database } = require(path.join(directory, 'db.js'));
const { default: Manager } = require(path.join(directory, 'manager.js'));
const EVENT_NAME = 'chat:1:message:created';
const SUBSCRIBERS = ['subscribers/feed', 'subscribers/audit'];

const createPublisher = ({ beforeQuery = async () => {} } = {}) => {
  const committed = [];
  const sessions = [];
  const database = new Database({});
  database.opened = true;
  database.pool = {
    query: async () => {
      throw new Error('Query escaped its transaction');
    },
    connect: async () => {
      const session = { queries: [], pending: [], released: false };
      sessions.push(session);
      return {
        query: async (text, values) => {
          session.queries.push(text);
          await beforeQuery(text, values);
          if (text === 'COMMIT') committed.push(...session.pending);
          if (text === 'ROLLBACK') session.pending.length = 0;
          if (text.includes('INSERT INTO pgboss.job')) {
            const jobs = JSON.parse(values[0]);
            session.pending.push(...jobs);
            return { rows: jobs.map(() => ({ id: 'job-1' })) };
          }
          if (text.includes('FROM pgboss.subscription')) {
            return { rows: SUBSCRIBERS.map((name) => ({ name })) };
          }
          if (values?.[0] instanceof Array) {
            const rows = values[0].map((name) => ({
              name,
              table: 'job',
              policy: 'standard',
              notify: false,
            }));
            return { rows };
          }
          return { rows: [] };
        },
        release: () => {
          session.released = true;
        },
      };
    },
  };

  const manager = new Manager(database, { schema: 'pgboss' });
  manager.getDb = () => database;
  const pgboss = new Pgboss({ enabled: true });
  pgboss.client = manager;
  pgboss.bindTransactions(database);
  const nats = {
    connection: {},
    publishEvent: () => assert.fail('NATS must be forwarded after commit'),
  };
  const publisher = new EventPublisher(pgboss, nats);
  publisher.registerEvent({
    eventName: EVENT_NAME,
    eventSubject: 'chat.1.message.created',
    transports: ['local', 'nats'],
  });
  return { publisher, pgboss, database, committed, sessions };
};

test('event transactions - commit local jobs and outbox together', async () => {
  const { publisher, committed, sessions } = createPublisher();

  const id = await publisher.emit(EVENT_NAME, { messageId: 42 });

  assert.deepStrictEqual(
    committed.map(({ name }) => name),
    [...SUBSCRIBERS, NATS_EVENT_QUEUE],
  );
  assert.strictEqual(committed[0].data.id, id);
  assert.strictEqual(committed[1].data.id, id);
  assert.strictEqual(committed[2].data.event.id, id);
  assert.strictEqual(sessions.length, 1);
  assert.strictEqual(sessions[0].queries[0], 'BEGIN');
  assert.strictEqual(sessions[0].queries.at(-1), 'COMMIT');
  assert.strictEqual(sessions[0].released, true);
});

for (const failedQueue of [SUBSCRIBERS[1], NATS_EVENT_QUEUE]) {
  test(`event transactions - roll back failure in ${failedQueue}`, async () => {
    const failure = new Error('Insert failed');
    let failing = true;
    const { publisher, committed, sessions } = createPublisher({
      beforeQuery: async (text, values) => {
        if (!text.includes('INSERT INTO pgboss.job')) return;
        const [job] = JSON.parse(values[0]);
        if (failing && job.name === failedQueue) throw failure;
      },
    });

    await assert.rejects(
      publisher.emit(EVENT_NAME, {}),
      (error) =>
        error === failure ||
        (error instanceof AggregateError &&
          error.errors.includes(`${failedQueue}: Insert failed`)),
    );

    assert.deepStrictEqual(committed, []);
    assert.strictEqual(sessions[0].queries.at(-1), 'ROLLBACK');
    assert.strictEqual(sessions[0].released, true);

    failing = false;
    await publisher.emit(EVENT_NAME, {});
    assert.strictEqual(committed.length, 3);
    assert.strictEqual(sessions[1].queries.at(-1), 'COMMIT');
    assert.strictEqual(sessions[1].released, true);
  });
}

test('event transactions - wait for commit before returning', async () => {
  const entered = Promise.withResolvers();
  const commit = Promise.withResolvers();
  const { publisher, committed } = createPublisher({
    beforeQuery: async (text) => {
      if (text !== 'COMMIT') return;
      entered.resolve();
      await commit.promise;
    },
  });
  let returned = false;
  const publishing = publisher.emit(EVENT_NAME, {}).then(() => {
    returned = true;
  });

  await entered.promise;
  try {
    assert.strictEqual(returned, false);
    assert.deepStrictEqual(committed, []);
  } finally {
    commit.resolve();
    await publishing;
  }
  assert.strictEqual(returned, true);
  assert.strictEqual(committed.length, 3);
});

test('event transactions - leave commit and rollback to caller', async () => {
  const { publisher, database, pgboss, committed, sessions } =
    createPublisher();
  pgboss.withTransaction = () => assert.fail('Nested transaction');
  const failure = new Error('Business operation failed');

  await assert.rejects(
    database.withTransaction(async (db) => {
      const transaction = {
        query: (text, values) => db.executeSql(text, values),
      };
      await publisher.emit(EVENT_NAME, {}, { transaction });
      assert.deepStrictEqual(committed, []);
      assert.strictEqual(sessions[0].pending.length, 3);
      assert.strictEqual(sessions[0].queries.includes('COMMIT'), false);
      throw failure;
    }),
    (error) => error === failure,
  );

  assert.deepStrictEqual(committed, []);
  assert.strictEqual(sessions.length, 1);
  assert.strictEqual(sessions[0].queries.at(-1), 'ROLLBACK');
  assert.strictEqual(sessions[0].released, true);
});

test('event transactions - isolate concurrent publications', async () => {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const { publisher, committed, sessions } = createPublisher({
    beforeQuery: async (text, values) => {
      if (!text.includes('INSERT INTO pgboss.job')) return;
      const [job] = JSON.parse(values[0]);
      if (job.name !== NATS_EVENT_QUEUE || !job.data.event.data.fail) return;
      entered.resolve();
      await release.promise;
      throw new Error('First transaction failed');
    },
  });
  const failed = assert.rejects(
    publisher.emit(EVENT_NAME, { fail: true }),
    /First transaction failed/,
  );
  await entered.promise;
  let id;
  try {
    id = await publisher.emit(EVENT_NAME, { messageId: 42 });
  } finally {
    release.resolve();
    await failed;
  }

  assert.strictEqual(committed.length, 3);
  assert.strictEqual(committed[0].data.id, id);
  assert.strictEqual(committed[1].data.id, id);
  assert.strictEqual(committed[2].data.event.id, id);
  assert.strictEqual(sessions[0].queries.at(-1), 'ROLLBACK');
  assert.strictEqual(sessions[1].queries.at(-1), 'COMMIT');
  assert.strictEqual(
    sessions.every((session) => session.released),
    true,
  );
});

test('event transactions - require support from custom adapters', async () => {
  const { publisher, database, committed } = createPublisher();
  database.withTransaction = undefined;

  await assert.rejects(
    publisher.emit(EVENT_NAME, {}),
    /pgboss database requires a supplied transaction/,
  );
  assert.deepStrictEqual(committed, []);
});
