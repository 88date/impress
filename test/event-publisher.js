'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const {
  EventPublisher,
  NATS_EVENT_QUEUE,
} = require('../lib/events/publisher.js');

class PgbossClient {
  constructor() {
    this.calls = [];
    this.queues = new Map();
    this.handlers = new Map();
    this.lastWorkId = 0;
  }

  async getQueue(name) {
    this.calls.push(['getQueue', name]);
    return this.queues.get(name) || null;
  }

  async createQueue(name, options) {
    this.calls.push(['createQueue', name, options]);
    this.queues.set(name, { name, ...options });
  }

  async updateQueue(name, options) {
    this.calls.push(['updateQueue', name, options]);
    const queue = this.queues.get(name);
    this.queues.set(name, { ...queue, ...options });
  }

  async work(name, options, handler) {
    this.calls.push(['work', name, options]);
    this.handlers.set(name, handler);
    return `worker-${++this.lastWorkId}`;
  }

  async offWork(name, options) {
    this.calls.push(['offWork', name, options]);
    this.handlers.delete(name);
  }
}

const createPgboss = () => {
  const calls = [];
  const client = new PgbossClient();
  return {
    calls,
    client,
    enabled: true,
    config: { useListenNotify: true },
    publish: async (...args) => calls.push(['publish', ...args]),
    send: async (...args) => calls.push(['send', ...args]),
    withTransaction: (action) => action({ query: async () => ({ rows: [] }) }),
  };
};

const createNats = () => {
  const calls = [];
  return {
    calls,
    connection: {
      flush: async () => calls.push(['flush']),
    },
    publishEvent: async (...args) => calls.push(['publishEvent', ...args]),
  };
};

const registerMessageCreated = (events, transports) => {
  events.registerEvent({
    eventName: 'chat:1:message:created',
    eventSubject: 'chat.v1.message-created',
    transports,
  });
};

test('event-publisher - publish a local event in a transaction', async () => {
  const pgboss = createPgboss();
  const transaction = { query: async () => ({ rows: [] }) };
  const events = new EventPublisher(pgboss);
  registerMessageCreated(events, ['local']);

  const id = await events.emit(
    'chat:1:message:created',
    { messageId: 42 },
    { transaction },
  );

  assert.match(id, /^[0-9a-f-]{36}$/);
  assert.strictEqual(pgboss.calls.length, 1);
  const [operation, eventName, event, options] = pgboss.calls[0];
  assert.strictEqual(operation, 'publish');
  assert.strictEqual(eventName, 'chat:1:message:created');
  assert.deepStrictEqual(event, {
    id,
    name: 'chat:1:message:created',
    createdAt: event.createdAt,
    data: { messageId: 42 },
  });
  assert.strictEqual('subject' in event, false);
  assert.strictEqual(options.transaction, transaction);
});

test('event-publisher - enqueue a NATS event subject', async () => {
  const pgboss = createPgboss();
  const nats = createNats();
  const transaction = { query: async () => ({ rows: [] }) };
  const events = new EventPublisher(pgboss, nats);
  registerMessageCreated(events, ['local', 'nats']);

  const id = await events.emit(
    'chat:1:message:created',
    { messageId: 42 },
    { transaction },
  );

  assert.strictEqual(pgboss.calls.length, 2);
  assert.strictEqual(pgboss.calls[0][0], 'publish');
  const [operation, queueName, outbox, options] = pgboss.calls[1];
  assert.strictEqual(operation, 'send');
  assert.strictEqual(queueName, NATS_EVENT_QUEUE);
  assert.strictEqual(outbox.subject, 'chat.v1.message-created');
  assert.strictEqual(outbox.event.id, id);
  assert.strictEqual('subject' in outbox.event, false);
  assert.strictEqual(options.transaction, transaction);
  assert.deepStrictEqual(nats.calls, []);
});

test('event-publisher - exclude a local transport', async () => {
  const pgboss = createPgboss();
  const events = new EventPublisher(pgboss, createNats());
  registerMessageCreated(events, ['nats']);

  await events.emit('chat:1:message:created', { messageId: 42 });

  assert.strictEqual(pgboss.calls.length, 1);
  assert.strictEqual(pgboss.calls[0][0], 'send');
});

test('event-publisher - publish outbox entries through NATS', async () => {
  const pgboss = createPgboss();
  const nats = createNats();
  const events = new EventPublisher(pgboss, nats);

  await events.start();

  assert.deepStrictEqual(pgboss.client.calls, [
    ['getQueue', NATS_EVENT_QUEUE],
    ['createQueue', NATS_EVENT_QUEUE, { notify: true }],
    ['work', NATS_EVENT_QUEUE, {}],
  ]);
  const first = {
    id: 'event-1',
    name: 'chat:1:message:created',
    createdAt: '2026-09-04T00:00:00.000Z',
    data: { messageId: 42 },
  };
  const second = {
    id: 'event-2',
    name: 'chat:1:message:updated',
    createdAt: '2026-09-04T00:00:01.000Z',
    data: { messageId: 42 },
  };
  const handler = pgboss.client.handlers.get(NATS_EVENT_QUEUE);
  await handler([
    { data: { subject: 'chat.v1.message-created', event: first } },
    { data: { subject: 'chat.v1.message-updated', event: second } },
  ]);

  assert.deepStrictEqual(nats.calls, [
    ['publishEvent', 'chat.v1.message-created', first],
    ['publishEvent', 'chat.v1.message-updated', second],
    ['flush'],
  ]);
});

test('event-publisher - unregister its outbox worker', async () => {
  const pgboss = createPgboss();
  const events = new EventPublisher(pgboss, createNats());

  await events.start();
  await events.stop();

  assert.deepStrictEqual(pgboss.client.calls.at(-1), [
    'offWork',
    NATS_EVENT_QUEUE,
    { id: 'worker-1', wait: true },
  ]);
  assert.strictEqual(events.workId, null);
});

test('event-publisher - publish directly without pgboss', async () => {
  const nats = createNats();
  const events = new EventPublisher(null, nats);
  registerMessageCreated(events, ['nats']);

  const id = await events.emit('chat:1:message:created', {
    messageId: 42,
  });

  assert.deepStrictEqual(nats.calls, [
    [
      'publishEvent',
      'chat.v1.message-created',
      {
        id,
        name: 'chat:1:message:created',
        createdAt: nats.calls[0][2].createdAt,
        data: { messageId: 42 },
      },
    ],
    ['flush'],
  ]);
});

test('event-publisher - require pgboss for transactional NATS', async () => {
  const events = new EventPublisher(null, createNats());
  const transaction = { query: async () => ({ rows: [] }) };
  registerMessageCreated(events, ['nats']);

  await assert.rejects(
    events.emit('chat:1:message:created', {}, { transaction }),
    /requires pgboss/,
  );
});

test('event-publisher - reject unknown and removed events', async () => {
  const pgboss = createPgboss();
  const events = new EventPublisher(pgboss);
  const eventName = 'chat:1:message:created';
  const error = { message: `Unknown event: ${eventName}` };

  await assert.rejects(events.emit(eventName, {}), error);
  registerMessageCreated(events, ['local']);
  events.unregisterEvent(eventName);
  await assert.rejects(events.emit(eventName, {}), error);

  assert.deepStrictEqual(pgboss.calls, []);
});

const unavailableTransports = [
  { transports: [], missing: null, reason: 'has no delivery transports' },
  {
    transports: ['local'],
    missing: 'pgboss',
    reason: 'requires pg-boss for local delivery',
  },
  {
    transports: ['nats'],
    missing: 'nats',
    reason: 'requires NATS for delivery',
  },
  {
    transports: ['local', 'nats'],
    missing: 'pgboss',
    reason: 'requires pg-boss for local delivery',
  },
  {
    transports: ['local', 'nats'],
    missing: 'nats',
    reason: 'requires NATS for delivery',
  },
];

for (const { transports, missing, reason } of unavailableTransports) {
  const label = transports.join(' + ') || 'no transports';
  const name = `event-publisher - reject ${label}: ${reason}`;
  test(name, async () => {
    const pgboss = createPgboss();
    const nats = createNats();
    const events = new EventPublisher(pgboss, nats);
    const eventName = 'chat:1:message:created';
    registerMessageCreated(events, transports);
    if (missing === 'pgboss') pgboss.client = null;
    if (missing === 'nats') nats.connection = null;

    await assert.rejects(events.emit(eventName, {}), {
      message: `Event ${eventName} ${reason}`,
    });

    assert.deepStrictEqual(pgboss.calls, []);
    assert.deepStrictEqual(nats.calls, []);
  });
}

test('event-publisher - recover transports without reload', async () => {
  const pgboss = createPgboss();
  const nats = createNats();
  const client = pgboss.client;
  const connection = nats.connection;
  pgboss.client = null;
  nats.connection = null;
  const events = new EventPublisher(pgboss, nats);
  registerMessageCreated(events, ['local', 'nats']);

  await assert.rejects(
    events.emit('chat:1:message:created', {}),
    /requires pg-boss/,
  );
  pgboss.client = client;
  await assert.rejects(
    events.emit('chat:1:message:created', {}),
    /requires NATS/,
  );
  assert.deepStrictEqual(pgboss.calls, []);

  nats.connection = connection;
  const id = await events.emit('chat:1:message:created', { messageId: 42 });

  assert.strictEqual(pgboss.calls.length, 2);
  assert.strictEqual(pgboss.calls[0][0], 'publish');
  assert.strictEqual(pgboss.calls[0][2].id, id);
  assert.strictEqual(pgboss.calls[1][0], 'send');
  assert.strictEqual(pgboss.calls[1][2].event.id, id);
  assert.deepStrictEqual(nats.calls, []);
});
