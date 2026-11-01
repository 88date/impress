'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { randomBytes } = require('node:crypto');
const { AsyncLocalStorage } = require('node:async_hooks');
const metavm = require('metavm');
const { DomainError } = require('metautil');
const { Broker } = require('../lib/broker.js');
const { Nats } = require('../lib/nats.js');
const { Service } = require('../lib/service.js');
const {
  EventPublisher,
  SubscriptionManager,
  JetStreamSubscriptions,
} = require('../lib/events/index.js');

const servers = process.env.NATS_TEST_SERVERS;
const credentials = process.env.NATS_TEST_CREDENTIALS;
const skip = !servers || !credentials;

const createApplication = (kind) => {
  const application = {
    kind,
    sandbox: metavm.createContext({ service: {} }),
    console,
    contextStorage: new AsyncLocalStorage(),
    config: {
      server: {
        nats: {
          servers: servers.split(',').map((server) => server.trim()),
          credentials,
          discovery: { maxWait: 1000 },
        },
        timeouts: { request: 2000 },
      },
    },
    nats: null,
    schemas: null,
    absolute: (name) => name,
  };
  application.service = new Service('service', application);
  return application;
};

const configureAction = (application, name, method) => {
  const script = (context) => ({
    transports: ['nats'],
    access: 'logged',
    timeout: 2000,
    method: (args) => method(context, args),
  });
  const broker = new Broker(script, 'method', 'integration.1', application);
  application.service.changeUnit('integration.1', name, broker);
};

test(
  'lib/nats integration - preserves externally configured consumers',
  { skip, timeout: 10000 },
  async (t) => {
    const application = createApplication('server');
    const suffix = randomBytes(8).toString('hex');
    const stream = `CONSUMER_TEST_${suffix}`;
    const subject = `impress_test.${suffix}.events`;
    const nats = new Nats(application);
    let manager = null;
    let streamCreated = false;
    t.after(async () => {
      try {
        if (streamCreated) await manager.streams.delete(stream);
      } finally {
        await nats.close();
      }
    });
    await nats.connect();
    const jetstream = await nats.getJetStream();
    manager = jetstream.manager;
    await assert.rejects(manager.streams.info(stream), { code: 10059 });
    await manager.streams.add({ name: stream, subjects: [subject] });
    streamCreated = true;
    const transport = new JetStreamSubscriptions(nats, console, true);
    const binding = transport.createBinding({
      subscriberName: 'audit:1:message:completed',
      stream,
      subject,
    });
    await manager.consumers.add(stream, {
      ...binding.options,
      deliver_policy: 'all',
      replay_policy: 'original',
      ack_wait: 5_000_000_000,
      max_deliver: -1,
      max_ack_pending: 1,
      backoff: [5_000_000_000, 10_000_000_000],
    });
    await jetstream.client.publish(subject, Buffer.from('{}'));
    const original = await manager.consumers.info(stream, binding.consumerName);
    for (const managesTopology of [true, false, true]) {
      const connecting = new JetStreamSubscriptions(
        nats,
        console,
        managesTopology,
      );
      assert.strictEqual(
        await connecting.ensureConsumer(manager, binding),
        true,
      );
      const current = await manager.consumers.info(
        stream,
        binding.consumerName,
      );
      assert.deepStrictEqual(current.config, original.config);
      assert.strictEqual(current.created, original.created);
      assert.deepStrictEqual(current.delivered, original.delivered);
      assert.deepStrictEqual(current.ack_floor, original.ack_floor);
      assert.strictEqual(current.num_pending, 1);
    }
  },
);

test(
  'lib/nats integration - routes subscribers by stream and subject',
  { skip, timeout: 10000 },
  async (t) => {
    const suffix = randomBytes(8).toString('hex');
    const application = createApplication('server');
    const nats = new Nats(application);
    const subscriptions = new SubscriptionManager(
      new EventPublisher(),
      new JetStreamSubscriptions(nats, console, true),
    );
    const routes = ['EVENTS', 'CDC'].map((name) => ({
      stream: `${name}_TEST_${suffix}`,
      subject: `impress_test.${suffix}.${name.toLowerCase()}`,
      received: Promise.withResolvers(),
    }));
    const created = [];
    let manager = null;
    t.after(async () => {
      try {
        await subscriptions.stop();
      } finally {
        try {
          for (const stream of created) await manager.streams.delete(stream);
        } finally {
          await nats.close();
        }
      }
    });
    await nats.connect();
    const jetstream = await nats.getJetStream();
    manager = jetstream.manager;
    for (const route of routes) {
      await assert.rejects(manager.streams.info(route.stream), { code: 10059 });
      const subject = `${route.subject}.*`;
      await manager.streams.add({ name: route.stream, subjects: [subject] });
      created.push(route.stream);
      await subscriptions.registerSubscriber({
        subscriberName: `${route.stream}:1:message:created`,
        stream: route.stream,
        subject,
        method: (message, context) => {
          route.received.resolve({ message, context });
        },
      });
    }
    await subscriptions.start();
    for (const route of routes) {
      const messages = [
        {
          before: null,
          after: { id: 42 },
          op: 'c',
          source: { table: 'users', lsn: 100 },
        },
        null,
      ];
      for (const message of messages) {
        route.received = Promise.withResolvers();
        const ack = await jetstream.client.publish(
          `${route.subject}.created`,
          Buffer.from(JSON.stringify(message)),
        );
        assert.strictEqual(ack.stream, route.stream);
        const received = await route.received.promise;
        assert.deepStrictEqual(received.message, message);
        assert.deepStrictEqual(Object.keys(received.context), ['signal']);
        assert.strictEqual(received.context.signal.aborted, false);
      }
    }
    assert.strictEqual(subscriptions.active.size, 2);
  },
);

test(
  'lib/nats integration - should call actions',
  { skip, timeout: 10000 },
  async (testContext) => {
    const provider = createApplication('server');
    const consumer = createApplication('worker');
    const echo = (context, args) => ({
      value: args.value,
      session: context.session,
    });
    const fail = () => {
      throw new DomainError('E_INTEGRATION');
    };
    configureAction(provider, 'echo', echo);
    configureAction(provider, 'fail', fail);

    provider.nats = new Nats(provider);
    consumer.nats = new Nats(consumer);
    testContext.after(async () => {
      await consumer.nats.close();
      await provider.nats.close();
    });
    await provider.nats.start();
    await consumer.nats.start();

    assert.strictEqual(provider.nats.serviceSubscriptions.size, 2);
    assert.strictEqual(consumer.nats.serviceSubscriptions.size, 0);
    assert.strictEqual(consumer.nats.discoveryCatalogSubscription, null);

    const session = {
      token: 'integration-token',
      state: { userId: 'user-1' },
    };
    const context = { session };
    const { integration } = consumer.sandbox.service;
    const result = await consumer.contextStorage.run(context, () =>
      integration.echo({ value: 42 }),
    );

    assert.deepStrictEqual(result, { value: 42, session });
    await assert.rejects(
      consumer.contextStorage.run(context, () => integration.fail()),
      { code: 'E_INTEGRATION' },
    );
  },
);

test(
  'lib/nats integration - JetStream retries and resumes durable events',
  { skip, timeout: 15000 },
  async (t) => {
    const application = createApplication('server');
    const suffix = randomBytes(8).toString('hex');
    const stream = `EVENTS_TEST_${suffix}`;
    const eventName = `integration${suffix}:1:message:completed`;
    const subject = `impress_test.${suffix}.events.message.completed`;
    const nats = new Nats(application);
    application.nats = nats;
    let subscriptions = null;
    let manager = null;
    let streamCreated = false;
    t.after(async () => {
      try {
        await subscriptions?.stop();
      } finally {
        try {
          if (streamCreated) await manager.streams.delete(stream);
        } finally {
          await nats.close();
        }
      }
    });
    await nats.connect();
    const jetstream = await nats.getJetStream();
    const { client } = jetstream;
    manager = jetstream.manager;
    await assert.rejects(manager.streams.info(stream), { code: 10059 });
    await manager.streams.add({ name: stream, subjects: [subject] });
    streamCreated = true;

    const publisher = new EventPublisher();
    const records = [];
    publisher.configure({
      database: {
        query: async (text, values) => {
          records.push({
            id: values[0],
            name: values[1],
            subject: values[2],
            createdAt: values[3],
            payload: JSON.parse(values[4]),
          });
        },
      },
    });
    subscriptions = new SubscriptionManager(
      publisher,
      new JetStreamSubscriptions(nats, console, true),
    );
    let received = Promise.withResolvers();
    let attempts = 0;
    await subscriptions.registerEvent({
      eventName,
      eventSubject: subject,
    });
    await subscriptions.registerSubscriber({
      subscriberName: 'audit:1:message:completed',
      stream,
      subject,
      retryDelay: 10,
      method: (message, context) => {
        if (++attempts === 1) throw new Error('Retry this event');
        received.resolve({ message, context });
      },
    });
    await subscriptions.start();

    // Emulate CDC forwarding; the database path has its own integration test.
    const deliver = async (payload) => {
      const id = await publisher.emit(eventName, payload);
      const record = records.at(-1);
      await client.publish(subject, Buffer.from(JSON.stringify(record)));
      return id;
    };
    const firstId = await deliver({ value: 42 });
    const first = await received.promise;
    assert.deepStrictEqual(first.message, records.at(-1));
    assert.deepStrictEqual(first.message.payload, { value: 42 });
    assert.strictEqual(first.message.id, firstId);
    assert.deepStrictEqual(Object.keys(first.context), ['signal']);
    assert.strictEqual(first.context.signal.aborted, false);
    assert.strictEqual(attempts, 2);
    await subscriptions.stop();
    await nats.connection.flush();

    received = Promise.withResolvers();
    const secondId = await deliver(null);
    await subscriptions.start();
    const second = await received.promise;
    assert.deepStrictEqual(second.message, records.at(-1));
    assert.strictEqual(second.message.id, secondId);
    assert.strictEqual(second.message.payload, null);
  },
);
