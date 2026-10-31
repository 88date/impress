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
    const subject = `events.integration${suffix}.1.message.completed`;
    application.config.server.nats.jetstream = { stream };
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
    await nats.start();
    const jetstream = await nats.getJetStream();
    const { client } = jetstream;
    manager = jetstream.manager;
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
      eventName,
      retryDelay: 10,
      method: (data, event) => {
        if (++attempts === 1) throw new Error('Retry this event');
        received.resolve({ data, event });
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
    assert.deepStrictEqual(first.data, { value: 42 });
    assert.strictEqual(first.event.id, firstId);
    assert.strictEqual(attempts, 2);
    await subscriptions.stop();
    await nats.connection.flush();

    received = Promise.withResolvers();
    const secondId = await deliver(null);
    await subscriptions.start();
    const second = await received.promise;
    assert.strictEqual(second.event.id, secondId);
    assert.strictEqual(second.data, null);
  },
);
