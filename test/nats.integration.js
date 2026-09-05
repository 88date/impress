'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const metavm = require('metavm');
const { DomainError } = require('metautil');
const { Broker } = require('../lib/broker.js');
const { Nats } = require('../lib/nats.js');
const { Service } = require('../lib/service.js');
const {
  EventPublisher,
  SubscriptionManager,
  PgbossSubscriptions,
  NatsSubscriptions,
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
          servers,
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
  'lib/nats integration - should call actions and deliver events',
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

    const publisher = new EventPublisher(null, provider.nats);
    const subscriptions = new SubscriptionManager(
      publisher,
      new PgbossSubscriptions(null),
      new NatsSubscriptions(provider.nats),
    );
    const received = Promise.withResolvers();
    await subscriptions.registerEvent({
      eventName: 'integration:1:completed',
      eventSubject: 'integration.1.completed',
      transports: ['nats'],
    });
    await subscriptions.registerSubscriber({
      subscriberName: 'audit:1:completed',
      eventName: 'integration:1:completed',
      method: (data, event) => received.resolve({ data, event }),
    });
    await subscriptions.start();
    try {
      await provider.nats.connection.flush();
      const id = await publisher.emit('integration:1:completed', { value: 42 });
      const message = await received.promise;
      assert.deepStrictEqual(message.data, { value: 42 });
      assert.strictEqual(message.event.id, id);
    } finally {
      await subscriptions.stop();
    }
  },
);
