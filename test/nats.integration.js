'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const metavm = require('metavm');
const { DomainError } = require('metautil');
const { Broker } = require('../lib/broker.js');
const { Nats } = require('../lib/nats.js');
const { Service } = require('../lib/service.js');

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
      service: {
        servers,
        credentials,
        discovery: { maxWait: 1000 },
      },
      server: { timeouts: { request: 2000 } },
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

const configureEvents = (application, source = true) => {
  if (source) {
    const { eventBroker } = application.service.prepareUnit('integration.1');
    eventBroker.load({
      completed: {
        parameters: { value: 'number' },
      },
    });
  }
  application.service.prepareUnit('audit.1');
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
    configureEvents(provider);
    configureEvents(consumer, false);

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
    const { integration, audit } = consumer.sandbox.service;
    const result = await consumer.contextStorage.run(context, () =>
      integration.echo({ value: 42 }),
    );

    assert.deepStrictEqual(result, { value: 42, session });
    await assert.rejects(
      consumer.contextStorage.run(context, () => integration.fail()),
      { code: 'E_INTEGRATION' },
    );

    const received = new Promise((resolve) => {
      audit.on('integration:completed', resolve);
    });
    await consumer.nats.connection.flush();
    await provider.sandbox.service.integration.emit('completed', {
      value: 42,
    });

    assert.deepStrictEqual(await received, { value: 42 });
  },
);
