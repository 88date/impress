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

const createApplication = () => {
  const application = {
    sandbox: metavm.createContext({ service: {} }),
    console,
    contextStorage: new AsyncLocalStorage(),
    config: { service: { servers, credentials } },
    nats: null,
    schemas: null,
    absolute: (name) => name,
  };
  application.service = new Service('service', application);
  return application;
};

const configureAction = (application, location, name, method) => {
  application.service.configs['integration.1'] = {
    location,
    versions: { default: 1 },
    request: { timeout: 2000 },
    discovery: { maxWait: 1000 },
  };
  const script = (context) => ({
    access: 'logged',
    method: (args) => method(context, args),
  });
  const broker = new Broker(script, 'method', 'integration.1', application);
  application.service.changeUnit('integration.1', name, broker);
};

const configureRemote = (application) => {
  application.service.configs['integration.1'] = {
    location: 'remote',
    versions: { default: 1 },
    request: { timeout: 2000 },
    discovery: { maxWait: 1000 },
  };
  application.service.prepareUnit('integration.1');
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
    const provider = createApplication();
    const consumer = createApplication();
    const echo = (context, args) => ({
      value: args.value,
      session: context.session,
    });
    const fail = () => {
      throw new DomainError('E_INTEGRATION');
    };
    configureAction(provider, 'local', 'echo', echo);
    configureAction(provider, 'local', 'fail', fail);
    configureRemote(consumer);
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
