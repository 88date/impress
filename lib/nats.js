'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Error, DomainError } = metarhia.metautil;
const { connect, credsAuthenticator } = npm.nats;

class Nats {
  constructor(application) {
    this.application = application;
    this.connection = null;
    this.serviceSubscriptions = new Map();
    this.eventSubscriptions = new Map();
  }

  async start() {
    const config = this.application.config.service;
    if (!config.servers || !config.credentials) {
      throw new Error('NATS servers and credentials expected');
    }
    const credentials = await node.fsp.readFile(config.credentials);
    const authenticator = credsAuthenticator(credentials);
    this.connection = await connect({
      servers: config.servers,
      authenticator,
      maxReconnectAttempts: -1,
    });
    this.subscribeServices();
    this.subscribeEvents();
    await this.connection.flush();
  }

  async request(subject, args, timeout) {
    const sourceContext = this.application.contextStorage.getStore();
    const context = { session: sourceContext?.session ?? null };
    const payload = JSON.stringify({ context, args });
    const options = { timeout };
    const message = await this.connection.request(subject, payload, options);
    const { result, error } = message.json();

    if (error) {
      const isDomainError = error.type === 'domain';
      if (isDomainError) throw new DomainError(error.code);
      throw new Error('Service request failed');
    }

    return result;
  }

  subscribe(subject, handler) {
    const callback = async (error, message) => {
      if (error) {
        this.application.console.error(error);
        return;
      }
      let response = null;
      try {
        const { context, args } = message.json();
        const invoke = () => handler(context, args);
        const result = await this.application.contextStorage.run(
          context,
          invoke,
        );
        response = { result };
      } catch (error) {
        if (error instanceof DomainError) {
          response = { error: { type: 'domain', code: error.code } };
        } else {
          this.application.console.error(error);
          response = { error: { type: 'internal' } };
        }
      }
      message.respond(JSON.stringify(response));
    };
    return this.connection.subscribe(subject, { queue: subject, callback });
  }

  subscribeServices() {
    const { collection, configs } = this.application.service;
    const subjects = new Set();
    for (const [name, unit] of Object.entries(collection)) {
      for (const version of Object.keys(unit)) {
        if (version === 'default') continue;
        const config = configs[`${name}.1`] || configs[`${name}.${version}`];
        if (!config || config.location !== 'local') continue;
        const methods = unit[version];
        for (const broker of Object.values(methods)) {
          subjects.add(broker.subject);
          this.subscribeService(broker);
        }
      }
    }

    for (const subject of this.serviceSubscriptions.keys()) {
      if (!subjects.has(subject)) this.unsubscribeService(subject);
    }
  }

  subscribeService(broker) {
    const config = broker.config;
    if (!config || config.location !== 'local') return;
    const { subject, serviceName, version, actionName } = broker;
    if (this.serviceSubscriptions.has(subject)) return;
    const handler = (context, args) => {
      const { collection } = this.application.service;
      const methods = collection[serviceName]?.[version.toString()];
      const currentBroker = methods?.[actionName];
      if (!currentBroker) {
        throw new Error(`Service action is not available: ${subject}`);
      }
      return currentBroker.invoke(context, args);
    };
    const subscription = this.subscribe(subject, handler);
    this.serviceSubscriptions.set(subject, subscription);
  }

  unsubscribeService(subject) {
    const subscription = this.serviceSubscriptions.get(subject);
    if (!subscription) return;
    subscription.unsubscribe();
    this.serviceSubscriptions.delete(subject);
  }

  publishEvent(eventName, payload) {
    const data = JSON.stringify(payload);
    const subject = eventName.replaceAll(':', '.');
    this.connection.publish(subject, data);
  }

  subscribeEvents() {
    const { events } = this.application.service;
    const keys = new Set();
    for (const broker of Object.values(events)) {
      for (const eventName of broker.eventNames()) {
        const key = `${broker.name}:${eventName}`;
        keys.add(key);
        this.subscribeEvent(broker, eventName);
      }
    }

    for (const key of this.eventSubscriptions.keys()) {
      if (!keys.has(key)) this.unsubscribeEvent(key);
    }
  }

  subscribeEvent(broker, eventName) {
    const key = `${broker.name}:${eventName}`;
    if (this.eventSubscriptions.has(key)) return;
    const subject = eventName.replaceAll(':', '.');
    const callback = async (error, message) => {
      if (error) {
        this.application.console.error(error);
        return;
      }
      try {
        const payload = message.json();
        const { events } = this.application.service;
        const currentBroker = events[broker.name];
        if (!currentBroker) {
          throw new Error(
            `Service event subscriber is not available: ${broker.name}`,
          );
        }
        await currentBroker.invoke(eventName, payload);
      } catch (error) {
        this.application.console.error(error);
      }
    };
    const options = { queue: broker.name, callback };
    const subscription = this.connection.subscribe(subject, options);
    this.eventSubscriptions.set(key, subscription);
  }

  unsubscribeEvent(key) {
    const subscription = this.eventSubscriptions.get(key);
    if (!subscription) return;
    subscription.unsubscribe();
    this.eventSubscriptions.delete(key);
  }

  async close() {
    if (!this.connection) return;
    await this.connection.drain();
    this.connection = null;
    this.serviceSubscriptions.clear();
    this.eventSubscriptions.clear();
  }
}

module.exports = { Nats };
