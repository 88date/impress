'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Error } = metarhia.metautil;
const { connect, credsAuthenticator } = npm.nats;

const DISCOVERY_SUBJECT = 'service.discovery';
const DISCOVERY_CHANGED_SUBJECT = 'service.discovery.changed';

class Nats {
  constructor(application) {
    this.application = application;
    this.connection = null;
    this.serviceSubscriptions = new Map();
    this.eventSubscriptions = new Map();
    this.discoverySubscriptions = new Map();
    this.discoveryRequests = new Map();
    this.discoveryChangeSubscription = null;
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
    this.watchStatus();
    this.subscribeServices();
    this.subscribeDiscovery();
    this.subscribeDiscoveryChanges();
    this.subscribeEvents();
    await this.connection.flush();
    this.announceServices();
    await this.discoverServices();
    await this.connection.flush();
  }

  async watchStatus() {
    const connection = this.connection;
    try {
      for await (const status of connection.status()) {
        if (status.type !== 'reconnect') continue;
        this.announceServices();
        try {
          await this.discoverServices();
        } catch (error) {
          this.application.console.error(error);
        }
      }
    } catch (error) {
      if (this.connection === connection) {
        this.application.console.error(error);
      }
    }
  }

  async request(subject, args, timeout) {
    const sourceContext = this.application.contextStorage.getStore();
    const context = {
      session: sourceContext?.session ?? null,
      ip: sourceContext?.client?.ip ?? sourceContext?.ip ?? null,
    };
    const payload = JSON.stringify({ context, args });
    const options = { timeout };
    const message = await this.connection.request(subject, payload, options);
    const { result, error } = message.json();

    if (error) {
      if (error.type === 'internal') {
        throw new Error('Service request failed');
      }
      throw new Error(error.message, { code: error.code });
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
        if (error instanceof Error) {
          response = {
            error: {
              code: error.code,
              message: error.message,
            },
          };
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
    const { collection } = this.application.service;
    const subjects = new Set();
    for (const unit of Object.values(collection)) {
      for (const version of Object.keys(unit)) {
        if (version === 'default') continue;
        const methods = unit[version];
        for (const broker of Object.values(methods)) {
          if (broker.discovered) continue;
          const config = broker.config;
          if (!config || config.location !== 'local') continue;
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
    if (broker.discovered) return;
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

  subscribeDiscovery() {
    const { collection } = this.application.service;
    const names = new Set();
    for (const name of Object.keys(collection)) {
      const config = this.application.service.getConfig(name);
      if (!config || config.location !== 'local') continue;
      names.add(name);
      if (this.discoverySubscriptions.has(name)) continue;
      const subject = `${DISCOVERY_SUBJECT}.${name}`;
      const callback = (error, message) => {
        if (error) {
          this.application.console.error(error);
          return;
        }
        try {
          const metadata = this.application.service.describe(name);
          message.respond(JSON.stringify(metadata));
        } catch (error) {
          this.application.console.error(error);
        }
      };
      const subscription = this.connection.subscribe(subject, { callback });
      this.discoverySubscriptions.set(name, subscription);
    }
    for (const name of this.discoverySubscriptions.keys()) {
      if (names.has(name)) continue;
      const subscription = this.discoverySubscriptions.get(name);
      subscription.unsubscribe();
      this.discoverySubscriptions.delete(name);
    }
  }

  subscribeDiscoveryChanges() {
    if (this.discoveryChangeSubscription) return;
    const subject = `${DISCOVERY_CHANGED_SUBJECT}.*`;
    const callback = async (error, message) => {
      if (error) {
        this.application.console.error(error);
        return;
      }
      const prefixLength = DISCOVERY_CHANGED_SUBJECT.length + 1;
      const name = message.subject.substring(prefixLength);
      const config = this.application.service.getConfig(name);
      if (!config || config.location !== 'remote') return;
      try {
        await this.discoverService(name, config.discovery?.maxWait);
      } catch (error) {
        this.application.console.error(error);
      }
    };
    this.discoveryChangeSubscription = this.connection.subscribe(subject, {
      callback,
    });
  }

  announceServices() {
    for (const name of this.discoverySubscriptions.keys()) {
      this.publishDiscoveryChange(name);
    }
  }

  updateDiscovery(name) {
    this.subscribeDiscovery();
    this.publishDiscoveryChange(name);
  }

  publishDiscoveryChange(name) {
    const subject = `${DISCOVERY_CHANGED_SUBJECT}.${name}`;
    this.connection.publish(subject);
  }

  async discoverServices() {
    const { collection } = this.application.service;
    const requests = [];
    for (const name of Object.keys(collection)) {
      const config = this.application.service.getConfig(name);
      if (!config || config.location !== 'remote') continue;
      requests.push(this.discoverService(name, config.discovery?.maxWait));
    }
    await Promise.all(requests);
  }

  discoverService(name, maxWait) {
    const current = this.discoveryRequests.get(name);
    if (current) return current;
    const request = this.requestDiscovery(name, maxWait);
    this.discoveryRequests.set(name, request);
    const clear = () => {
      if (this.discoveryRequests.get(name) === request) {
        this.discoveryRequests.delete(name);
      }
    };
    request.then(clear, clear);
    return request;
  }

  async requestDiscovery(name, maxWait) {
    const subject = `${DISCOVERY_SUBJECT}.${name}`;
    let responses = 0;
    const actions = new Map();
    const events = new Map();
    try {
      const messages = await this.connection.requestMany(subject, undefined, {
        strategy: 'timer',
        maxWait,
      });
      for await (const message of messages) {
        responses++;
        const metadata = message.json();
        for (const action of metadata.actions) {
          const key = `${action.version}.${action.name}`;
          actions.set(key, action);
        }
        for (const event of metadata.events || []) {
          events.set(event.name, event);
        }
      }
    } catch (error) {
      throw new Error(`Service discovery failed: ${name}`, { cause: error });
    }
    if (responses === 0) {
      throw new Error(`Service discovery failed: ${name}`);
    }
    this.application.service.loadRemote(
      name,
      Array.from(actions.values()),
      Array.from(events.values()),
    );
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
        await currentBroker.dispatch(eventName, payload);
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
    this.discoverySubscriptions.clear();
    this.discoveryRequests.clear();
    this.discoveryChangeSubscription = null;
  }
}

module.exports = { Nats };
