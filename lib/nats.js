'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Error } = metarhia.metautil;
const { connect, credsAuthenticator, NoRespondersError } = npm.nats;

const DISCOVERY_SUBJECT = 'service.discovery';
const DISCOVERY_CHANGED_SUBJECT = 'service.discovery.changed';

class Nats {
  constructor(application, discovery = null) {
    this.application = application;
    this.discovery = discovery;
    this.catalog = null;
    this.catalogRevision = 0;
    this.catalogReady = null;
    this.discoveryPromise = null;
    this.discoveryPending = false;
    this.isStopping = false;
    this.connection = null;
    this.serviceSubscriptions = new Map();
    this.eventSubscriptions = new Map();
    this.discoverySubscriptions = new Map();
    this.discoveryCatalogSubscription = null;
    this.discoveryChangeSubscription = null;
  }

  get isServer() {
    return this.application.kind === 'server';
  }

  get isDiscoveryWorker() {
    return this.discovery?.loader ?? true;
  }

  async connect() {
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
  }

  async start() {
    await this.connect();
    if (this.isDiscoveryWorker) this.discovery?.request();
    this.watchStatus();
    this.subscribeServices();
    this.subscribeDiscovery();
    this.subscribeDiscoveryCatalog();
    this.subscribeDiscoveryChanges();
    await this.connection.flush();
    this.announceServices();
    const timeout = this.application.config.server?.timeouts?.start ?? 0;
    await this.discoverServices(timeout);
    await this.connection.flush();
  }

  async watchStatus() {
    const connection = this.connection;
    try {
      for await (const status of connection.status()) {
        if (this.isStopping) break;
        if (status.type !== 'reconnect') continue;
        this.announceServices();
        if (!this.isDiscoveryWorker) continue;
        this.discoveryPending = true;
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
    let message = null;
    try {
      message = await this.connection.request(subject, payload, options);
    } catch (error) {
      if (error instanceof NoRespondersError) {
        throw new Error('Not Found', { code: 404, cause: error });
      }
      throw error;
    }
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
    if (!this.isServer) return;
    const { collection } = this.application.service;
    const subjects = new Set();
    for (const unit of Object.values(collection)) {
      for (const version of Object.keys(unit)) {
        if (version === 'default') continue;
        const methods = unit[version];
        for (const broker of Object.values(methods)) {
          if (!broker.method) continue;
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
    if (!this.isServer || !broker.method) return;
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
    if (!this.isServer) return;
    const { collection } = this.application.service;
    const names = new Set();
    for (const name of Object.keys(collection)) {
      const metadata = this.application.service.describe(name);
      if (metadata.actions.length === 0) continue;
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
    if (!this.isDiscoveryWorker || this.discoveryChangeSubscription) return;
    const callback = async (error) => {
      if (this.isStopping) return;
      if (error) {
        this.application.console.error(error);
        return;
      }
      this.discoveryPending = true;
      try {
        await this.discoverServices();
      } catch (error) {
        this.application.console.error(error);
      }
    };
    this.discoveryChangeSubscription = this.connection.subscribe(
      DISCOVERY_CHANGED_SUBJECT,
      { callback },
    );
  }

  subscribeDiscoveryCatalog() {
    if (!this.isServer || this.discoveryCatalogSubscription) return;
    const callback = (error, message) => {
      if (error) {
        this.application.console.error(error);
        return;
      }
      try {
        const services = [];
        for (const name of this.discoverySubscriptions.keys()) {
          services.push(this.application.service.describe(name));
        }
        message.respond(JSON.stringify(services));
      } catch (error) {
        this.application.console.error(error);
      }
    };
    this.discoveryCatalogSubscription = this.connection.subscribe(
      DISCOVERY_SUBJECT,
      { callback },
    );
  }

  announceServices() {
    this.publishDiscoveryChange();
  }

  updateDiscovery() {
    this.subscribeDiscovery();
    this.publishDiscoveryChange();
  }

  publishDiscoveryChange() {
    if (!this.isServer) return;
    this.connection.publish(DISCOVERY_CHANGED_SUBJECT);
  }

  applyCatalog({ revision, services }) {
    if (this.isStopping || revision <= this.catalogRevision) {
      return this.catalog;
    }
    const catalog = new Map();
    for (const { name, actions } of services) {
      this.application.service.loadRemote(name, actions);
      const methods = new Map();
      for (const action of actions) {
        methods.set(`${action.version}.${action.name}`, action);
      }
      catalog.set(name, methods);
    }
    this.catalog = catalog;
    this.catalogRevision = revision;
    this.catalogReady?.resolve(catalog);
    this.catalogReady = null;
    return catalog;
  }

  assertDiscoveryActive() {
    if (this.isStopping) throw new Error('Service discovery stopped');
  }

  async getCatalog(timeout = 0) {
    this.assertDiscoveryActive();
    if (this.catalog !== null) return this.catalog;
    if (!this.catalogReady) this.catalogReady = Promise.withResolvers();
    const { promise } = this.catalogReady;
    try {
      this.discovery?.request();
    } catch (error) {
      this.catalogReady.reject(error);
    }
    if (timeout > 0) return metarhia.metautil.timeoutify(promise, timeout);
    return promise;
  }

  async discoverServices(timeout = 0) {
    this.assertDiscoveryActive();
    if (!this.isDiscoveryWorker) return this.getCatalog(timeout);
    if (!this.discoveryPromise) {
      this.discoveryPromise = this.refreshCatalog(timeout);
    }
    return this.discoveryPromise;
  }

  async refreshCatalog(timeout) {
    try {
      do {
        this.discoveryPending = false;
        const services = await this.fetchServices(timeout);
        this.assertDiscoveryActive();
        const snapshot = this.discovery
          ? await this.discovery.publish(services)
          : { revision: this.catalogRevision + 1, services };
        this.assertDiscoveryActive();
        this.applyCatalog(snapshot);
      } while (this.discoveryPending);
      return this.catalog;
    } finally {
      this.discoveryPromise = null;
    }
  }

  async fetchServices(timeout = 0) {
    const { maxWait } = this.application.config.service.discovery;
    const deadline = Date.now() + timeout;
    const discovered = new Map();
    let retryDelayMs = 100;
    let responses = 0;
    while (responses === 0) {
      this.assertDiscoveryActive();
      try {
        const remaining = deadline - Date.now();
        const wait =
          timeout > 0 ? Math.min(maxWait, Math.max(1, remaining)) : maxWait;
        const options = { strategy: 'timer', maxWait: wait };
        const messages = await this.connection.requestMany(
          DISCOVERY_SUBJECT,
          undefined,
          options,
        );
        for await (const message of messages) {
          responses++;
          const services = message.json();
          for (const metadata of services) {
            let actions = discovered.get(metadata.name);
            if (!actions) {
              actions = new Map();
              discovered.set(metadata.name, actions);
            }
            for (const action of metadata.actions) {
              const key = `${action.version}.${action.name}`;
              actions.set(key, action);
            }
          }
        }
      } catch (error) {
        if (!(error instanceof NoRespondersError) || Date.now() >= deadline) {
          throw new Error('Service discovery failed', { cause: error });
        }
      }
      if (responses > 0) break;
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error('Service discovery failed');
      await metarhia.metautil.delay(Math.min(retryDelayMs, remaining));
      retryDelayMs = Math.min(retryDelayMs * 2, 1000);
    }

    const services = [];
    for (const [name, actions] of discovered) {
      services.push({ name, actions: Array.from(actions.values()) });
    }
    return services;
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
    this.isStopping = true;
    this.discoveryPending = false;
    this.catalogReady?.reject(new Error('Service discovery stopped'));
    this.catalogReady = null;
    if (!this.connection) return;
    await this.connection.drain();
    this.connection = null;
    this.serviceSubscriptions.clear();
    this.eventSubscriptions.clear();
    this.discoverySubscriptions.clear();
    this.discoveryCatalogSubscription = null;
    this.discoveryChangeSubscription = null;
    this.catalog = null;
    this.catalogRevision = 0;
  }
}

module.exports = { Nats };
