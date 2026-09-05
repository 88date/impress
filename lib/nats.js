'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Error } = metarhia.metautil;
const { connect, credsAuthenticator, NoRespondersError } = npm.nats;

const DISCOVERY_SUBJECT = 'service.discovery';
const DISCOVERY_CHANGED_SUBJECT = 'service.discovery.changed';
const EVENT_DISCOVERY_SUBJECT = 'event.discovery';
const EVENT_DISCOVERY_CHANGED_SUBJECT = 'event.discovery.changed';
const EVENT_DISCOVERY_INTERVAL = 30_000;

class Nats {
  constructor(application, discovery = null, eventDiscovery = null) {
    this.application = application;
    this.discovery = discovery;
    this.eventDiscovery = eventDiscovery;
    this.catalog = null;
    this.catalogRevision = 0;
    this.catalogReady = null;
    this.discoveryPromise = null;
    this.discoveryPending = false;
    this.eventCatalog = null;
    this.eventCatalogRevision = 0;
    this.eventDiscoveryPromise = null;
    this.eventDiscoveryPending = false;
    this.eventDiscoveryTimer = null;
    this.isStopping = false;
    this.connection = null;
    this.serviceSubscriptions = new Map();
    this.discoverySubscriptions = new Map();
    this.discoveryCatalogSubscription = null;
    this.discoveryChangeSubscription = null;
    this.eventCatalogSubscription = null;
    this.eventDiscoveryChangeSubscription = null;
  }

  get isServer() {
    return this.application.kind === 'server';
  }

  get isDiscoveryWorker() {
    return this.discovery?.loader ?? true;
  }

  get isEventDiscoveryWorker() {
    return this.isServer && this.eventDiscovery?.loader === true;
  }

  async connect() {
    const config = this.application.config.server.nats;
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
    this.eventDiscovery?.request();
    this.watchStatus();
    this.subscribeServices();
    this.subscribeDiscovery();
    this.subscribeDiscoveryCatalog();
    this.subscribeDiscoveryChanges();
    this.subscribeEventCatalog();
    this.subscribeEventDiscoveryChanges();
    await this.connection.flush();
    this.announceServices();
    this.announceEvents();
    const timeout = this.application.config.server?.timeouts?.start ?? 0;
    const services = this.discoverServices(timeout);
    const events = this.discoverEvents(timeout).catch((error) => {
      this.application.console.error(error);
    });
    await Promise.all([services, events]);
    this.scheduleEventDiscovery();
    await this.connection.flush();
  }

  async watchStatus() {
    const connection = this.connection;
    try {
      for await (const status of connection.status()) {
        if (this.isStopping) break;
        if (status.type !== 'reconnect') continue;
        this.announceServices();
        this.announceEvents();
        if (this.isDiscoveryWorker) {
          this.discoveryPending = true;
          try {
            await this.discoverServices();
          } catch (error) {
            this.application.console.error(error);
          }
        }
        if (this.isEventDiscoveryWorker) {
          this.eventDiscoveryPending = true;
          try {
            await this.discoverEvents();
          } catch (error) {
            this.application.console.error(error);
          }
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
    if (!this.isServer || !this.connection) return;
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
    if (!this.isServer || !this.connection || !broker.method) return;
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
    if (!this.isServer || !this.connection) return;
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

  subscribeEventCatalog() {
    if (
      !this.eventDiscovery ||
      !this.isEventDiscoveryWorker ||
      this.eventCatalogSubscription
    ) {
      return;
    }
    const callback = (error, message) => {
      if (error) {
        this.application.console.error(error);
        return;
      }
      try {
        const events = this.application.subscriptions.describeEvents();
        message.respond(JSON.stringify(events));
      } catch (error) {
        this.application.console.error(error);
      }
    };
    this.eventCatalogSubscription = this.connection.subscribe(
      EVENT_DISCOVERY_SUBJECT,
      { callback },
    );
  }

  subscribeEventDiscoveryChanges() {
    if (!this.isEventDiscoveryWorker || this.eventDiscoveryChangeSubscription) {
      return;
    }
    const callback = async (error) => {
      if (this.isStopping) return;
      if (error) {
        this.application.console.error(error);
        return;
      }
      this.eventDiscoveryPending = true;
      try {
        await this.discoverEvents();
      } catch (error) {
        this.application.console.error(error);
      }
    };
    this.eventDiscoveryChangeSubscription = this.connection.subscribe(
      EVENT_DISCOVERY_CHANGED_SUBJECT,
      { callback },
    );
  }

  announceServices() {
    this.publishDiscoveryChange();
  }

  announceEvents() {
    this.publishEventDiscoveryChange();
  }

  updateDiscovery() {
    this.subscribeDiscovery();
    this.publishDiscoveryChange();
  }

  updateEventDiscovery() {
    this.publishEventDiscoveryChange();
  }

  scheduleEventDiscovery() {
    if (!this.isEventDiscoveryWorker || this.eventDiscoveryTimer) return;
    this.eventDiscoveryTimer = setInterval(() => {
      if (this.isStopping) return;
      this.discoverEvents().catch((error) => {
        if (!this.isStopping) this.application.console.error(error);
      });
    }, EVENT_DISCOVERY_INTERVAL);
    this.eventDiscoveryTimer.unref();
  }

  publishDiscoveryChange() {
    if (!this.isServer || !this.connection) return;
    this.connection.publish(DISCOVERY_CHANGED_SUBJECT);
  }

  publishEventDiscoveryChange() {
    if (!this.isEventDiscoveryWorker || !this.connection) return;
    this.connection.publish(EVENT_DISCOVERY_CHANGED_SUBJECT);
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

  applyEventCatalog({ revision, events }) {
    if (this.isStopping || revision <= this.eventCatalogRevision) {
      return this.eventCatalog;
    }
    const catalog = new Map();
    for (const event of events) catalog.set(event.name, event);
    this.eventCatalog = catalog;
    this.eventCatalogRevision = revision;
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

  async discoverEvents(timeout = 0) {
    this.assertDiscoveryActive();
    if (!this.eventDiscovery || !this.application.subscriptions) {
      return this.eventCatalog;
    }
    if (!this.isEventDiscoveryWorker) return this.eventCatalog;
    if (!this.eventDiscoveryPromise) {
      this.eventDiscoveryPromise = this.refreshEventCatalog(timeout);
    }
    return this.eventDiscoveryPromise;
  }

  async refreshEventCatalog(timeout) {
    try {
      do {
        this.eventDiscoveryPending = false;
        const events = await this.fetchEvents(timeout);
        this.assertDiscoveryActive();
        const snapshot = await this.eventDiscovery.publish(events);
        this.assertDiscoveryActive();
        this.applyEventCatalog(snapshot);
      } while (this.eventDiscoveryPending);
      return this.eventCatalog;
    } finally {
      this.eventDiscoveryPromise = null;
    }
  }

  async fetchServices(timeout = 0) {
    const responses = await this.fetchDiscovery(
      DISCOVERY_SUBJECT,
      timeout,
      'Service discovery failed',
    );
    const discovered = new Map();
    for (const services of responses) {
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

    const services = [];
    for (const [name, actions] of discovered) {
      services.push({ name, actions: Array.from(actions.values()) });
    }
    return services;
  }

  async fetchEvents(timeout = 0) {
    const responses = await this.fetchDiscovery(
      EVENT_DISCOVERY_SUBJECT,
      timeout,
      'Event discovery failed',
    );
    const events = new Map();
    const subjects = new Map();
    for (const discovered of responses) {
      for (const event of discovered) {
        const current = events.get(event.name);
        if (current && current.subject !== event.subject) {
          throw new Error(`Conflicting event contract: ${event.name}`);
        }
        const eventName = subjects.get(event.subject);
        if (eventName && eventName !== event.name) {
          throw new Error(`Conflicting event subject: ${event.subject}`);
        }
        if (!current || JSON.stringify(current) < JSON.stringify(event)) {
          events.set(event.name, event);
        }
        subjects.set(event.subject, event.name);
      }
    }
    return Array.from(events.values()).sort((first, second) =>
      first.name.localeCompare(second.name),
    );
  }

  async fetchDiscovery(subject, timeout, errorMessage) {
    const { maxWait } = this.application.config.server.nats.discovery;
    const deadline = Date.now() + timeout;
    let retryDelayMs = 100;
    while (true) {
      this.assertDiscoveryActive();
      try {
        const remaining = deadline - Date.now();
        const wait =
          timeout > 0 ? Math.min(maxWait, Math.max(1, remaining)) : maxWait;
        const options = { strategy: 'timer', maxWait: wait };
        const messages = await this.connection.requestMany(
          subject,
          undefined,
          options,
        );
        const responses = [];
        for await (const message of messages) {
          responses.push(message.json());
        }
        if (responses.length > 0) return responses;
      } catch (error) {
        if (!(error instanceof NoRespondersError) || Date.now() >= deadline) {
          throw new Error(errorMessage, { cause: error });
        }
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error(errorMessage);
      await metarhia.metautil.delay(Math.min(retryDelayMs, remaining));
      retryDelayMs = Math.min(retryDelayMs * 2, 1000);
    }
  }

  publishEvent(subject, payload) {
    const data = JSON.stringify(payload);
    this.connection.publish(subject, data);
  }

  async close() {
    this.isStopping = true;
    this.discoveryPending = false;
    this.eventDiscoveryPending = false;
    clearInterval(this.eventDiscoveryTimer);
    this.eventDiscoveryTimer = null;
    this.catalogReady?.reject(new Error('Service discovery stopped'));
    this.catalogReady = null;
    if (this.connection) await this.connection.drain();
    this.connection = null;
    this.serviceSubscriptions.clear();
    this.discoverySubscriptions.clear();
    this.discoveryCatalogSubscription = null;
    this.discoveryChangeSubscription = null;
    this.eventCatalogSubscription = null;
    this.eventDiscoveryChangeSubscription = null;
    this.catalog = null;
    this.catalogRevision = 0;
    this.eventCatalog = null;
    this.eventCatalogRevision = 0;
  }
}

module.exports = {
  Nats,
  EVENT_DISCOVERY_SUBJECT,
  EVENT_DISCOVERY_CHANGED_SUBJECT,
};
