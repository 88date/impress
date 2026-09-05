'use strict';

const { isDeepStrictEqual } = require('node:util');

const indexServices = (services) => {
  const index = new Map();
  for (const { name, actions } of services) {
    const methods = new Map();
    for (const action of actions) {
      methods.set(`${action.version}.${action.name}`, action);
    }
    index.set(name, methods);
  }
  return index;
};

const indexEvents = (events) => {
  const index = new Map();
  for (const event of events) index.set(event.name, event);
  return index;
};

class DiscoveryWorker {
  constructor(threads, { ports = [], workers = {} } = {}) {
    this.threads = threads;
    this.kind = 'balancer';
    if (workers.pool > 0) this.kind = 'worker';
    if (ports.length > 0) this.kind = 'server';
    this.id = null;
  }

  register(id, kind) {
    if (this.id === null && kind === this.kind) this.id = id;
    return this.id === id;
  }

  isLoader(thread) {
    return this.id !== null && this.threads.get(this.id) === thread;
  }
}

class ServiceCatalog {
  constructor(threads, discoveryWorker) {
    this.threads = threads;
    this.discoveryWorker = discoveryWorker;
    this.snapshot = null;
    this.index = null;
  }

  send(thread) {
    if (!this.snapshot) return;
    thread.postMessage({ name: 'catalog', snapshot: this.snapshot });
  }

  publish(thread, services) {
    if (!this.discoveryWorker.isLoader(thread)) {
      throw new Error('Only the discovery worker can publish the catalog');
    }
    const index = indexServices(services);
    if (this.snapshot && isDeepStrictEqual(this.index, index)) {
      return this.snapshot;
    }
    const revision = (this.snapshot?.revision ?? 0) + 1;
    this.index = index;
    this.snapshot = { revision, services };
    for (const worker of this.threads.values()) this.send(worker);
    return this.snapshot;
  }
}

class EventCatalog {
  constructor(threads, discoveryWorker) {
    this.threads = threads;
    this.discoveryWorker = discoveryWorker;
    this.snapshot = null;
    this.index = null;
  }

  send(thread) {
    if (!this.snapshot) return false;
    try {
      thread.postMessage({ name: 'eventCatalog', snapshot: this.snapshot });
      return true;
    } catch {
      return false;
    }
  }

  publish(thread, events) {
    if (!this.discoveryWorker.isLoader(thread)) {
      throw new Error(
        'Only the discovery worker can publish the event catalog',
      );
    }
    const index = indexEvents(events);
    if (this.snapshot && isDeepStrictEqual(this.index, index)) {
      return this.snapshot;
    }
    const revision = (this.snapshot?.revision ?? 0) + 1;
    this.index = index;
    this.snapshot = { revision, events };
    for (const worker of this.threads.values()) this.send(worker);
    return this.snapshot;
  }
}

module.exports = { DiscoveryWorker, ServiceCatalog, EventCatalog };
