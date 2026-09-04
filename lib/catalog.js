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

class ServiceCatalog {
  constructor(threads, { ports = [], workers = {} } = {}) {
    this.threads = threads;
    this.loaderKind = 'balancer';
    if (workers.pool > 0) this.loaderKind = 'worker';
    if (ports.length > 0) this.loaderKind = 'server';
    this.loaderId = null;
    this.snapshot = null;
    this.index = null;
  }

  register(id, kind) {
    if (this.loaderId === null && kind === this.loaderKind) {
      this.loaderId = id;
    }
    return this.loaderId === id;
  }

  send(thread) {
    if (!this.snapshot) return;
    thread.postMessage({ name: 'catalog', snapshot: this.snapshot });
  }

  publish(thread, services) {
    if (this.threads.get(this.loaderId) !== thread) {
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

module.exports = { ServiceCatalog };
