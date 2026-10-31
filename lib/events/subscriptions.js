'use strict';

const { isDeepStrictEqual } = require('node:util');

const RETRY_DELAY = 1000;

const createDispatch = (target) => (record, context) => {
  const { id, name, createdAt, payload } = record;
  return target.method(payload, { id, name, createdAt, ...context });
};

class SubscriptionManager {
  constructor(emitter, transport, logger = global.console) {
    this.emitter = emitter;
    this.transport = transport;
    this.logger = logger;
    this.events = Object.create(null);
    this.subscribers = Object.create(null);
    this.active = new Map();
    this.started = false;
    this.operation = Promise.resolve();
    this.retryTimer = null;
  }

  registerEvent(contract) {
    this.events[contract.eventName] = contract;
    this.emitter.registerEvent(contract);
    return this.synchronize();
  }

  unregisterEvent(eventName) {
    delete this.events[eventName];
    this.emitter.unregisterEvent(eventName);
    return this.synchronize();
  }

  registerSubscriber(contract) {
    this.subscribers[contract.subscriberName] = contract;
    return this.synchronize();
  }

  removeSubscriber(subscriberName) {
    delete this.subscribers[subscriberName];
    return this.synchronize();
  }

  describeEvents() {
    const events = Object.values(this.events).map((contract) => ({
      name: contract.eventName,
      subject: contract.eventSubject,
      caption: contract.caption ?? '',
      description: contract.description ?? '',
      examples: contract.examples ?? null,
      transports: ['nats'],
    }));
    return events.sort((first, second) =>
      first.name.localeCompare(second.name),
    );
  }

  enqueue(action) {
    const operation = this.operation.then(action);
    this.operation = operation.catch(() => {});
    return operation;
  }

  start() {
    if (this.started) return this.operation;
    this.started = true;
    return this.enqueue(async () => {
      if (!this.started) return;
      try {
        const isReady = await this.reconcile();
        if (!isReady) this.retry();
      } catch (error) {
        this.started = false;
        try {
          await this.stopActive();
        } catch (cause) {
          this.logger.error('Failed to stop event subscribers', cause);
        }
        throw error;
      }
    });
  }

  stop() {
    this.started = false;
    clearTimeout(this.retryTimer);
    this.retryTimer = null;
    return this.enqueue(() => this.stopActive());
  }

  async stopActive() {
    const failures = [];
    const pending = [...this.active].map(async ([name, subscription]) => {
      try {
        await subscription.handle.stop();
        this.active.delete(name);
      } catch (error) {
        failures.push(error);
      }
    });
    await Promise.all(pending);
    if (failures.length > 0) {
      throw new Error('Failed to stop event subscribers', {
        cause: failures[0],
      });
    }
  }

  synchronize() {
    if (!this.started) return Promise.resolve();
    return this.enqueue(async () => {
      if (!this.started) return;
      try {
        const isReady = await this.reconcile();
        if (!isReady) {
          this.retry();
          return;
        }
        clearTimeout(this.retryTimer);
        this.retryTimer = null;
      } catch (error) {
        this.logger.error('Failed to synchronize event subscribers', error);
        this.retry();
      }
    });
  }

  retry() {
    if (!this.started || this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.synchronize();
    }, RETRY_DELAY);
    this.retryTimer.unref();
  }

  async reconcile() {
    const errors = [];
    for (const [name, subscription] of this.active) {
      if (this.subscribers[name]) continue;
      try {
        await subscription.handle.stop();
        this.active.delete(name);
      } catch (error) {
        errors.push(error);
      }
    }
    const result = await this.reconcileSubscribers();
    errors.push(...result.errors);
    if (errors.length > 0) {
      for (const error of errors) this.logger.error(error);
      throw new Error('Failed to reconcile event subscribers', {
        cause: errors[0],
      });
    }
    return result.ready;
  }

  async reconcileSubscribers() {
    const { transport } = this;
    const result = { ready: true, errors: [] };
    if (!transport.enabled) return result;
    const contracts = Object.values(this.subscribers);
    if (contracts.length === 0 && !transport.managesTopology) return result;
    result.ready = await transport.ensureStream();
    if (!result.ready) return result;
    for (const contract of contracts) {
      try {
        const isReady = await this.reconcileSubscriber(contract);
        if (!isReady) result.ready = false;
      } catch (error) {
        result.errors.push(error);
      }
    }
    return result;
  }

  async reconcileSubscriber(contract) {
    const { subscriberName } = contract;
    const binding = this.transport.createBinding(contract);
    const current = this.active.get(subscriberName);
    if (
      current?.handle.running &&
      isDeepStrictEqual(current.binding, binding)
    ) {
      current.target.method = contract.method;
      return true;
    }
    if (current) {
      await current.handle.stop();
      this.active.delete(subscriberName);
    }
    if (!this.transport.available) return false;
    const target = { method: contract.method };
    const handle = await this.transport.bind(binding, createDispatch(target));
    if (!handle) return false;
    this.active.set(subscriberName, { binding, target, handle });
    return true;
  }
}

module.exports = { SubscriptionManager };
