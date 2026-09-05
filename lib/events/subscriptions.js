'use strict';

const { isDeepStrictEqual } = require('node:util');

const RETRY_DELAY = 1000;

const sameTarget = (first, second) => {
  if (first.transport !== second.transport) return false;
  if (first.transport === 'nats') {
    return (
      first.eventSubject === second.eventSubject &&
      first.queueGroup === second.queueGroup
    );
  }
  return (
    first.eventName === second.eventName && first.queueName === second.queueName
  );
};

const createDispatch = (target) => (envelope, context) =>
  Promise.resolve().then(() => {
    const { data, ...event } = envelope;
    const metadata = context ? { ...event, ...context } : event;
    return target.method(data, metadata);
  });

class SubscriptionManager {
  constructor(emitter, local, nats, logger = global.console) {
    this.emitter = emitter;
    this.local = local;
    this.nats = nats;
    this.logger = logger;
    this.events = Object.create(null);
    this.subscribers = Object.create(null);
    this.active = new Map();
    this.removedQueues = new Set();
    this.registeredEvents = new Map();
    this.started = false;
    this.operation = Promise.resolve();
    this.retryTimer = null;
  }

  registerEvent(contract) {
    this.events[contract.eventName] = contract;
    return this.synchronize();
  }

  unregisterEvent(eventName) {
    delete this.events[eventName];
    return this.synchronize();
  }

  registerSubscriber(contract) {
    this.subscribers[contract.subscriberName] = contract;
    return this.synchronize();
  }

  removeSubscriber(subscriberName) {
    const contract = this.subscribers[subscriberName];
    if (contract) {
      this.removedQueues.add(this.local.createBinding(contract).queueName);
    }
    delete this.subscribers[subscriberName];
    return this.synchronize();
  }

  describeEvents({ natsOnly = true } = {}) {
    const events = [];
    for (const contract of Object.values(this.events)) {
      if (natsOnly && !contract.transports.includes('nats')) continue;
      const {
        eventName: name,
        eventSubject: subject,
        caption = '',
        description = '',
        examples = null,
        transports,
      } = contract;
      events.push({
        name,
        subject,
        caption,
        description,
        examples,
        transports: [...transports],
      });
    }
    return events.sort((first, second) =>
      first.name.localeCompare(second.name),
    );
  }

  start() {
    if (this.started) return this.operation;
    this.started = true;
    return this.enqueue(async () => {
      if (!this.started) return;
      try {
        await this.reconcile();
        await this.removeStaleQueues();
        clearTimeout(this.retryTimer);
        this.retryTimer = null;
      } catch (error) {
        this.started = false;
        try {
          await this.stopActive();
        } catch (stopError) {
          const details = stopError?.stack || stopError?.message;
          this.logger.error('Failed to stop event subscribers', details);
        }
        throw error;
      }
    });
  }

  stop() {
    if (
      !this.started &&
      this.active.size === 0 &&
      this.registeredEvents.size === 0
    ) {
      return this.operation;
    }
    this.started = false;
    clearTimeout(this.retryTimer);
    this.retryTimer = null;
    return this.enqueue(() => this.stopActive());
  }

  async stopActive() {
    const active = [...this.active];
    const stopping = active.map(([, subscription]) =>
      subscription.handle.stop(),
    );
    const results = await Promise.allSettled(stopping);
    const errors = [];
    for (let index = 0; index < results.length; index++) {
      const result = results[index];
      if (result.status === 'fulfilled') this.active.delete(active[index][0]);
      else errors.push(result.reason);
    }
    for (const eventName of this.registeredEvents.keys()) {
      this.emitter.unregisterEvent(eventName);
    }
    this.registeredEvents.clear();
    if (errors.length > 0) {
      throw new Error('Failed to stop event subscribers', {
        cause: errors[0],
      });
    }
  }

  enqueue(action) {
    const operation = this.operation.then(action);
    this.operation = operation.catch(() => {});
    return operation;
  }

  synchronize() {
    if (!this.started) return Promise.resolve();
    return this.enqueue(async () => {
      if (!this.started) return;
      try {
        await this.reconcile();
        clearTimeout(this.retryTimer);
        this.retryTimer = null;
      } catch (error) {
        const details = error?.stack || error?.message || String(error);
        this.logger.error('Failed to synchronize event subscribers', details);
        this.retry();
      }
    });
  }

  retry() {
    if (this.retryTimer || !this.started) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.synchronize();
    }, RETRY_DELAY);
    this.retryTimer.unref();
  }

  async reconcile() {
    const names = new Set();
    const errors = [];
    for (const contract of Object.values(this.subscribers)) {
      names.add(contract.subscriberName);
      try {
        await this.reconcileSubscriber(contract);
      } catch (error) {
        errors.push(error);
      }
    }
    for (const [name, subscription] of this.active) {
      if (names.has(name)) continue;
      try {
        await subscription.handle.detach();
        this.active.delete(name);
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) {
      throw new Error('Failed to bind event subscribers', {
        cause: errors[0],
      });
    }
    await this.removeSubscriberQueues();
    this.synchronizeEvents();
  }

  getDeclaredQueues() {
    return new Set(
      Object.values(this.subscribers).map(
        (contract) => this.local.createBinding(contract).queueName,
      ),
    );
  }

  removeStaleQueues() {
    return this.local.removeStaleQueues(this.getDeclaredQueues());
  }

  async removeSubscriberQueues() {
    const declared = this.getDeclaredQueues();
    for (const queueName of this.removedQueues) {
      if (!declared.has(queueName)) {
        const removed = await this.local.removeQueue(queueName);
        if (!removed) continue;
      }
      this.removedQueues.delete(queueName);
    }
  }

  synchronizeEvents() {
    const names = new Set(Object.keys(this.events));
    for (const contract of Object.values(this.events)) {
      if (this.registeredEvents.get(contract.eventName) === contract) continue;
      this.emitter.registerEvent(contract);
      this.registeredEvents.set(contract.eventName, contract);
    }
    for (const eventName of this.registeredEvents.keys()) {
      if (names.has(eventName)) continue;
      this.emitter.unregisterEvent(eventName);
      this.registeredEvents.delete(eventName);
    }
  }

  createBinding(contract) {
    const event = this.events[contract.eventName];
    const local = event?.transports.includes('local');
    const nats = !event || event.transports.includes('nats');
    let transport = null;
    if (local && this.local.available) transport = this.local;
    else if (nats && this.nats.available) transport = this.nats;
    else if (local) transport = this.local;
    else if (nats) transport = this.nats;
    if (!transport) return null;
    const binding = transport.createBinding(contract);
    return { binding, available: transport.available, transport };
  }

  async reconcileSubscriber(contract) {
    const { subscriberName } = contract;
    const selected = this.createBinding(contract);
    const binding = selected?.binding;
    const current = this.active.get(subscriberName);
    if (selected?.transport !== this.local) {
      await this.local.removeBinding(contract);
    }
    if (!selected?.available) {
      if (!current) return;
      if (!binding || !sameTarget(current.binding, binding)) {
        await current.handle.detach();
        this.active.delete(subscriberName);
        return;
      }
      if (current.running) await current.handle.stop();
      current.binding = binding;
      current.target.method = contract.method;
      current.running = false;
      return;
    }
    if (current && isDeepStrictEqual(current.binding, binding)) {
      current.target.method = contract.method;
      if (current.running) return;
    }
    if (current) {
      const close = sameTarget(current.binding, binding)
        ? current.handle.stop
        : current.handle.detach;
      await close();
      this.active.delete(subscriberName);
    }

    const target = { method: contract.method };
    const dispatch = createDispatch(target);
    const handle = await selected.transport.bind(binding, dispatch);
    this.active.set(subscriberName, {
      binding,
      target,
      handle,
      running: true,
    });
  }
}

module.exports = { SubscriptionManager };
