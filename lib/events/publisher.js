'use strict';

const { randomUUID } = require('node:crypto');

const NATS_EVENT_QUEUE = 'events/nats/publish';

class EventPublisher {
  constructor(pgboss = null, nats = null) {
    this.pgboss = pgboss;
    this.nats = nats;
    this.notify = pgboss?.config?.useListenNotify === true;
    this.workId = null;
    this.operation = null;
    this.emitters = new Map();
  }

  get client() {
    return this.pgboss?.client || null;
  }

  get pgbossEnabled() {
    return this.client !== null;
  }

  get natsEnabled() {
    return Boolean(this.nats?.connection);
  }

  async ensureQueue() {
    const options = { notify: this.notify };
    const queue = await this.client.getQueue(NATS_EVENT_QUEUE);
    if (!queue) {
      await this.client.createQueue(NATS_EVENT_QUEUE, options);
      return;
    }
    if (queue.notify !== options.notify) {
      await this.client.updateQueue(NATS_EVENT_QUEUE, options);
    }
  }

  async start() {
    if (!this.pgbossEnabled || !this.natsEnabled) return;
    if (this.workId) return;
    if (this.operation) {
      await this.operation;
      return;
    }

    this.operation = this.startPublisher();
    try {
      await this.operation;
    } finally {
      this.operation = null;
    }
  }

  async startPublisher() {
    await this.ensureQueue();
    const handler = async (jobs) => {
      const messages = jobs.map((job) => job.data);
      await this.publishNats(messages);
    };
    const options = {};
    this.workId = await this.client.work(NATS_EVENT_QUEUE, options, handler);
  }

  async stop() {
    if (this.operation) await this.operation;
    if (!this.workId) return;
    const workId = this.workId;
    this.workId = null;
    await this.client.offWork(NATS_EVENT_QUEUE, { id: workId, wait: true });
  }

  registerEvent({ eventName, eventSubject, transports }) {
    this.emitters.set(eventName, {
      eventSubject,
      transports: new Set(transports),
    });
  }

  unregisterEvent(eventName) {
    this.emitters.delete(eventName);
  }

  async emit(eventName, data, { transaction = null } = {}) {
    const emitter = this.emitters.get(eventName);
    if (!emitter) throw new Error(`Unknown event: ${eventName}`);
    const send = this.createDispatcher(eventName, emitter.transports);
    const event = {
      id: randomUUID(),
      name: eventName,
      createdAt: new Date().toISOString(),
      data,
    };
    const message = { subject: emitter.eventSubject, event };
    await send.call(this, message, transaction);
    return event.id;
  }

  createDispatcher(eventName, transports) {
    const local = transports.has('local');
    const nats = transports.has('nats');
    if (local && !this.pgbossEnabled) {
      throw new Error(`Event ${eventName} requires pg-boss for local delivery`);
    }
    if (nats && !this.natsEnabled) {
      throw new Error(`Event ${eventName} requires NATS for delivery`);
    }
    if (local && nats) return this.emitCombined;
    if (local) return this.emitLocal;
    if (nats && this.pgbossEnabled) {
      return this.emitNatsQueued;
    }
    if (nats) return this.emitNatsDirect;
    throw new Error(`Event ${eventName} has no delivery transports`);
  }

  emitLocal({ event }, transaction) {
    return this.pgboss.publish(event.name, event, { transaction });
  }

  emitNatsQueued(message, transaction) {
    return this.pgboss.send(NATS_EVENT_QUEUE, message, { transaction });
  }

  async emitCombined(message, transaction) {
    if (!transaction) {
      await this.pgboss.withTransaction((current) =>
        this.emitCombined(message, current),
      );
      return;
    }
    await this.emitLocal(message, transaction);
    await this.emitNatsQueued(message, transaction);
  }

  async emitNatsDirect(message, transaction) {
    if (transaction) {
      throw new Error('Transactional NATS event requires pgboss');
    }
    await this.publishNats(message);
  }

  async publishNats(messages) {
    const batch = Array.isArray(messages) ? messages : [messages];
    for (const { subject, event } of batch) {
      await this.nats.publishEvent(subject, event);
    }
    await this.nats.connection.flush();
  }
}

module.exports = { EventPublisher, NATS_EVENT_QUEUE };
