'use strict';

const { randomUUID } = require('node:crypto');
const { Outbox } = require('./outbox.js');

class EventPublisher {
  constructor() {
    this.outbox = null;
    this.emitters = new Map();
  }

  configure(options) {
    this.outbox = new Outbox(options);
  }

  registerEvent({ eventName, eventSubject }) {
    this.emitters.set(eventName, eventSubject);
  }

  unregisterEvent(eventName) {
    this.emitters.delete(eventName);
  }

  createRecord(name, payload) {
    const subject = this.emitters.get(name);
    if (!subject) throw new Error(`Unknown event: ${name}`);
    if (!this.outbox) {
      throw new Error(
        'Event outbox is not configured: call events.configure()',
      );
    }
    const id = randomUUID();
    const createdAt = new Date().toISOString();
    return { id, name, subject, createdAt, payload };
  }

  async emit(name, payload, { transaction = null } = {}) {
    const record = this.createRecord(name, payload);
    await this.outbox.write(record, transaction);
    return record.id;
  }

  async emitBulk(events, { transaction = null } = {}) {
    if (events.length === 0) return [];
    const records = events.map(({ name, payload }) =>
      this.createRecord(name, payload),
    );
    return this.outbox.writeBulk(records, transaction);
  }
}

module.exports = { EventPublisher };
