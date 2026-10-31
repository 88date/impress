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

  async emit(name, payload, { transaction = null } = {}) {
    const subject = this.emitters.get(name);
    if (!subject) throw new Error(`Unknown event: ${name}`);
    if (!this.outbox) {
      throw new Error(
        'Event outbox is not configured: call events.configure()',
      );
    }
    const id = randomUUID();
    const createdAt = new Date().toISOString();
    await this.outbox.write(
      { id, name, subject, createdAt, payload },
      transaction,
    );
    return id;
  }
}

module.exports = { EventPublisher };
