'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

class JetStreamReader {
  constructor(consumer, name, maxMessages, logger = console) {
    this.consumer = consumer;
    this.name = name;
    this.maxMessages = maxMessages;
    this.logger = logger;
    this.running = false;
    this.messages = null;
    this.pending = new Set();
    this.consuming = null;
    this.closing = null;
  }

  start() {
    if (this.running || this.closing) return;
    this.running = true;
    this.consuming = this.consume();
  }

  async consume() {
    let messages = null;
    try {
      messages = await this.consumer.consume({
        max_messages: this.maxMessages,
      });
      this.messages = messages;
      if (!this.running) messages.stop();
      for await (const message of messages) {
        if (!this.running) {
          message.nak();
          break;
        }
        const operation = this.handle(message)
          .catch((error) => this.logger.error(error))
          .finally(() => this.pending.delete(operation));
        this.pending.add(operation);
      }
    } catch (error) {
      if (this.running) {
        this.logger.error(`Failed to read ${this.name}`, error);
      }
    } finally {
      this.running = false;
      if (messages) {
        try {
          await messages.close();
        } catch (error) {
          this.logger.error(`Failed to close ${this.name}`, error);
        }
      }
      this.messages = null;
    }
  }

  stop() {
    if (this.closing) return this.closing;
    this.running = false;
    this.messages?.stop();
    this.closing = this.close();
    return this.closing;
  }

  async close() {
    await this.consuming;
    await Promise.all(this.pending);
  }
}

module.exports = { JetStreamReader };
