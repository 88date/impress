'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

class JetStreamConsumer {
  constructor(consumer, binding, dispatch, logger = console) {
    this.consumer = consumer;
    this.binding = binding;
    this.dispatch = dispatch;
    this.logger = logger;
    this.running = false;
    this.messages = null;
    this.pending = new Set();
    this.consuming = null;
    this.closing = null;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.consuming = this.consume();
  }

  async handle(message) {
    const controller = new AbortController();
    let timer;
    try {
      const event = message.json();
      const expired = new Promise((resolve, reject) => {
        timer = setTimeout(() => {
          const error = new Error('Event subscriber timed out');
          controller.abort(error);
          reject(error);
        }, this.binding.timeout);
      });
      const operation = Promise.resolve().then(() =>
        this.dispatch(event, { signal: controller.signal }),
      );
      await Promise.race([operation, expired]);
      message.ack();
    } catch (error) {
      this.logger.error(
        `Failed to execute event subscriber: ${this.binding.subscriberName}`,
        error,
      );
      message.nak(this.binding.retryDelay);
    } finally {
      clearTimeout(timer);
    }
  }

  async consume() {
    let messages = null;
    try {
      messages = await this.consumer.consume({
        max_messages: this.binding.maxMessages,
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
        this.logger.error(
          `Failed to read event subscriber: ${this.binding.subscriberName}`,
          error,
        );
      }
    } finally {
      this.running = false;
      if (messages) await this.closeMessages(messages);
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

  async closeMessages(messages) {
    try {
      await messages.close();
    } catch (error) {
      this.logger.error('Failed to close event subscriber messages', error);
    }
  }
}

module.exports = { JetStreamConsumer };
