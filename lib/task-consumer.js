'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

const { metarhia } = require('./deps.js');

class TaskConsumer {
  constructor(consumer, binding, declaration, execute, logger = console) {
    this.consumer = consumer;
    this.binding = binding;
    this.declaration = declaration;
    this.execute = execute;
    this.logger = logger;
    this.running = false;
    this.messages = null;
    this.pending = new Set();
    this.consuming = null;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.consuming = this.consume();
  }

  async handle(message) {
    const declaration = this.declaration;
    try {
      const { streamSequence, timestampNanos, deliveryCount } = message.info;
      const job = {
        id: `${this.binding.stream}:${streamSequence}`,
        name: declaration.name,
        data: message.json(),
        createdAt: new Date(timestampNanos / 1_000_000).toISOString(),
        retryCount: deliveryCount - 1,
      };
      const operation = this.execute(declaration, job);
      await metarhia.metautil.timeoutify(operation, declaration.timeout);
      message.ack();
    } catch (error) {
      this.logger.error(`Failed to execute task: ${declaration.name}`, error);
      message.nak(declaration.retryDelay);
    }
  }

  async consume() {
    let messages = null;
    try {
      messages = await this.consumer.consume({
        max_messages: this.declaration.maxMessages,
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
          `Failed to read task: ${this.declaration.name}`,
          error,
        );
      }
    } finally {
      this.running = false;
      if (messages) {
        try {
          await messages.close();
        } catch (error) {
          this.logger.error('Failed to close task messages', error);
        }
      }
      this.messages = null;
    }
  }

  async stop() {
    this.running = false;
    this.messages?.stop();
    await this.consuming;
    await Promise.all(this.pending);
  }
}

module.exports = { TaskConsumer };
