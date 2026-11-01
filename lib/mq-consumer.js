'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

const { JetStreamReader } = require('./jetstream-reader.js');
const { withTimeout } = require('./timeout.js');

class MQConsumer extends JetStreamReader {
  constructor(consumer, queueName, handlers, options = {}, logger = console) {
    super(consumer, `MQ queue: ${queueName}`, options.maxMessages ?? 5, logger);
    this.queueName = queueName;
    this.handlers = handlers;
    this.stream = options.stream ?? 'MQ';
    this.retryDelay = options.retryDelay ?? 0;
    this.timeout = options.timeout ?? 30000;
  }

  static async create(
    nats,
    queueName,
    handlers,
    options = {},
    logger = console,
  ) {
    const {
      stream = 'MQ',
      maxAckPending,
      retryLimit,
      timeout = 30000,
    } = options;
    const maxDeliver = retryLimit >= 0 ? retryLimit + 1 : -1;
    const { client, manager } = await nats.getJetStream();
    const consumerName = `mq-${encodeURIComponent(queueName)}`;
    const filter = `mq.${queueName}.*`;
    let info;
    try {
      info = await manager.consumers.info(stream, consumerName);
    } catch (error) {
      if (error.code !== 10014) throw error;
      await manager.consumers.add(stream, {
        durable_name: consumerName,
        filter_subject: filter,
        ack_policy: 'explicit',
        deliver_policy: 'all',
        replay_policy: 'instant',
        ack_wait: (timeout + 1000) * 1_000_000,
        max_ack_pending: maxAckPending ?? -1,
        max_deliver: maxDeliver,
      });
    }
    if (info) {
      const expected = { filter_subject: filter, ack_policy: 'explicit' };
      for (const [field, value] of Object.entries(expected)) {
        if (info.config[field] !== value) {
          throw new Error(`Conflicting MQ consumer: ${consumerName}`);
        }
      }
      if (info.config.deliver_subject) {
        throw new Error(`Conflicting MQ consumer: ${consumerName}`);
      }
      const updates = {};
      if (
        maxAckPending !== undefined &&
        info.config.max_ack_pending !== maxAckPending
      ) {
        updates.max_ack_pending = maxAckPending;
      }
      if (retryLimit !== undefined && info.config.max_deliver !== maxDeliver) {
        updates.max_deliver = maxDeliver;
      }
      if (Object.keys(updates).length > 0) {
        await manager.consumers.update(stream, consumerName, updates);
      }
    }
    const consumer = await client.consumers.get(stream, consumerName);
    return new MQConsumer(consumer, queueName, handlers, options, logger);
  }

  async handle(message) {
    try {
      const name = message.subject.slice(message.subject.lastIndexOf('.') + 1);
      const handler = this.handlers[name];
      if (typeof handler?.method !== 'function') {
        throw new Error(`Unknown MQ job: ${this.queueName}:${name}`);
      }
      const data = message.json();
      const { streamSequence, timestampNanos, deliveryCount } = message.info;
      const execute = (signal) => {
        const job = {
          id: `${this.stream}:${streamSequence}`,
          name,
          data,
          createdAt: new Date(timestampNanos / 1_000_000).toISOString(),
          retryCount: deliveryCount - 1,
          signal,
          headers: message.headers,
        };
        return handler.method(data, job);
      };
      await withTimeout(execute, this.timeout, 'MQ job timed out');
      message.ack();
    } catch (error) {
      this.logger.error(`Failed to execute MQ job: ${this.queueName}`, error);
      message.nak(this.retryDelay);
    }
  }
}

module.exports = { MQConsumer };
