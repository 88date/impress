'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

const { JetStreamConsumer } = require('./consumer.js');

class JetStreamSubscriptions {
  constructor(nats, logger = console, managesTopology = false) {
    this.nats = nats;
    this.logger = logger;
    this.managesTopology = managesTopology;
  }

  get enabled() {
    return Boolean(this.nats);
  }

  get available() {
    return Boolean(this.nats?.connection) && !this.nats.isStopping;
  }

  async ensureStream(stream) {
    if (!this.available) return false;
    const { manager } = await this.nats.getJetStream();
    try {
      await manager.streams.info(stream);
    } catch (error) {
      if (error.code !== 10059) throw error;
      return false;
    }
    return true;
  }

  createBinding(contract) {
    const {
      subscriberName,
      stream,
      subject,
      maxMessages = 5,
      maxAckPending,
      retryLimit = 2,
      retryDelay = 0,
      timeout = 30000,
      deliverPolicy = 'new',
      startSequence,
      startTime,
    } = contract;
    const config = this.nats?.application.config.server.nats.jetstream ?? {};
    const { consumerPrefix = 'impress' } = config;
    const subscriberId = `${consumerPrefix}|${subscriberName}`;
    const filter = encodeURIComponent(subject)
      .replaceAll('.', ':')
      .replaceAll('*', '%2A');
    const consumerName = `${subscriberId}|${filter}`;
    return {
      subscriberName,
      stream,
      subject,
      consumerName,
      maxMessages,
      maxAckPending,
      retryDelay,
      timeout,
      options: {
        durable_name: consumerName,
        filter_subject: subject,
        ack_policy: 'explicit',
        deliver_policy: deliverPolicy,
        opt_start_seq:
          deliverPolicy === 'by_start_sequence' ? startSequence : undefined,
        opt_start_time:
          deliverPolicy === 'by_start_time' ? startTime : undefined,
        replay_policy: 'instant',
        ack_wait: (timeout + 1000) * 1_000_000,
        max_deliver: retryLimit + 1,
        max_ack_pending: maxAckPending ?? -1,
      },
    };
  }

  async ensureConsumer(manager, binding) {
    const { stream, consumerName, options, maxAckPending } = binding;
    let info;
    try {
      info = await manager.consumers.info(stream, consumerName);
    } catch (error) {
      if (error.code !== 10014) throw error;
      if (!this.managesTopology) return false;
      await manager.consumers.add(stream, options);
      return true;
    }
    const current = info.config;
    for (const field of ['filter_subject', 'ack_policy']) {
      if (current[field] !== options[field]) {
        throw new Error(`Conflicting JetStream consumer: ${consumerName}`);
      }
    }
    if (current.deliver_subject) {
      throw new Error(`Conflicting JetStream consumer: ${consumerName}`);
    }
    if (
      maxAckPending !== undefined &&
      current.max_ack_pending !== maxAckPending
    ) {
      if (!this.managesTopology) return false;
      await manager.consumers.update(stream, consumerName, {
        max_ack_pending: maxAckPending,
      });
    }
    return true;
  }

  async bind(binding, dispatch) {
    const { client, manager } = await this.nats.getJetStream();
    const ready = await this.ensureConsumer(manager, binding);
    if (!ready) return null;
    const consumer = await client.consumers.get(
      binding.stream,
      binding.consumerName,
    );
    const subscription = new JetStreamConsumer(
      consumer,
      binding,
      dispatch,
      this.logger,
    );
    subscription.start();
    return subscription;
  }
}

module.exports = { JetStreamSubscriptions };
