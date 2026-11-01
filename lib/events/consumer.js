'use strict';

const { JetStreamReader } = require('../jetstream-reader.js');
const { withTimeout } = require('../timeout.js');

class JetStreamConsumer extends JetStreamReader {
  constructor(consumer, binding, dispatch, logger = console) {
    super(
      consumer,
      `event subscriber: ${binding.subscriberName}`,
      binding.maxMessages,
      logger,
    );
    this.binding = binding;
    this.dispatch = dispatch;
  }

  async handle(message) {
    try {
      const event = message.json();
      const dispatch = (signal) =>
        this.dispatch(event, { signal, headers: message.headers });
      await withTimeout(
        dispatch,
        this.binding.timeout,
        'Event subscriber timed out',
      );
      message.ack();
    } catch (error) {
      this.logger.error(
        `Failed to execute event subscriber: ${this.binding.subscriberName}`,
        error,
      );
      message.nak(this.binding.retryDelay);
    }
  }
}

module.exports = { JetStreamConsumer };
