'use strict';

const { metarhia } = require('./deps.js');
const { JetStreamReader } = require('./jetstream-reader.js');

class TaskConsumer extends JetStreamReader {
  constructor(consumer, binding, declaration, execute, logger = console) {
    super(
      consumer,
      `task: ${declaration.name}`,
      declaration.maxMessages,
      logger,
    );
    this.binding = binding;
    this.declaration = declaration;
    this.execute = execute;
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
}

module.exports = { TaskConsumer };
