'use strict';

const { isDeepStrictEqual } = require('node:util');

const SUBSCRIBER_QUEUE_PREFIX = 'subscribers/';
const PGBOSS_DEFAULTS = Object.freeze({
  retryLimit: 2,
  retryDelay: 0,
  expireInSeconds: 900,
});

const ensureQueue = async (client, name, options) => {
  const queue = await client.getQueue(name);
  if (!queue) {
    await client.createQueue(name, options);
    return;
  }
  const changes = {};
  for (const [field, value] of Object.entries(options)) {
    if (!isDeepStrictEqual(queue[field], value)) changes[field] = value;
  }
  if (Object.keys(changes).length > 0) {
    await client.updateQueue(name, changes);
  }
};

const bindPgboss = async (
  pgboss,
  binding,
  dispatch,
  managesTopology = true,
) => {
  const { client } = pgboss;
  const { eventName, queueName, queueOptions, workOptions } = binding;
  if (managesTopology) {
    await ensureQueue(client, queueName, queueOptions);
    await pgboss.replaceSubscription(eventName, queueName);
  } else {
    const queue = await client.getQueue(queueName);
    if (!queue) return null;
  }
  const handler = async (jobs) => {
    for (const job of jobs) {
      const context = job.signal ? { signal: job.signal } : null;
      await dispatch(job.data, context);
    }
  };
  const workId = await client.work(queueName, workOptions, handler);
  let stopped = false;
  let detached = false;
  const stop = async () => {
    if (stopped) return;
    await client.offWork(queueName, { id: workId, wait: true });
    stopped = true;
  };
  const detach = async () => {
    if (detached) return;
    await stop();
    if (managesTopology) await client.unsubscribe(eventName, queueName);
    detached = true;
  };
  return { stop, detach };
};

const toSeconds = (milliseconds, defaultValue) => {
  if (!milliseconds) return defaultValue;
  return Math.ceil(milliseconds / 1000);
};

const createPgbossBinding = (contract, notify) => ({
  transport: 'pgboss',
  subscriberName: contract.subscriberName,
  eventName: contract.eventName,
  queueName:
    contract.queueName ??
    SUBSCRIBER_QUEUE_PREFIX +
      (contract.subscriberPath ?? contract.subscriberName.replaceAll(':', '/')),
  queueOptions: {
    notify,
    retryLimit: contract.retryLimit ?? PGBOSS_DEFAULTS.retryLimit,
    retryDelay: toSeconds(contract.retryDelay, PGBOSS_DEFAULTS.retryDelay),
    expireInSeconds: toSeconds(
      contract.timeout,
      PGBOSS_DEFAULTS.expireInSeconds,
    ),
  },
  workOptions: {
    localConcurrency: contract.concurrency ?? 1,
  },
});

class PgbossSubscriptions {
  constructor(pgboss, managesTopology = false) {
    this.pgboss = pgboss;
    this.managesTopology = managesTopology;
  }

  get available() {
    return Boolean(this.pgboss?.client);
  }

  createBinding(contract) {
    const notify = this.pgboss?.config?.useListenNotify === true;
    return createPgbossBinding(contract, notify);
  }

  bind(binding, dispatch) {
    return bindPgboss(this.pgboss, binding, dispatch, this.managesTopology);
  }

  async removeQueue(name) {
    if (!this.managesTopology) return true;
    if (!this.available) return false;
    await this.pgboss.client.deleteQueue(name);
    return true;
  }

  async removeBinding(contract) {
    if (!this.managesTopology || !this.available) return;
    const { queueName } = this.createBinding(contract);
    await this.pgboss.clearSubscription(queueName);
  }

  async removeStaleQueues(declared) {
    if (!this.managesTopology || !this.available) return;
    const queues = await this.pgboss.client.getQueues();
    for (const { name } of queues) {
      if (!name.startsWith(SUBSCRIBER_QUEUE_PREFIX)) continue;
      if (!declared.has(name)) await this.removeQueue(name);
    }
  }
}

const bindNats = (nats, binding, dispatch, logger) => {
  const pending = new Set();
  const callback = (error, message) => {
    if (error) {
      logger.error(error);
      return Promise.resolve();
    }
    const operation = Promise.resolve()
      .then(() => dispatch(message.json()))
      .catch((error) => {
        logger.error(
          `Failed to execute event subscriber: ${binding.subscriberName}`,
          error,
        );
      })
      .finally(() => pending.delete(operation));
    pending.add(operation);
    return operation;
  };
  const subscription = nats.connection.subscribe(binding.eventSubject, {
    queue: binding.queueGroup,
    callback,
  });
  let stopped = false;
  const stop = async () => {
    if (stopped) return;
    await subscription.drain();
    await Promise.allSettled(pending);
    stopped = true;
  };
  return { stop, detach: stop };
};

const createNatsBinding = (contract) => ({
  transport: 'nats',
  subscriberName: contract.subscriberName,
  eventSubject:
    contract.eventSubject ?? contract.eventName.replaceAll(':', '.'),
  queueGroup:
    contract.queueGroup ?? contract.subscriberName.replaceAll(':', '.'),
});

class NatsSubscriptions {
  constructor(nats, logger = console) {
    this.nats = nats;
    this.logger = logger;
    this.createBinding = createNatsBinding;
  }

  get available() {
    return Boolean(this.nats?.connection);
  }

  bind(binding, dispatch) {
    return bindNats(this.nats, binding, dispatch, this.logger);
  }
}

module.exports = {
  PgbossSubscriptions,
  NatsSubscriptions,
  SUBSCRIBER_QUEUE_PREFIX,
};
