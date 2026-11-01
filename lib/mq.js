'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Place } = require('./place.js');
const { MQConsumer } = require('./mq-consumer.js');

const QUEUE_CONFIG = '.queue.js';
const NAME_PATTERN = /^[^.:*>\s]+$/;
const DELAY_UNITS = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 };
const QUEUE_OPTIONS = new Set([
  'stream',
  'maxAckPending',
  'maxMessages',
  'retryLimit',
  'retryDelay',
  'timeout',
]);
const LEGACY_JOB_OPTIONS = new Set([
  'attempts',
  'backoff',
  'timeout',
  'delay',
  'priority',
  'removeOnComplete',
  'removeOnFail',
  'onCompleted',
  'onFailed',
  'repeat',
  'jobId',
]);

const parseDelay = (value = 0) => {
  let delay = value;
  if (typeof value === 'string') {
    const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
    if (!match) throw new Error('Invalid MQ delay');
    delay = Number(match[1]) * DELAY_UNITS[match[2]];
  }
  if (!Number.isSafeInteger(delay) || delay < 0) {
    throw new Error('Invalid MQ delay');
  }
  return delay;
};

const prepareMessage = (name, data, options = {}) => {
  for (const key of Object.keys(options)) {
    if (key !== 'delay' && key !== 'msgID') {
      throw new Error('MQ publication option is not supported: ' + key);
    }
  }
  const delay = parseDelay(options.delay);
  if (!/^[^.:*>\s]+:[^.:*>\s]+$/.test(name)) {
    throw new Error('MQ task name must have the form queue:job');
  }
  const payload = JSON.stringify(data);
  if (payload === undefined) {
    throw new Error('MQ data must be JSON-serializable');
  }
  let subject = 'mq.' + name.replace(':', '.');
  const publishOptions = { msgID: options.msgID };
  if (delay > 0) {
    const at = new Date(Date.now() + delay);
    if (!Number.isFinite(at.getTime())) throw new Error('Invalid MQ delay');
    publishOptions.schedule = { specification: { at }, target: subject };
    subject += '.' + node.crypto.randomUUID();
  }
  return { subject, payload, options: publishOptions };
};

const validateConfig = (config) => {
  for (const [key, value] of Object.entries(config)) {
    if (!QUEUE_OPTIONS.has(key)) {
      throw new Error('Unsupported MQ queue option: ' + key);
    }
    if (value === undefined) continue;
    if (key === 'stream') {
      if (typeof value !== 'string' || value.trim() === '') {
        throw new Error('MQ stream must be a non-empty name');
      }
      continue;
    }
    if (key === 'retryLimit') {
      const isInteger = Number.isSafeInteger(value);
      const isSafeLimit = isInteger && Number.isSafeInteger(value + 1);
      if (!isInteger || !isSafeLimit || value < -1) {
        throw new Error('Invalid MQ queue option: retryLimit');
      }
      continue;
    }
    if (key === 'maxAckPending' && value === -1) continue;
    const minimum = key === 'retryDelay' ? 0 : 1;
    if (!Number.isSafeInteger(value) || value < minimum) {
      throw new Error('Invalid MQ queue option: ' + key);
    }
  }
};

class MQ extends Place {
  constructor(name, application) {
    super(name, application);
    this.tree = {
      add: (name, data, options) => this.add(name, data, options),
      addBulk: (jobs) => this.addBulk(jobs),
      getStatus: (name, options) => this.getStatus(name, options),
    };
    this.consumers = Object.create(null);
    this.handlers = Object.create(null);
    this.configs = Object.create(null);
    this.closing = false;
    this.started = false;
    this.loadError = null;
    this.updating = Promise.resolve();
  }

  async add(name, data, options = {}) {
    if (this.closing) throw new Error('MQ is stopping');
    const message = prepareMessage(name, data, options);
    const { nats } = this.application;
    if (!nats) throw new Error('NATS is disabled');
    const client = nats.getJetStreamClient();
    return client.publish(message.subject, message.payload, message.options);
  }

  async addBulk(jobs) {
    if (this.closing) throw new Error('MQ is stopping');
    if (jobs.length === 0) return null;
    const messages = jobs.map(({ name, data, options }) => {
      const message = prepareMessage(name, data, options);
      const { msgID, schedule } = message.options;
      const headers = npm.nats.headers();
      if (msgID) headers.set('Nats-Msg-Id', msgID);
      if (schedule) {
        const at = schedule.specification.at.toISOString();
        headers.set('Nats-Schedule', '@at ' + at);
        headers.set('Nats-Schedule-Target', schedule.target);
      }
      return { ...message, options: { headers } };
    });
    const { nats } = this.application;
    if (!nats) throw new Error('NATS is disabled');
    const client = nats.getJetStreamClient();
    const first = messages[0];
    if (messages.length === 1) {
      // SDK commit() adds a message, so a single entry uses native headers.
      const { headers } = first.options;
      headers.set('Nats-Batch-Id', node.crypto.randomUUID());
      headers.set('Nats-Batch-Sequence', '1');
      headers.set('Nats-Batch-Commit', '1');
      return client.publish(first.subject, first.payload, first.options);
    }
    const batch = await client.startBatch(
      first.subject,
      first.payload,
      first.options,
    );
    for (let i = 1; i < messages.length - 1; i++) {
      const { subject, payload, options } = messages[i];
      batch.add(subject, payload, options);
    }
    const last = messages.at(-1);
    return batch.commit(last.subject, last.payload, last.options);
  }

  update(operation) {
    const pending = this.updating.then(operation);
    this.updating = pending.catch(() => {});
    return pending;
  }

  start() {
    return this.update(async () => {
      if (this.closing || this.started) return;
      if (this.loadError) throw this.loadError;
      this.started = true;
      try {
        for (const queueName of Object.keys(this.handlers)) {
          await this.rebuildQueue(queueName);
        }
      } catch (error) {
        this.started = false;
        await this.stopConsumers();
        throw error;
      }
    });
  }

  async stopConsumers() {
    await Promise.all(Object.values(this.consumers).map((item) => item.stop()));
    this.consumers = Object.create(null);
  }

  async stop() {
    this.closing = true;
    this.started = false;
    await this.updating;
    await this.stopConsumers();
  }

  async delete(filePath) {
    if (this.closing) return;
    const parts = filePath.substring(this.path.length + 1).split(node.path.sep);
    if (parts.length > 2) return;
    const [queueName, fileName] = parts;
    try {
      await this.update(async () => {
        if (this.closing) return;
        if (!this.configs[queueName] && !this.handlers[queueName]) return;
        if (!fileName) {
          await this.consumers[queueName]?.stop();
          delete this.consumers[queueName];
          delete this.handlers[queueName];
          delete this.configs[queueName];
          delete this.tree[queueName];
          return;
        }
        if (!fileName.endsWith('.js')) return;
        if (fileName === QUEUE_CONFIG) {
          await this.configureQueue(queueName, undefined);
        } else {
          const jobName = node.path.basename(fileName, '.js');
          delete this.handlers[queueName]?.[jobName];
          await this.rebuildQueue(queueName);
        }
        this.buildTree(queueName);
      });
    } catch (error) {
      this.application.console.error(error);
    }
  }

  loadScript(filePath) {
    const { application } = this;
    const context = Object.assign({}, application.sandbox);
    const sandbox = metarhia.metavm.createContext(context);
    const options = { context: sandbox, filename: filePath };
    return metarhia.metavm.readScript(filePath, options);
  }

  async change(filePath) {
    if (this.closing || !filePath.endsWith('.js')) return;
    const parts = filePath.substring(this.path.length + 1).split(node.path.sep);
    if (parts.length !== 2) return;
    const [queueName, fileName] = parts;
    try {
      await this.update(async () => {
        if (this.closing) return;
        if (['add', 'addBulk', 'getStatus', '__proto__'].includes(queueName)) {
          throw new Error('MQ: "' + queueName + '" is a reserved queue name');
        }
        if (!NAME_PATTERN.test(queueName)) {
          throw new Error('Invalid MQ queue name: ' + queueName);
        }
        const { exports: unit } = await this.loadScript(filePath);
        if (!unit || typeof unit !== 'object' || Array.isArray(unit)) {
          throw new Error('MQ declaration must be an object: ' + filePath);
        }
        if (fileName === QUEUE_CONFIG) {
          validateConfig(unit);
          await this.configureQueue(queueName, unit);
        } else {
          const jobName = node.path.basename(fileName, '.js');
          if (
            ['add', 'addBulk'].includes(jobName) ||
            !NAME_PATTERN.test(jobName)
          ) {
            throw new Error('Invalid or reserved MQ job name: ' + jobName);
          }
          for (const key of Object.keys(unit)) {
            if (LEGACY_JOB_OPTIONS.has(key)) {
              throw new Error('Unsupported MQ handler option: ' + key);
            }
          }
          if (typeof unit.method !== 'function') {
            throw new Error('MQ handler must define method: ' + filePath);
          }
          this.handlers[queueName] ??= Object.create(null);
          this.handlers[queueName][jobName] = unit;
        }
        this.buildTree(queueName);
        if (fileName !== QUEUE_CONFIG) await this.rebuildQueue(queueName);
      });
    } catch (error) {
      if (error.code === 'ENOENT') return;
      if (!this.started) this.loadError = error;
      this.application.console.error(error);
    }
  }

  async configureQueue(queueName, config) {
    const previous = this.configs[queueName];
    if (config === undefined) delete this.configs[queueName];
    else this.configs[queueName] = config;
    try {
      await this.rebuildQueue(queueName, true);
    } catch (error) {
      if (previous === undefined) delete this.configs[queueName];
      else this.configs[queueName] = previous;
      throw error;
    }
  }

  async rebuildQueue(queueName, reconfigure = false) {
    if (this.closing || !this.started) return;
    const { application } = this;
    if (application.kind !== 'worker') return;
    const handlers = this.handlers[queueName] || {};
    const hasHandlers = Object.keys(handlers).length > 0;
    const previous = this.consumers[queueName];
    if (!hasHandlers) {
      await previous?.stop();
      delete this.consumers[queueName];
      return;
    }
    if (previous && !reconfigure) return;
    if (!application.nats) throw new Error('NATS is disabled');
    const consumer = await MQConsumer.create(
      application.nats,
      queueName,
      handlers,
      this.configs[queueName],
      application.console,
    );
    await previous?.stop();
    this.consumers[queueName] = consumer;
    if (!this.closing) consumer.start();
  }

  buildTree(queueName) {
    const handlers = this.handlers[queueName] || {};
    if (!this.configs[queueName] && Object.keys(handlers).length === 0) {
      delete this.tree[queueName];
      return;
    }
    const queue = Object.create(null);
    queue.add = (name, data, options) =>
      this.add(queueName + ':' + name, data, options);
    queue.addBulk = (jobs) =>
      this.addBulk(
        jobs.map(({ name, data, options }) => ({
          name: queueName + ':' + name,
          data,
          options,
        })),
      );
    for (const jobName of Object.keys(handlers)) {
      queue[jobName] = (data, options) =>
        this.add(queueName + ':' + jobName, data, options);
    }
    this.tree[queueName] = queue;
  }

  async getQueueStatus(name, options = {}) {
    if (!NAME_PATTERN.test(name)) {
      throw new Error('Invalid MQ queue name: ' + name);
    }
    const { nats } = this.application;
    if (!nats) throw new Error('NATS is disabled');
    const stream = options.stream ?? this.configs[name]?.stream ?? 'MQ';
    const consumerName = `mq-${encodeURIComponent(name)}`;
    const { manager } = await nats.getJetStream();
    let info;
    try {
      info = await manager.consumers.info(stream, consumerName);
    } catch (error) {
      if (error.code !== 10014) throw error;
      const { state } = await manager.streams.info(stream, {
        // eslint-disable-next-line camelcase -- JetStream API field.
        subjects_filter: `mq.${name}.*`,
      });
      const counts = Object.values(state.subjects ?? {});
      return {
        stream,
        consumer: null,
        running: false,
        pending: counts.reduce((total, count) => total + count, 0),
        unacknowledged: 0,
      };
    }
    const receiver = this.consumers[name];
    return {
      stream,
      consumer: info.name,
      running: receiver?.stream === stream && receiver.running,
      pending: info.num_pending,
      unacknowledged: info.num_ack_pending,
    };
  }

  async getStatus(name, options) {
    if (name !== undefined) return this.getQueueStatus(name, options);
    const status = {};
    for (const [name, receiver] of Object.entries(this.consumers)) {
      const info = await receiver.consumer.info();
      status[name] = {
        stream: receiver.stream,
        consumer: info.name,
        running: receiver.running,
        pending: info.num_pending,
        unacknowledged: info.num_ack_pending,
      };
    }
    return status;
  }
}

module.exports = { MQ };
