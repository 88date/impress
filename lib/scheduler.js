'use strict';

/* eslint-disable camelcase -- JetStream API uses snake_case field names. */

const { isDeepStrictEqual } = require('node:util');
const { TaskConsumer } = require('./task-consumer.js');

const TASK_NAMESPACE = 'tasks/';

class Scheduler {
  constructor(config = {}, nats = null, logger = console) {
    const jetstream = nats?.application.config.server.nats.jetstream ?? {};
    this.enabled = config.enabled ?? false;
    this.active = config.active ?? false;
    this.stream = config.stream ?? 'TASKS';
    this.consumerPrefix =
      config.consumerPrefix ?? jetstream.consumerPrefix ?? 'impress';
    this.replicas = config.replicas ?? jetstream.replicas ?? 1;
    this.subjectPrefix = `tasks.${this.stream}.${this.consumerPrefix}`;
    this.nats = nats;
    this.logger = logger;
    this.consumers = new Map();
    this.running = false;
    this.stopping = false;
    this.synchronizing = Promise.resolve();
  }

  async initialize() {
    if (!this.enabled) return;
    const { nats, workers = {} } = this.nats.application.config.server;
    if (!nats.enabled) {
      throw new Error('Tasks require server.nats.enabled');
    }
    if (!workers.pool) throw new Error('Tasks require application workers');
    await this.nats.connect();
  }

  async ensureStream(manager) {
    let info;
    try {
      info = await manager.streams.info(this.stream);
    } catch (error) {
      if (error.code !== 10059) throw error;
      await manager.streams.add({
        name: this.stream,
        subjects: [`tasks.${this.stream}.>`],
        storage: 'file',
        retention: 'workqueue',
        num_replicas: this.replicas,
        allow_msg_schedules: true,
      });
      return;
    }
    if (
      !info.config.allow_msg_schedules ||
      info.config.retention !== 'workqueue'
    ) {
      throw new Error(`Incompatible task stream: ${this.stream}`);
    }
  }

  createBinding(declaration) {
    const taskName = declaration.path.replaceAll('/', ':');
    return {
      stream: this.stream,
      consumerName: `${this.consumerPrefix}|tasks:${taskName}`,
      scheduleSubject: `${this.subjectPrefix}.schedule.${taskName}`,
      subject: `${this.subjectPrefix}.run.${taskName}`,
    };
  }

  async registerSchedule(manager, client, declaration, binding) {
    const fields = declaration.cron.trim().split(/\s+/);
    if (fields.length === 5) fields.unshift('0');
    const cron = fields.join(' ');
    const timezone = declaration.tz ?? 'UTC';
    const { scheduleSubject, subject } = binding;
    const current = await manager.streams.getMessage(this.stream, {
      last_by_subj: scheduleSubject,
    });
    if (
      current &&
      current.header.get('Nats-Schedule') === cron &&
      current.header.get('Nats-Schedule-Target') === subject &&
      current.header.get('Nats-Schedule-Time-Zone') === timezone &&
      isDeepStrictEqual(current.json(), declaration.data)
    ) {
      return;
    }
    await client.publish(scheduleSubject, JSON.stringify(declaration.data), {
      schedule: { specification: { cron }, target: subject, timezone },
    });
  }

  async ensureConsumer(manager, declaration, binding) {
    const { consumerName, subject } = binding;
    const options = {
      durable_name: consumerName,
      filter_subject: subject,
      ack_policy: 'explicit',
      deliver_policy: 'all',
      replay_policy: 'instant',
      ack_wait: (declaration.timeout + 1000) * 1_000_000,
      max_deliver: declaration.retryLimit + 1,
      max_ack_pending: -1,
    };
    let info;
    try {
      info = await manager.consumers.info(this.stream, consumerName);
    } catch (error) {
      if (error.code !== 10014) throw error;
      await manager.consumers.add(this.stream, options);
      return;
    }
    const current = info.config;
    for (const field of [
      'filter_subject',
      'ack_policy',
      'deliver_policy',
      'replay_policy',
    ]) {
      if (current[field] !== options[field]) {
        throw new Error(`Conflicting task consumer: ${consumerName}`);
      }
    }
    if (!this.active) return;
    const changes = {};
    for (const field of ['ack_wait', 'max_deliver', 'max_ack_pending']) {
      if (current[field] !== options[field]) changes[field] = options[field];
    }
    if (Object.keys(changes).length > 0) {
      await manager.consumers.update(this.stream, consumerName, changes);
    }
  }

  async bind(client, declaration, binding, execute) {
    const registered = this.consumers.get(declaration.name);
    if (registered) {
      const sameBuffer =
        registered.declaration.maxMessages === declaration.maxMessages;
      if (sameBuffer && (!this.running || registered.running)) {
        registered.declaration = declaration;
        registered.execute = execute;
        return;
      }
      await registered.stop();
    }
    const consumer = await client.consumers.get(
      this.stream,
      binding.consumerName,
    );
    const subscription = new TaskConsumer(
      consumer,
      binding,
      declaration,
      execute,
      this.logger,
    );
    this.consumers.set(declaration.name, subscription);
    if (this.running) subscription.start();
  }

  synchronize(declarations, execute) {
    if (!this.enabled || this.stopping) return Promise.resolve();
    const synchronize = () => this.synchronizeTasks(declarations, execute);
    this.synchronizing = this.synchronizing.then(synchronize, synchronize);
    return this.synchronizing;
  }

  async synchronizeTasks(declarations, execute) {
    if (this.stopping) return;
    const { client, manager } = await this.nats.getJetStream();
    await this.ensureStream(manager);
    const subjects = new Set();
    for (const declaration of declarations) {
      const binding = this.createBinding(declaration);
      subjects.add(binding.scheduleSubject);
      await this.ensureConsumer(manager, declaration, binding);
      await this.bind(client, declaration, binding, execute);
      if (this.active) {
        await this.registerSchedule(manager, client, declaration, binding);
      }
    }
    if (this.active) {
      const info = await manager.streams.info(this.stream, {
        subjects_filter: `${this.subjectPrefix}.schedule.>`,
      });
      for (const subject of Object.keys(info.state.subjects ?? {})) {
        if (subjects.has(subject)) continue;
        await manager.streams.purge(this.stream, { filter: subject });
      }
    }
    const names = new Set(declarations.map((declaration) => declaration.name));
    for (const [name, consumer] of this.consumers) {
      if (names.has(name)) continue;
      await consumer.stop();
      this.consumers.delete(name);
    }
  }

  start() {
    if (!this.enabled || this.stopping) return;
    this.running = true;
    for (const consumer of this.consumers.values()) consumer.start();
  }

  async stop(timeout = 5000) {
    this.stopping = true;
    this.running = false;
    let timer;
    const close = async () => {
      const consumers = [...this.consumers.values()];
      const closing = consumers.map((consumer) => consumer.stop());
      await Promise.all([
        this.synchronizing.catch((error) => this.logger.error(error)),
        ...closing,
      ]);
      this.consumers.clear();
    };
    const expired = new Promise((resolve) => {
      timer = setTimeout(() => {
        this.logger.error('Task shutdown timed out');
        resolve();
      }, timeout);
    });
    try {
      await Promise.race([close(), expired]);
    } finally {
      clearTimeout(timer);
    }
  }
}

module.exports = { Scheduler, TASK_NAMESPACE };
