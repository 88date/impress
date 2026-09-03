'use strict';

const { isDeepStrictEqual } = require('node:util');

const TASK_NAMESPACE = 'tasks/';

const isSameSchedule = (schedule, declaration) => {
  if (!schedule) return false;
  const { cron, data = {}, options = {} } = declaration;
  return (
    schedule.cron === cron &&
    isDeepStrictEqual(schedule.data, data) &&
    isDeepStrictEqual(schedule.options, options)
  );
};

const serializeJob = (job) => {
  const result = {};
  for (const [name, value] of Object.entries(job)) {
    if (name !== 'signal') result[name] = value;
  }
  return result;
};

class Scheduler {
  constructor(config = {}, pgboss = null) {
    const { enabled = false, active = false, notify = false } = config;
    this.enabled = enabled;
    this.active = active;
    this.notify = notify;
    this.pgboss = pgboss;
    this.declarations = new Map();
    this.consumers = new Map();
  }

  get client() {
    return this.pgboss?.client || null;
  }

  async ensureQueue(name, options = {}) {
    const client = this.client;
    const settings = { ...options, notify: this.notify };
    let queue = await client.getQueue(name);
    if (!queue) {
      await client.createQueue(name, settings);
      if (!this.active) return;
      queue = await client.getQueue(name);
    }
    if (!this.active) return;

    const changes = {};
    for (const [field, value] of Object.entries(settings)) {
      if (value === undefined) continue;
      if (isDeepStrictEqual(queue[field], value)) continue;
      if (field === 'policy' || field === 'partition') {
        throw new Error(`Task queue "${name}": ${field} cannot be changed`);
      }
      changes[field] = value;
    }
    if (Object.keys(changes).length > 0) {
      await client.updateQueue(name, changes);
    }
  }

  async register(declarations, schedules = new Map()) {
    const client = this.client;
    if (!this.enabled || !this.active || !client) return;

    for (const declaration of declarations) {
      const { name, cron, data = {}, options = {}, queue = {} } = declaration;
      const schedule = schedules.get(name);
      await this.ensureQueue(name, queue);
      if (isSameSchedule(schedule, declaration)) {
        this.declarations.set(name, declaration);
        continue;
      }
      await client.schedule(name, cron, data, options);
      this.declarations.set(name, declaration);
    }
  }

  async unregister(names) {
    const client = this.client;
    if (!this.enabled || !this.active || !client) return;

    for (const name of names) {
      await client.unschedule(name);
      this.declarations.delete(name);
    }
  }

  async registerConsumers(declarations, execute) {
    const client = this.client;
    if (!this.enabled || !client || typeof execute !== 'function') return;

    for (const declaration of declarations) {
      const { name, worker = {}, queue = {} } = declaration;
      const registered = this.consumers.get(name);
      if (registered && isDeepStrictEqual(registered.worker, worker)) {
        registered.declaration = declaration;
        registered.execute = execute;
        continue;
      }
      if (registered) {
        await client.offWork(name, { id: registered.workId });
        this.consumers.delete(name);
      }

      const consumer = { declaration, worker, execute, workId: null };
      this.consumers.set(name, consumer);
      const handler = async (jobs) => {
        const active = this.consumers.get(name);
        if (!active) throw new Error(`Task consumer not found: ${name}`);
        const job = jobs[0];
        if (!job) throw new Error(`Task job not found: ${name}`);
        return active.execute(active.declaration, serializeJob(job));
      };
      try {
        if (!this.active) await this.ensureQueue(name, queue);
        const options = { ...worker, includeMetadata: true };
        consumer.workId = await client.work(name, options, handler);
      } catch (error) {
        this.consumers.delete(name);
        throw error;
      }
    }
  }

  async unregisterConsumers(names) {
    const client = this.client;
    if (!this.enabled || !client) return;

    for (const name of names) {
      const consumer = this.consumers.get(name);
      if (!consumer) continue;
      await client.offWork(name, { id: consumer.workId });
      this.consumers.delete(name);
    }
  }

  async synchronizeConsumers(declarations, execute) {
    if (!this.enabled || typeof execute !== 'function') return;

    await this.registerConsumers(declarations, execute);
    const names = new Set(declarations.map((declaration) => declaration.name));
    const currentNames = [...this.consumers.keys()];
    const removed = currentNames.filter((name) => !names.has(name));
    await this.unregisterConsumers(removed);
  }

  async synchronize(declarations, execute) {
    const client = this.client;
    if (!this.enabled || !client) return;

    if (!this.active) {
      await this.synchronizeConsumers(declarations, execute);
      return;
    }

    const schedules = await client.getSchedules();
    const existing = new Map();
    for (const schedule of schedules) {
      existing.set(schedule.name, schedule);
    }
    await this.register(declarations, existing);
    await this.synchronizeConsumers(declarations, execute);

    const names = new Set(declarations.map((declaration) => declaration.name));
    const removed = [];
    for (const schedule of schedules) {
      if (!schedule.name.startsWith(TASK_NAMESPACE)) continue;
      if (names.has(schedule.name)) continue;
      removed.push(schedule.name);
    }
    await this.unregister(removed);
  }
}

module.exports = { Scheduler, TASK_NAMESPACE };
