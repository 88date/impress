'use strict';

const { node, metarhia, wt } = require('./deps.js');
const { Place } = require('./place.js');

const WORKER_OPTIONS = [
  'concurrency',
  'limiter',
  'lockDuration',
  'stalledInterval',
];

const RESERVED = new Set(['flow', 'queue', 'worker', 'add', 'getStatus']);

const QUEUE_CONFIG = '.queue.js';

const JOB_OPTIONS = [
  'attempts',
  'backoff',
  'timeout',
  'delay',
  'priority',
  'removeOnComplete',
  'removeOnFail',
];

const JOB_EVENTS = ['onCompleted', 'onFailed'];

class MQ extends Place {
  constructor(name, application) {
    super(name, application);
    this.tree = {};
    this.queues = {};
    this.workers = {};
    this.flows = {};
    this.queueEvents = {};
    this.handlers = {};
    this.configs = {};
    this.closing = false;
    this.initializing = false;
  }

  async load(targetPath) {
    if (targetPath) return super.load(targetPath);
    this.initializing = true;
    await super.load();
    this.initializing = false;
    const queueNames = new Set([
      ...Object.keys(this.configs),
      ...Object.keys(this.handlers),
    ]);
    for (const queueName of queueNames) {
      await this.rebuildQueue(queueName);
    }
  }

  getConnection() {
    const mqConfig = this.application.config.mq;
    const connection = { ...mqConfig };
    const prefix = connection.prefix || 'bull';
    delete connection.prefix;
    return { connection, prefix };
  }

  async stop() {
    this.closing = true;
    const graceful = [];
    for (const name of Object.keys(this.workers)) {
      graceful.push(this.workers[name].close(false));
    }
    await Promise.race([
      Promise.allSettled(graceful),
      metarhia.metautil.delay(5000),
    ]);
    const force = [];
    for (const name of Object.keys(this.workers)) {
      force.push(this.workers[name].close(true).catch(() => {}));
    }
    for (const name of Object.keys(this.flows)) {
      force.push(this.flows[name].close());
    }
    for (const name of Object.keys(this.queueEvents)) {
      force.push(this.queueEvents[name].close());
    }
    for (const name of Object.keys(this.queues)) {
      force.push(this.queues[name].close());
    }
    await Promise.allSettled(force);
    this.tree = {};
    this.queues = {};
    this.workers = {};
    this.flows = {};
    this.queueEvents = {};
  }

  async removeQueue(queueName) {
    const closing = [];
    if (this.workers[queueName]) closing.push(this.workers[queueName].close());
    if (this.flows[queueName]) closing.push(this.flows[queueName].close());
    if (this.queueEvents[queueName]) {
      closing.push(this.queueEvents[queueName].close());
    }
    if (this.queues[queueName]) closing.push(this.queues[queueName].close());
    await Promise.allSettled(closing);
    delete this.workers[queueName];
    delete this.flows[queueName];
    delete this.queueEvents[queueName];
    delete this.queues[queueName];
    delete this.handlers[queueName];
    delete this.configs[queueName];
    delete this.tree[queueName];
  }

  async delete(filePath) {
    const relPath = filePath.substring(this.path.length + 1);
    const parts = relPath.split(node.path.sep);
    if (parts.length !== 2) return;
    const [queueName, fileName] = parts;
    if (fileName === QUEUE_CONFIG) {
      delete this.configs[queueName];
      await this.rebuildQueue(queueName);
      return;
    }
    const jobName = node.path.basename(fileName, '.js');
    if (this.handlers[queueName]) {
      delete this.handlers[queueName][jobName];
    }
    const remaining = Object.keys(this.handlers[queueName] || {}).length;
    if (remaining === 0 && this.workers[queueName]) {
      await this.workers[queueName].close().catch(() => {});
      delete this.workers[queueName];
    }
    await this.syncQueueEvents(queueName);
    this.buildTree(queueName);
  }

  loadScript(filePath) {
    const { application } = this;
    const context = Object.assign({}, application.sandbox);
    const sandbox = metarhia.metavm.createContext(context);
    const options = { context: sandbox, filename: filePath };
    return metarhia.metavm.readScript(filePath, options);
  }

  async change(filePath) {
    if (!filePath.endsWith('.js')) return;
    const { application } = this;
    const { config } = application;
    if (!config.mq) return;
    if (this.closing) return;

    const relPath = filePath.substring(this.path.length + 1);
    const parts = relPath.split(node.path.sep);
    if (parts.length !== 2) return;

    const [queueName, fileName] = parts;

    let unit;
    try {
      const script = await this.loadScript(filePath);
      unit = script.exports;
    } catch (error) {
      if (error.code !== 'ENOENT') {
        application.console.error(error.stack);
      }
      return;
    }

    if (fileName === QUEUE_CONFIG) {
      this.configs[queueName] = unit;
      if (!this.initializing) await this.rebuildQueue(queueName);
      return;
    }

    const jobName = node.path.basename(fileName, '.js');
    if (RESERVED.has(jobName)) {
      const msg = `MQ: "${jobName}" is a reserved name`;
      application.console.error(msg);
      return;
    }

    if (!this.handlers[queueName]) {
      this.handlers[queueName] = {};
    }
    this.handlers[queueName][jobName] = unit;

    if (this.initializing) return;

    if (!this.queues[queueName]) {
      await this.rebuildQueue(queueName);
    } else {
      if (!this.workers[queueName]) {
        await this.ensureWorker(queueName);
      }
      await this.syncQueueEvents(queueName);
      this.buildTree(queueName);
    }
  }

  async rebuildQueue(queueName) {
    if (this.closing) return;
    const { application } = this;
    const { Queue, FlowProducer } = require('bullmq');
    const { connection, prefix } = this.getConnection();

    if (this.workers[queueName]) {
      await this.workers[queueName].close();
      delete this.workers[queueName];
    }
    if (this.flows[queueName]) {
      await this.flows[queueName].close();
      delete this.flows[queueName];
    }
    if (this.queueEvents[queueName]) {
      await this.queueEvents[queueName].close();
      delete this.queueEvents[queueName];
    }
    if (this.queues[queueName]) {
      await this.queues[queueName].close();
      delete this.queues[queueName];
    }

    const config = this.configs[queueName] || {};

    const queueOptions = { connection, prefix };
    if (config.defaultJobOptions) {
      queueOptions.defaultJobOptions = config.defaultJobOptions;
    }
    const queue = new Queue(queueName, queueOptions);
    this.queues[queueName] = queue;

    queue.on('error', (error) => {
      const msg = `MQ queue "${queueName}": ${error.message}`;
      application.console.error(msg);
    });

    const flow = new FlowProducer({ connection, prefix });
    this.flows[queueName] = flow;

    flow.on('error', (error) => {
      const msg = `MQ flow "${queueName}": ${error.message}`;
      application.console.error(msg);
    });

    await this.ensureWorker(queueName);
    await this.syncQueueEvents(queueName);
    this.buildTree(queueName);
  }

  async ensureWorker(queueName) {
    const { application } = this;
    if (application.kind !== 'worker') return;
    const hasHandlers = Object.keys(this.handlers[queueName] || {}).length > 0;
    if (!hasHandlers || this.workers[queueName]) return;

    const { Worker } = require('bullmq');
    const { connection, prefix } = this.getConnection();
    const config = this.configs[queueName] || {};

    const workerOptions = { connection, prefix };
    for (const key of WORKER_OPTIONS) {
      if (config[key] !== undefined) {
        workerOptions[key] = config[key];
      }
    }

    const processor = async (job) => {
      const handler = this.handlers[queueName]?.[job.name];
      if (!handler) {
        throw new Error(`Unknown job "${job.name}"`);
      }
      return handler.method(job.data, job);
    };

    const worker = new Worker(queueName, processor, workerOptions);
    this.workers[queueName] = worker;

    worker.on('error', (error) => {
      const msg = `MQ worker "${queueName}"`;
      application.console.error(msg, error.message);
    });

    worker.on('failed', (job, error) => {
      const id = job ? job.id : 'unknown';
      const msg = `MQ job failed "${queueName}/${id}"`;
      application.console.error(msg, error.message);
    });

    if (config.events) {
      for (const [event, handler] of Object.entries(config.events)) {
        worker.on(event, (...args) => {
          try {
            handler(...args);
          } catch (error) {
            const msg = `MQ event "${event}" error`;
            application.console.error(msg, error.message);
          }
        });
      }
    }

    const { schedules } = config;
    const queue = this.queues[queueName];
    const syncWithRetry = async () => {
      if (this.closing || this.queues[queueName] !== queue) return;
      try {
        await MQ.syncSchedulers(queue, schedules);
      } catch (err) {
        if (this.closing || this.queues[queueName] !== queue) return;
        const msg = `MQ schedulers "${queueName}"`;
        application.console.error(msg, err.message);
        setTimeout(syncWithRetry, 5000);
      }
    };
    syncWithRetry();
  }

  async syncQueueEvents(queueName) {
    const { application } = this;
    const handlers = this.handlers[queueName] || {};
    const queue = this.queues[queueName];
    if (!queue) return;

    const needsEvents = Object.values(handlers).some((unit) =>
      JOB_EVENTS.some((e) => typeof unit[e] === 'function'),
    );

    if (needsEvents && wt.threadId === 1 && !this.queueEvents[queueName]) {
      const { QueueEvents } = require('bullmq');
      const { connection, prefix } = this.getConnection();
      const qe = new QueueEvents(queueName, { connection, prefix });
      this.queueEvents[queueName] = qe;

      qe.on('error', (error) => {
        const msg = `MQ events "${queueName}": ${error.message}`;
        application.console.error(msg);
      });

      qe.on('completed', async ({ jobId, returnvalue }) => {
        try {
          const job = await queue.getJob(jobId);
          if (!job) return;
          const handler = this.handlers[queueName]?.[job.name];
          if (handler?.onCompleted) handler.onCompleted(returnvalue, job);
        } catch (err) {
          const msg = `MQ QueueEvents "${queueName}" completed`;
          application.console.error(msg, err.message);
        }
      });

      qe.on('failed', async ({ jobId, failedReason }) => {
        try {
          const job = await queue.getJob(jobId);
          if (!job) return;
          const handler = this.handlers[queueName]?.[job.name];
          if (handler?.onFailed) handler.onFailed(failedReason, job);
        } catch (err) {
          const msg = `MQ QueueEvents "${queueName}" failed`;
          application.console.error(msg, err.message);
        }
      });
    }

    if (!needsEvents && this.queueEvents[queueName]) {
      await this.queueEvents[queueName].close();
      delete this.queueEvents[queueName];
    }
  }

  buildTree(queueName) {
    const queue = this.queues[queueName];
    if (!queue) return;
    const flow = this.flows[queueName];
    const handlers = this.handlers[queueName] || {};

    const proxy = {
      queue,
      worker: this.workers[queueName] || null,
      add: (jobName, data, opts) => {
        if (this.closing) return Promise.resolve();
        return queue.add(jobName, data, opts);
      },
      flow: (flowOpts) => {
        if (this.closing) return Promise.resolve();
        return flow.add(flowOpts);
      },
    };

    for (const [jobName, unit] of Object.entries(handlers)) {
      proxy[jobName] = (data, opts = {}) => {
        const jobOpts = {};
        for (const key of JOB_OPTIONS) {
          if (unit[key] !== undefined) jobOpts[key] = unit[key];
        }
        return queue.add(jobName, data, { ...jobOpts, ...opts });
      };
    }

    this.tree[queueName] = proxy;
  }

  static async syncSchedulers(queue, schedules) {
    const declaredNames = new Set();
    if (schedules) {
      for (const schedule of schedules) {
        declaredNames.add(schedule.name);
        const opts = {};
        if (schedule.pattern) opts.pattern = schedule.pattern;
        if (schedule.every) opts.every = schedule.every;
        await queue.upsertJobScheduler(schedule.name, opts, {
          data: schedule.data || {},
        });
      }
    }
    const existing = await queue.getJobSchedulers();
    for (const scheduler of existing) {
      const name = scheduler?.name ?? scheduler?.key;
      if (!name) continue;
      if (!declaredNames.has(name)) {
        await queue.removeJobScheduler(name);
      }
    }
  }

  async getStatus() {
    const status = {};
    for (const [name, queue] of Object.entries(this.queues)) {
      const counts = await queue.getJobCounts();
      const paused = await queue.isPaused();
      status[name] = { counts, paused };
    }
    return status;
  }
}

module.exports = { MQ };
