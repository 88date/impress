'use strict';

const { node, npm } = require('./deps.js');

const DEFAULT_LOG_EVENTS = Object.freeze(['error', 'warning']);
const PGBOSS_EVENTS = new Set(Object.values(npm.pgboss.events));

const getLogEvents = (value = DEFAULT_LOG_EVENTS) => {
  if (!Array.isArray(value)) {
    throw new Error('pgboss logEvents must be an array');
  }
  const events = [...new Set(value)];
  for (const event of events) {
    if (!PGBOSS_EVENTS.has(event)) {
      throw new Error(`Unknown pgboss log event: ${event}`);
    }
  }
  return events;
};

const getPgbossConfig = (config = {}, scheduler = {}) => {
  if (!scheduler.enabled) return config;
  return { ...config, useListenNotify: scheduler.notify === true };
};

const loadConfig = async (config) => {
  const { ssl } = config;
  if (!ssl?.caPath) return config;
  const { caPath, ...options } = ssl;
  const ca = await node.fsp.readFile(caPath, 'utf8');
  return { ...config, ssl: { ...options, ca } };
};

const watchEvents = (client, logger, events) => {
  const handlers = {
    error: (error) => {
      const details = error?.stack || error?.message || String(error);
      logger.error('pgboss error', details);
    },
    warning: ({ message, data }) => {
      logger.warn(`pgboss warning: ${message}`, data);
    },
    wip: (workers) => {
      logger.debug('pgboss workers', workers);
    },
    stopped: () => {
      logger.info('pgboss stopped');
    },
    bam: (event) => {
      const message = `pgboss BAM: ${event.name} ${event.status}`;
      if (event.status === 'failed') logger.error(message, event.error);
      else logger.info(message);
    },
    flow: (event) => {
      logger.debug('pgboss flow', event);
    },
  };
  for (const event of events) client.on(event, handlers[event]);
  if (!events.includes('error')) client.on('error', () => {});
};

class Pgboss {
  constructor(config = {}, logger = console) {
    const { enabled = false, logEvents, ...options } = config;
    this.enabled = enabled;
    this.logEvents = Object.freeze(getLogEvents(logEvents));
    this.config = options;
    this.console = logger;
    this.client = null;
    this.state = 'idle';
    this.operation = null;
  }

  async start() {
    if (!this.enabled) return null;
    if (this.state === 'running') return this.client;
    if (this.state === 'starting') return this.operation;
    if (this.state === 'stopping') {
      await this.operation;
      return this.start();
    }

    this.state = 'starting';
    const start = loadConfig(this.config).then(async (config) => {
      const client = new npm.pgboss.PgBoss(config);
      watchEvents(client, this.console, this.logEvents);
      this.client = client;
      await client.start();
      return client;
    });
    this.operation = start.then(
      (client) => {
        this.state = 'running';
        this.operation = null;
        return client;
      },
      (error) => {
        this.client = null;
        this.state = 'idle';
        this.operation = null;
        throw error;
      },
    );
    return this.operation;
  }

  async stop(timeoutMs) {
    if (this.state === 'idle') return;
    if (this.state === 'stopping') {
      await this.operation;
      return;
    }
    if (this.state === 'starting') {
      try {
        await this.operation;
      } catch {
        return;
      }
      await this.stop(timeoutMs);
      return;
    }

    const client = this.client;
    this.state = 'stopping';
    const options = { graceful: true, timeout: timeoutMs };
    const stop = Promise.resolve().then(() => client.stop(options));
    this.operation = stop.then(
      () => {
        this.client = null;
        this.state = 'idle';
        this.operation = null;
      },
      (error) => {
        this.state = 'running';
        this.operation = null;
        const details = error?.stack || error?.message || String(error);
        this.console.error('Can not stop pgboss', details);
      },
    );
    await this.operation;
  }
}

module.exports = { Pgboss, getPgbossConfig, watchEvents };
