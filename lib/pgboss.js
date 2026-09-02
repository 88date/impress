'use strict';

const { node, npm } = require('./deps.js');

const loadConfig = async (config) => {
  const { ssl } = config;
  if (!ssl?.caPath) return config;
  const { caPath, ...options } = ssl;
  const ca = await node.fsp.readFile(caPath, 'utf8');
  return { ...config, ssl: { ...options, ca } };
};

const watchEvents = (client, logger) => {
  client.on('error', (error) => {
    const details = error?.stack || error?.message || String(error);
    logger.error('pgboss error', details);
  });

  client.on('warning', ({ message, data }) => {
    logger.warn(`pgboss warning: ${message}`, data);
  });

  client.on('wip', (workers) => {
    logger.debug('pgboss workers', workers);
  });

  client.on('stopped', () => {
    logger.info('pgboss stopped');
  });

  client.on('bam', (event) => {
    const message = `pgboss BAM: ${event.name} ${event.status}`;
    if (event.status === 'failed') logger.error(message, event.error);
    else logger.info(message);
  });

  client.on('flow', (event) => {
    logger.debug('pgboss flow', event);
  });
};

class Pgboss {
  constructor(config = {}, logger = console) {
    const { enabled = false, ...options } = config;
    this.enabled = enabled;
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
      watchEvents(client, this.console);
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

module.exports = { Pgboss };
