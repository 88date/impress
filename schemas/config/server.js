({
  host: 'string',
  balancer: '?number',
  protocol: { enum: ['http', 'https'] },
  ports: { array: 'number' },
  nagle: 'boolean',
  timeouts: {
    bind: 'number',
    start: 'number',
    stop: 'number',
    request: 'number',
    watch: 'number',
    test: 'number',
  },
  queue: {
    concurrency: 'number',
    size: 'number',
    timeout: 'number',
  },
  workers: {
    pool: 'number',
    wait: 'number',
    timeout: 'number',
  },
  scheduler: {
    enabled: 'boolean',
    active: 'boolean',
    notify: '?boolean',
  },
  pubsub: {
    active: 'boolean',
  },
  nats: {
    enabled: 'boolean',
    servers: '?string',
    credentials: '?string',
    discovery: {
      maxWait: 'number',
    },
  },
  pgboss: 'json',
  centrifugo: {
    secret: '?string',
  },
  cors: {
    origin: '?string',
  },
});
