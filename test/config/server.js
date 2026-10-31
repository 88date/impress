({
  host: '::',
  balancer: 8000,
  protocol: 'http',
  ports: [8001, 8002],
  nagle: true,
  timeouts: {
    bind: 2000,
    start: 30000,
    stop: 5000,
    request: 5000,
    watch: 1000,
    test: 10000,
  },
  queue: {
    concurrency: 1000,
    size: 2000,
    timeout: 3000,
  },
  workers: {
    pool: 2,
    wait: 2000,
    timeout: 5000,
  },
  scheduler: {
    enabled: false,
    active: false,
    stream: 'TASKS',
  },
  events: false,
  nats: {
    enabled: false,
    servers: ['nats://127.0.0.1:4222'],
    credentials: 'test.creds',
    jetstream: {
      stream: 'EVENTS',
      consumerPrefix: 'impress',
      replicas: 1,
    },
    discovery: {
      maxWait: 1000,
    },
  },
  centrifugo: {
    secret: 'test-secret',
  },
  cors: {
    origin: '*',
  },
});
