import type {
  ConstructorOptions as PgbossOptions,
  PgBossEventMap,
} from 'pg-boss';

export interface LogConfig {
  keepDays: number;
  writeInterval: number;
  writeBuffer: number;
  toFile: Array<string>;
  toStdout: Array<string>;
  json?: boolean;
}

export interface ScaleConfig {
  cloud: string;
  server: string;
  instance: 'standalone' | 'controller' | 'server';
  token: string;
  gc: number;
}

export interface ServerConfig {
  host: string;
  balancer: number;
  protocol: 'http' | 'https';
  ports: Array<number>;
  nagle: boolean;
  timeouts: {
    bind: number;
    start: number;
    stop: number;
    request: number;
    watch: number;
    test: number;
  };
  queue: {
    concurrency: number;
    size: number;
    timeout: number;
  };
  workers: {
    pool: number;
    wait: number;
    timeout: number;
  };
  scheduler: SchedulerConfig;
  pubsub: PubSubConfig;
  nats: NatsConfig;
  pgboss: PgbossConfig;
  centrifugo: {
    secret?: string;
  };
  cors?: {
    origin: string;
  };
}

export interface CacheConfig {
  size: string;
  maxFileSize: string;
  avoid?: Array<string>;
}

export interface NatsConfig {
  enabled: boolean;
  servers?: string;
  credentials?: string;
  discovery: {
    maxWait: number;
  };
}

export interface SchedulerConfig {
  enabled: boolean;
  active: boolean;
  /** Enable LISTEN/NOTIFY for tasks; false uses polling. Defaults to false. */
  notify?: boolean;
}

export interface PubSubConfig {
  /** Make this instance the pg-boss subscriber topology manager. */
  active: boolean;
}

export type PgbossConfig = PgbossOptions & {
  enabled: boolean;
  /** pg-boss events written to the application log. */
  logEvents?: Array<keyof PgBossEventMap>;
};

export interface SessionsConfig {
  sid: string;
  characters: string;
  length: number;
  secret: string;
  regenerate: number;
  expire: number;
  persistent: boolean;
  limits: {
    ip: number;
    user: number;
  };
}
