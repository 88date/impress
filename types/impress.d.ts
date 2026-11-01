import type { DeliverPolicy } from '@nats-io/jetstream';
import type { MsgHdrs } from '@nats-io/transport-node';
import {
  LogConfig,
  ScaleConfig,
  ServerConfig,
  SessionsConfig,
  CacheConfig,
} from './config';
import { Application, Context, Client } from './core';
import { Services } from './service';

import * as _util from 'node:util';
import * as _buffer from 'node:buffer';
import * as _cp from 'node:child_process';
import * as _os from 'node:os';
import * as _v8 from 'node:v8';
import * as _vm from 'node:vm';
import * as _path from 'node:path';
import * as _url from 'node:url';
import * as _sd from 'node:string_decoder';
import * as _qs from 'node:querystring';
import * as _querystring from 'node:querystring';
import * as _assert from 'node:assert';
import * as _stream from 'node:stream';
import * as _fs from 'node:fs';
import * as _crypto from 'node:crypto';
import * as _zlib from 'node:zlib';
import * as _readline from 'node:readline';
import * as _ph from 'node:perf_hooks';
import * as _ah from 'node:async_hooks';
import * as _timers from 'node:timers';
import * as _events from 'node:events';
import * as _dns from 'node:dns';
import * as _net from 'node:net';
import * as _tls from 'node:tls';
import * as _http from 'node:http';
import * as _https from 'node:https';
import * as _http2 from 'node:http2';
import * as _dgram from 'node:dgram';

import * as _ws from 'ws';

import * as _config from 'metaconfiguration';
import * as _metautil from 'metautil';
import * as _metavm from 'metavm';
import * as _metacom from 'metacom';
import * as _metalog from 'metalog';
import * as _metawatch from 'metawatch';
import * as _metaschema from 'metaschema';
import * as _metaconfiguration from 'metaconfiguration';

declare global {
  interface EventMetadata {
    id: string;
    name: string;
    createdAt: string;
  }

  interface SubscriberContext {
    signal: AbortSignal;
    headers?: MsgHdrs;
  }

  type EventDeclaration<Data = Record<string, unknown>> = {
    caption?: string;
    description?: string;
    examples?: Data[];
  };

  type SubscriberDeclaration<Message = unknown> = {
    stream: string;
    subject: string;
    /** Initial delivery settings apply only when creating a consumer. */
    deliverPolicy?: DeliverPolicy;
    startSequence?: number;
    startTime?: string;
    /** JetStream receive buffer size, not a handler limit. Defaults to 5. */
    maxMessages?: number;
    /**
     * Unacknowledged messages across all workers of this consumer.
     * Use a positive integer for a limit or -1 for unlimited delivery.
     * Applied by server.events instances on creation and connection.
     * Omit to preserve an existing limit; new consumers default to -1.
     */
    maxAckPending?: number;
    /** Redeliveries for a newly created consumer. Defaults to 2. */
    retryLimit?: number;
    /** Delay after a failed handler in milliseconds. Defaults to 0. */
    retryDelay?: number;
    /** Handler timeout in milliseconds. Defaults to 30000. */
    timeout?: number;
    method: (
      message: Message,
      context: SubscriberContext,
    ) => unknown | Promise<unknown>;
  };

  interface EventDatabase {
    query(text: string, values: unknown[]): PromiseLike<unknown>;
  }

  interface EventConfiguration {
    database: EventDatabase;
    schema?: string;
    table?: string;
  }

  interface EventEmitOptions {
    transaction?: EventDatabase;
  }

  interface Events {
    configure(options: EventConfiguration): void;
    emit(
      name: string,
      data: unknown,
      options?: EventEmitOptions,
    ): Promise<string>;
  }

  interface TaskJob<Data = Record<string, unknown>> {
    /** Stream and message sequence; unchanged on redelivery. */
    id: string;
    name: string;
    data: Data;
    createdAt: string;
    retryCount: number;
  }

  type TaskDeclaration<Data = Record<string, unknown>, Result = unknown> = {
    /** Five fields (minutes first), or six fields (seconds first). */
    cron: string;
    /** IANA time zone. Defaults to UTC. */
    tz?: string;
    data?: Data;
    /** JetStream receive buffer size, not a handler limit. Defaults to 5. */
    maxMessages?: number;
    /** Redeliveries after failure or missing ACK. Defaults to 2. */
    retryLimit?: number;
    /** Delay after failure in milliseconds. Defaults to 0. */
    retryDelay?: number;
    /** Execution deadline in milliseconds, including callbacks. */
    timeout?: number;
    method: (data: Data, job: TaskJob<Data>) => Result | Promise<Result>;
    onCompleted?: (result: Result, job: TaskJob<Data>) => void | Promise<void>;
    onFailed?: (reason: string, job: TaskJob<Data>) => void | Promise<void>;
  };

  const application: Application;
  const context: Context;
  const events: Events;
  const service: Services;
  const tasks: Record<string, TaskDeclaration>;

  namespace config {
    const log: LogConfig;
    const scale: ScaleConfig;
    const server: ServerConfig;
    const sessions: SessionsConfig;
    const cache: CacheConfig;
  }

  namespace metarhia {
    const config: typeof _config;
    const metautil: typeof _metautil;
    const metavm: typeof _metavm;
    const metacom: typeof _metacom;
    const metalog: typeof _metalog;
    const metawatch: typeof _metawatch;
    const metaschema: typeof _metaschema;
    const metaconfiguration: typeof _metaconfiguration;
  }

  namespace node {
    const util: typeof _util;
    const buffer: typeof _buffer;
    const child_process: typeof _cp;
    const childProcess: typeof _cp;
    const os: typeof _os;
    const v8: typeof _v8;
    const vm: typeof _vm;
    const path: typeof _path;
    const url: typeof _url;
    const StringDecoder: typeof _sd;
    const querystring: typeof _qs;
    const assert: typeof _assert;
    const stream: typeof _stream;
    const fs: typeof _fs;
    const fsp: typeof _fs.promises;
    const crypto: typeof _crypto;
    const zlib: typeof _zlib;
    const readline: typeof _readline;
    const perf_hooks: typeof _ph;
    const perfHooks: typeof _ph;
    const async_hooks: typeof _ah;
    const asyncHooks: typeof _ah;
    const timers: typeof _timers;
    const events: typeof _events;
    const dns: typeof _dns;
    const net: typeof _net;
    const tls: typeof _tls;
    const http: typeof _http;
    const https: typeof _https;
    const http2: typeof _http2;
    const dgram: typeof _dgram;
  }

  namespace npm {
    const ws: typeof _ws;
  }
}
