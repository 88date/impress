import { EventEmitter } from 'node:events';
import { IncomingMessage, ServerResponse } from 'node:http';

export interface Task {
  name: string;
  every: string;
  args: object;
  run: string;
}

export interface Scheduler {
  add(task: Task): Promise<string>;
  remove(id: string): void;
  stop(name: string): void;
}

export interface InvokeTarget {
  method: string;
  args: object;
  exclusive?: boolean;
}

export interface Static {
  name: string;
  path: string;
  files: Map<string, { data: Buffer | null; stat: object | null }>;
  get(name: string): { data: Buffer | null; stat: object | null } | undefined;
  find(
    path: string,
    code?: number,
  ): { data: Buffer | null; stat: object | null; code: number };
  serve(url: string, transport: object): Promise<void>;
  load(targetPath?: string): Promise<void>;
  delete(filePath: string): void;
  change(filePath: string): Promise<void>;
}

export interface Schemas {
  model: object;
  get(name: string): unknown;
  load(targetPath?: string): Promise<void>;
  delete(filePath: string): void;
  change(filePath: string): Promise<void>;
}

export interface Listener {
  (...args: Array<unknown>): void;
}

export interface Context {
  client: Client;
  token?: string;
  session?: object;
}

export interface Client {
  ip: string;
  session?: object;
  emit(name: string, data: object): void;
}

export interface DocumentationEndpoint {
  caption?: string;
  description?: string;
  protocols?: unknown;
  roles?: unknown;
  access?: string;
  parameters?: unknown;
  deprecated?: boolean;
  returns?: unknown;
  errors?: unknown;
  example: unknown;
}

export type DocumentationMethods = Record<string, DocumentationEndpoint>;
export type DocumentationVersions = Record<string, DocumentationMethods>;
export type DocumentationUnits = Record<string, DocumentationVersions>;

export interface ApplicationDocumentation {
  api: DocumentationUnits;
  services: DocumentationUnits;
  schemas: Record<string, unknown>;
  queues: Record<string, unknown>;
}

export interface Application extends EventEmitter {
  worker: { id: string };
  server: { host: string; port: number; protocol: string };
  resources: Static;
  schemas: Schemas;
  scheduler: Scheduler;
  mode: string;
  introspect: (units: Array<string>) => Promise<object>;
  getDocumentation: () => Promise<ApplicationDocumentation>;
  invoke: (target: InvokeTarget) => Promise<unknown>;
  on(event: 'loading', listener: Listener): this;
  once(event: 'loading', listener: Listener): this;
  on(event: 'loaded', listener: Listener): this;
  once(event: 'loaded', listener: Listener): this;
  on(event: 'started', listener: Listener): this;
  once(event: 'started', listener: Listener): this;
  on(event: 'ready', listener: Listener): this;
  once(event: 'ready', listener: Listener): this;
}
