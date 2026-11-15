'use strict';

const { sep, basename } = require('node:path');
const { node, metarhia } = require('../deps.js');
const { Place } = require('../place.js');

const contractNames = (directory, filePath, kind) => {
  if (!filePath.endsWith('.js')) return null;
  const relative = node.path.relative(directory, filePath);
  const parts = relative.split(sep);
  if (parts.length < 3 || parts.some((part) => part.startsWith('.'))) {
    return null;
  }
  const moduleName = parts.shift();
  const place = parts.shift();
  if (place !== 'events' && place !== 'subscribers') return null;
  if (kind && kind !== place) return null;
  const fileName = basename(parts.pop(), '.js');
  const match = /^([^.]+)(?:\.([1-9]\d*))?$/.exec(fileName);
  if (!match) throw new Error(`Invalid declaration filename: ${filePath}`);
  const [, eventName, version = '1'] = match;
  parts.push(eventName);
  const name = `${moduleName}:${version}:${parts.join(':')}`;
  const subject = [moduleName, version, ...parts].join('.');
  const path = [moduleName, version, ...parts].join('/');
  return { name, subject, path };
};

const EMPTY_CONTEXT = Object.freeze({});

class DeclarationLoader extends Place {
  constructor(name, application) {
    super(name, application);
    this.path = application.absolute('modules');
    this.revisions = new Map();
    this.sources = new Map();
  }

  nextRevision(filePath) {
    const revision = (this.revisions.get(filePath) || 0) + 1;
    this.revisions.set(filePath, revision);
    return revision;
  }

  async read(filePath) {
    try {
      const names = contractNames(this.path, filePath, this.name);
      if (!names) return null;
      const revision = this.nextRevision(filePath);
      const code = await node.fsp.readFile(filePath, 'utf8');
      if (!code.trim()) throw new Error('Empty declaration');
      const options = { context: this.application.sandbox };
      const { MetaScript } = metarhia.metavm;
      const script = new MetaScript(filePath, 'context => ' + code, options);
      const declaration = script.exports(EMPTY_CONTEXT);
      const contract = this.constructor.compile(declaration, names);
      return { filePath, revision, name: names.name, contract };
    } catch (cause) {
      throw new Error(`Cannot load declaration: ${filePath}`, { cause });
    }
  }

  async collect(targetPath, declarations) {
    const relative = node.path.relative(this.path, targetPath);
    const parts = relative ? relative.split(sep) : [];
    if (parts.some((part) => part.startsWith('.'))) return;
    if (parts.length >= 2 && parts[1] !== this.name) return;
    this.application.watcher.watch(targetPath);
    const files = await node.fsp.readdir(targetPath, { withFileTypes: true });
    for (const file of files) {
      if (file.name.startsWith('.')) continue;
      const filePath = node.path.join(targetPath, file.name);
      if (file.isDirectory()) {
        if (parts.length === 1 && file.name !== this.name) continue;
        await this.collect(filePath, declarations);
      } else {
        const declaration = await this.read(filePath);
        if (declaration) declarations.push(declaration);
      }
    }
  }

  validate(declaration, sources = this.sources) {
    const { name, filePath } = declaration;
    const source = sources.get(name);
    if (source && source !== filePath) {
      const message = `Duplicate declaration: ${name} (${source}, ${filePath})`;
      throw new Error(message);
    }
    sources.set(name, filePath);
  }

  async apply(declaration) {
    if (!declaration) return;
    const { filePath, revision, contract } = declaration;
    if (this.revisions.get(filePath) !== revision) return;
    this.validate(declaration);
    await this.register(contract);
  }

  async load(targetPath = this.path) {
    await metarhia.metautil.ensureDirectory(this.path);
    const declarations = [];
    await this.collect(targetPath, declarations);
    const sources = new Map(this.sources);
    for (const declaration of declarations) this.validate(declaration, sources);
    for (const declaration of declarations) await this.apply(declaration);
  }

  async change(filePath) {
    try {
      await this.apply(await this.read(filePath));
    } catch (error) {
      this.application.console.error(error);
    }
  }

  async delete(filePath) {
    const prefix = filePath + sep;
    for (const source of this.revisions.keys()) {
      if (source === filePath || source.startsWith(prefix)) {
        this.nextRevision(source);
      }
    }
    const removals = [];
    for (const [name, source] of this.sources) {
      if (source !== filePath && !source.startsWith(prefix)) continue;
      this.sources.delete(name);
      removals.push(this.unregister(name));
    }
    await Promise.all(removals);
  }
}

class EventLoader extends DeclarationLoader {
  constructor(application, subscriptions, onChange = () => {}) {
    super('events', application);
    this.subscriptions = subscriptions;
    this.collection = subscriptions.events;
    this.onChange = onChange;
  }

  static compile(declaration, names) {
    return {
      ...declaration,
      eventName: names.name,
      eventSubject: `events.${names.subject}`,
    };
  }

  async register(contract) {
    await this.subscriptions.registerEvent(contract);
    this.onChange();
  }

  async unregister(name) {
    await this.subscriptions.unregisterEvent(name);
    this.onChange();
  }
}

class SubscriberLoader extends DeclarationLoader {
  constructor(application, subscriptions) {
    super('subscribers', application);
    this.subscriptions = subscriptions;
    this.collection = subscriptions.subscribers;
  }

  static compile(declaration, names) {
    const { stream, subject, method, ...options } = declaration || {};
    if (typeof stream !== 'string' || !stream) {
      throw new Error('Subscriber stream name expected');
    }
    if (typeof subject !== 'string' || !subject) {
      throw new Error('Subscriber subject expected');
    }
    if (typeof method !== 'function') {
      throw new Error('Subscriber method expected');
    }
    return {
      stream,
      subject,
      maxMessages: options.maxMessages,
      maxAckPending: options.maxAckPending,
      retryLimit: options.retryLimit,
      retryDelay: options.retryDelay,
      timeout: options.timeout,
      deliverPolicy: options.deliverPolicy,
      startSequence: options.startSequence,
      startTime: options.startTime,
      method,
      subscriberName: names.name,
      subscriberPath: names.path,
    };
  }

  register(contract) {
    return this.subscriptions.registerSubscriber(contract);
  }

  unregister(name) {
    return this.subscriptions.removeSubscriber(name);
  }
}

module.exports = { EventLoader, SubscriberLoader, contractNames };
