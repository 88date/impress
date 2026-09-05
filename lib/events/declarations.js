'use strict';

const { sep, basename } = require('node:path');
const { node, metarhia } = require('../deps.js');
const { Place } = require('../place.js');

const splitEventName = (name) => {
  let result = '';
  for (let index = 0; index < name.length; index++) {
    const char = name[index];
    const previous = name[index - 1];
    const upperCase = char >= 'A' && char <= 'Z';
    const previousLowerCase = previous >= 'a' && previous <= 'z';
    const previousDigit = previous >= '0' && previous <= '9';
    if (upperCase && (previousLowerCase || previousDigit)) result += ':';
    result += char.toLowerCase();
  }
  return result;
};

const contractNames = (directory, filePath) => {
  if (!filePath.endsWith('.js')) return null;
  const relative = filePath.substring(directory.length + 1);
  const parts = relative.split(sep);
  if (parts.length < 2) return null;
  const unitName = parts.shift();
  const [service, version = '1'] = unitName.split('.');
  const fileName = parts.pop();
  parts.push(basename(fileName, '.js'));
  const event = parts.map(splitEventName).join(':');
  const name = `${service}:${version}:${event}`;
  const subject = [service, version, ...event.split(':')].join('.');
  const path = [service, version, ...parts].join('/');
  return { name, subject, path };
};

const EMPTY_CONTEXT = Object.freeze({});

class DeclarationLoader extends Place {
  constructor(name, application) {
    super(name, application);
    this.revisions = new Map();
  }

  nextRevision(filePath) {
    const revision = (this.revisions.get(filePath) || 0) + 1;
    this.revisions.set(filePath, revision);
    return revision;
  }

  async read(filePath) {
    const names = contractNames(this.path, filePath);
    if (!names) return null;
    const revision = this.nextRevision(filePath);
    try {
      const code = await node.fsp.readFile(filePath, 'utf8');
      if (!code.trim()) throw new Error('Empty declaration');
      const options = { context: this.application.sandbox };
      const { MetaScript } = metarhia.metavm;
      const script = new MetaScript(filePath, 'context => ' + code, options);
      const declaration = script.exports(EMPTY_CONTEXT);
      const contract = this.constructor.compile(declaration, names);
      return { filePath, revision, contract };
    } catch (cause) {
      throw new Error(`Cannot load declaration: ${filePath}`, { cause });
    }
  }

  async collect(targetPath, declarations) {
    this.application.watcher.watch(targetPath);
    const files = await node.fsp.readdir(targetPath, { withFileTypes: true });
    for (const file of files) {
      if (file.name.startsWith('.')) continue;
      const filePath = node.path.join(targetPath, file.name);
      if (file.isDirectory()) {
        await this.collect(filePath, declarations);
      } else {
        const declaration = await this.read(filePath);
        if (declaration) declarations.push(declaration);
      }
    }
  }

  async apply(declaration) {
    if (!declaration) return;
    const { filePath, revision, contract } = declaration;
    if (this.revisions.get(filePath) !== revision) return;
    await this.register(contract);
  }

  async load(targetPath = this.path) {
    await metarhia.metautil.ensureDirectory(this.path);
    const declarations = [];
    await this.collect(targetPath, declarations);
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
    this.nextRevision(filePath);
    const names = contractNames(this.path, filePath);
    if (names) await this.unregister(names.name);
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
    const transports = declaration?.transports;
    if (
      !Array.isArray(transports) ||
      transports.some((name) => name !== 'local' && name !== 'nats')
    ) {
      throw new Error('Event transports must contain local or nats');
    }
    return {
      ...declaration,
      eventName: names.name,
      eventSubject: names.subject,
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
    const { event, method, ...options } = declaration || {};
    if (typeof event !== 'string' || !event) {
      throw new Error('Subscriber event name expected');
    }
    if (typeof method !== 'function') {
      throw new Error('Subscriber method expected');
    }
    return {
      concurrency: options.concurrency,
      retryLimit: options.retryLimit,
      retryDelay: options.retryDelay,
      timeout: options.timeout,
      method,
      subscriberName: names.name,
      subscriberPath: names.path,
      eventName: event,
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
