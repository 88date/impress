'use strict';

const { node, metarhia } = require('./deps.js');
const { Error, DomainError } = metarhia.metautil;
const { EventEmitter } = node.events;

const EMPTY_CONTEXT = Object.freeze({});

class Broker {
  constructor(script, methodName, unitName, application) {
    const exp = script(EMPTY_CONTEXT);
    this.exports = exp;
    this.script = script;
    this.methodName = methodName;
    this.unitName = unitName;
    this.actionName = methodName;
    this.application = application;
    this.method = null;
    if (typeof exp === 'object') this.method = exp[methodName];
    else if (typeof exp === 'function') this.method = exp;
    const namespaces = application.schemas ? [application.schemas.model] : [];
    const { parameters, returns, errors } = exp;
    const { Schema } = metarhia.metaschema;
    this.parameters = parameters ? Schema.from(parameters, namespaces) : null;
    this.returns = returns ? Schema.from(returns, namespaces) : null;
    this.errors = errors || null;
    this.caption = exp.caption || '';
    this.description = exp.description || '';
    this.access = exp.access || '';
    this.validate = exp.validate || null;
    this.deprecated = exp.deprecated || false;
    this.examples = exp.examples || null;
  }

  get serviceName() {
    const index = this.unitName.lastIndexOf('.');
    return this.unitName.substring(0, index);
  }

  get version() {
    const index = this.unitName.lastIndexOf('.');
    return parseInt(this.unitName.substring(index + 1), 10);
  }

  get config() {
    const { configs } = this.application.service;
    return configs[`${this.serviceName}.1`] || configs[this.unitName];
  }

  get requestVersion() {
    const { versions } = this.config;
    return versions?.[this.actionName] ?? versions?.default ?? 1;
  }

  get subject() {
    return `${this.serviceName}.${this.version}.${this.actionName}`;
  }

  get requestSubject() {
    return `${this.serviceName}.${this.requestVersion}.${this.actionName}`;
  }

  get requestName() {
    return `${this.serviceName}.${this.requestVersion}/${this.actionName}`;
  }

  checkAccess(context) {
    if (this.access === 'public' || context?.session) return;
    throw new Error('Authentication required');
  }

  call(args = {}) {
    const context = this.application.contextStorage.getStore();
    const { location, request } = this.config;
    if (location === 'local') {
      const unit = this.application.service.collection[this.serviceName];
      const methods = unit[this.requestVersion.toString()];
      const broker = methods?.[this.actionName];
      if (!broker) {
        throw new Error(
          `Service action is not available: ${this.requestSubject}`,
        );
      }
      broker.checkAccess(context);
      return this.trace(context, broker.execute(context, args));
    }
    this.checkAccess(context);
    return this.trace(
      context,
      this.application.nats.request(this.requestSubject, args, request.timeout),
    );
  }

  async trace(context, promise) {
    const ip = context?.client?.ip || '-';
    try {
      const result = await promise;
      this.application.console.log(`${ip}\t${this.requestName}`);
      return result;
    } catch (error) {
      const code = error.code || 500;
      const httpCode = typeof code === 'string' ? 200 : 500;
      const reason = `${httpCode}\t${code}\t${error.stack}`;
      this.application.console.error(
        `${ip}\tservice\t${this.requestName}\t${reason}`,
      );
      throw error;
    }
  }

  async execute(context, args = {}) {
    const exp = this.script(context);
    const method = typeof exp === 'object' ? exp[this.methodName] : exp;
    try {
      return await method(args);
    } catch (error) {
      if (error instanceof DomainError) throw error.toError(this.errors);
      throw error;
    }
  }

  async invoke(context, args = {}) {
    this.checkAccess(context);
    const { parameters, validate, returns } = this;
    if (parameters) {
      const { valid, errors } = parameters.check(args);
      const problems = errors.join('; ');
      if (!valid) throw new Error('Invalid parameters type: ' + problems);
    }
    if (validate) {
      try {
        await validate(args);
      } catch (error) {
        if (error instanceof DomainError) throw error.toError(this.errors);
        throw new Error(error.message, { cause: error });
      }
    }
    const result = await this.execute(context, args);
    if (returns) {
      const { valid, errors } = this.returns.check(result);
      const problems = errors.join('; ');
      if (!valid) throw new Error('Invalid result type: ' + problems);
    }
    return result;
  }
}

class EventBroker extends EventEmitter {
  constructor(name, application) {
    super();
    this.name = name;
    this.application = application;
    this.collection = {};
    this.indexes = new Map();
  }

  load(events) {
    const namespaces = this.application.schemas
      ? [this.application.schemas.model]
      : [];
    const { Schema } = metarhia.metaschema;
    const collection = {};
    for (const [eventName, event] of Object.entries(events)) {
      const { parameters } = event;
      collection[eventName] = {
        exports: event,
        parameters: parameters ? Schema.from(parameters, namespaces) : null,
        caption: event.caption || '',
        description: event.description || '',
        deprecated: event.deprecated || false,
        examples: event.examples || null,
      };
    }
    this.collection = collection;
  }

  validate(eventName, payload) {
    const event = this.collection[eventName];
    if (!event) {
      const name = `${this.name}:${eventName}`;
      throw new Error(`Service event is not available: ${name}`);
    }
    if (!event.parameters) return;
    const { valid, errors } = event.parameters.check(payload);
    const problems = errors.join('; ');
    if (!valid) throw new Error('Invalid event parameters: ' + problems);
  }

  async emit(eventName, payload) {
    const name = `${this.name}:${eventName}`;
    if (this.application.nats) {
      this.validate(eventName, payload);
      this.application.nats.publishEvent(name, payload);
      return;
    }
    const calls = [];
    for (const broker of Object.values(this.application.service.events)) {
      if (broker.listenerCount(name) > 0) {
        calls.push(broker.dispatch(name, payload));
      }
    }
    await Promise.all(calls);
  }

  on(eventName, handler) {
    super.on(eventName, handler);
    if (this.application.nats) {
      this.application.nats.subscribeEvent(this, eventName);
    }
    return this;
  }

  async invoke(eventName, payload) {
    const separator = eventName.indexOf(':');
    const sourceName = eventName.substring(0, separator);
    const name = eventName.substring(separator + 1);
    const source = this.application.service.events[sourceName];
    if (!source) {
      throw new Error(`Service event is not available: ${eventName}`);
    }
    source.validate(name, payload);
    await this.dispatch(eventName, payload);
  }

  async dispatch(eventName, payload) {
    const handlers = this.listeners(eventName);
    if (handlers.length === 0) return;
    const index = this.indexes.get(eventName) || 0;
    const handler = handlers[index];
    this.indexes.set(eventName, (index + 1) % handlers.length);
    try {
      const invoke = () => handler(payload);
      await this.application.contextStorage.run(null, invoke);
    } catch (error) {
      this.application.console.error(error);
    }
  }
}

module.exports = { Broker, EventBroker };
