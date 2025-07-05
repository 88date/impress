'use strict';

const { metarhia } = require('./deps.js');
const { checkRoles } = require('./role.js');
const { Error, DomainError } = metarhia.metautil;

const EMPTY_CONTEXT = Object.freeze({});

class Procedure {
  constructor(script, methodName, application) {
    const exp = script(EMPTY_CONTEXT);
    this.exports = exp;
    this.script = script;
    this.methodName = methodName;
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
    this.locks = new Map();
    this.caption = exp.caption || '';
    this.description = exp.description || '';
    this.access = exp.access || '';
    this.validate = exp.validate || null;
    this.roles = exp.roles || null;
    this.segments = exp.segments || null;
    const { timeouts } = application.config.server;
    this.timeout = (exp.timeout ?? timeouts.request) || 0;
    this.serializer = exp.serialize || null;
    this.protocols = exp.protocols || null;
    this.deprecated = exp.deprecated || false;
    this.assert = exp.assert || null;
    this.examples = exp.examples || null;
  }

  async enter(ip) {
    await this.application.semaphore.enter();

    if (this.exports.queue) {
      if (!this.locks.has(ip)) {
        const { concurrency, size, timeout } = this.exports.queue;
        const { Semaphore } = metarhia.metautil;
        this.locks.set(ip, new Semaphore({ concurrency, size, timeout }));
      }

      try {
        const semaphore = this.locks.get(ip);
        await semaphore.enter();
      } catch (error) {
        this.application.semaphore.leave();
        throw error;
      }
    }
  }

  leave(ip) {
    this.application.semaphore.leave();
    if (this.exports.queue) {
      const semaphore = this.locks.get(ip);
      if (semaphore) semaphore.leave();
      if (semaphore.queue.length === 0) this.locks.delete(ip);
    }
  }

  async invoke(context, args = {}) {
    const { parameters, validate, returns, errors, timeout, roles } = this;
    const exp = this.script(context);
    const method = typeof exp === 'object' ? exp[this.methodName] : exp;
    if (parameters) {
      const { valid, errors } = parameters.check(args);
      const problems = errors.join('; ');
      if (!valid) return new Error('Invalid parameters type: ' + problems);
    }
    if (validate) await validate(args);
    if (roles) {
      const allowed = checkRoles(context, roles);
      if (!allowed) return new Error('Forbidden', { code: 400 });
    }
    let promise = method(args);
    if (timeout) promise = metarhia.metautil.timeoutify(promise, timeout);
    let result = await promise;
    if (metarhia.metautil.isError(result)) {
      if (result instanceof DomainError) result = result.toError(errors);
      return result;
    }
    if (returns) {
      const { valid, errors } = this.returns.check(result);
      const problems = errors.join('; ');
      if (!valid) return new Error('Invalid result type: ' + problems);
    }
    return result;
  }
}

module.exports = { Procedure };
