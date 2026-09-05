'use strict';

const { metarhia } = require('./deps.js');
const { Error, DomainError } = metarhia.metautil;

const EMPTY_CONTEXT = Object.freeze({});

class Broker {
  constructor(script, methodName, unitName, application, contract = null) {
    const exp = contract || script(EMPTY_CONTEXT);
    this.exports = exp;
    this.script = script;
    this.methodName = methodName;
    this.unitName = unitName;
    this.actionName = methodName;
    this.application = application;
    this.discovered = contract !== null;
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
    const { timeouts } = application.config.server;
    this.timeout = (exp.timeout ?? timeouts.request) || 0;
    this.transports = exp.transports || [];
    this.deprecated = exp.deprecated || false;
    this.examples = exp.examples || null;
  }

  static fromContract(contract, unitName, application) {
    return new Broker(null, contract.name, unitName, application, contract);
  }

  describe() {
    const exp = this.exports;
    return {
      name: this.actionName,
      version: this.version,
      access: this.access,
      parameters: exp.parameters || null,
      returns: exp.returns || null,
      errors: this.errors,
      caption: this.caption,
      description: this.description,
      timeout: this.timeout,
      transports: this.transports,
      deprecated: this.deprecated,
      examples: this.examples,
    };
  }

  get serviceName() {
    const index = this.unitName.lastIndexOf('.');
    return this.unitName.substring(0, index);
  }

  get version() {
    const index = this.unitName.lastIndexOf('.');
    return parseInt(this.unitName.substring(index + 1), 10);
  }

  get subject() {
    return `${this.serviceName}.${this.version}.${this.actionName}`;
  }

  get requestName() {
    return `${this.serviceName}.${this.version}/${this.actionName}`;
  }

  checkAccess(context) {
    if (this.access === 'public' || context?.session) return;
    throw new Error('Authentication required');
  }

  call(args = {}) {
    const context = this.application.contextStorage.getStore();
    if (this.method) {
      return this.invoke(context, args, false);
    }
    this.checkAccess(context);
    return this.application.nats.request(this.subject, args, this.timeout);
  }

  log(context, error = null) {
    const ip = context?.client?.ip || context?.ip || '-';
    if (error) {
      const code = error.code || 500;
      const httpCode = typeof code === 'string' ? 200 : 500;
      const reason = `${httpCode}\t${code}\t${error.stack}`;
      const message = `${ip}\tservice\t${this.requestName}\t${reason}`;
      this.application.console.error(message);
    } else {
      const message = `${ip}\tservice\t${this.requestName}`;
      this.application.console.log(message);
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

  async invoke(context, args = {}, shouldValidate = true) {
    try {
      this.checkAccess(context);
      const { parameters, validate, returns } = this;
      if (shouldValidate && parameters) {
        const { valid, errors } = parameters.check(args);
        const problems = errors.join('; ');
        if (!valid) throw new Error('Invalid parameters type: ' + problems);
      }
      if (shouldValidate && validate) {
        try {
          await validate(args);
        } catch (error) {
          if (error instanceof DomainError) throw error.toError(this.errors);
          throw new Error(error.message, { cause: error });
        }
      }
      let promise = this.execute(context, args);
      if (this.timeout) {
        promise = metarhia.metautil.timeoutify(promise, this.timeout);
      }
      const result = await promise;
      if (shouldValidate && returns) {
        const { valid, errors } = this.returns.check(result);
        const problems = errors.join('; ');
        if (!valid) throw new Error('Invalid result type: ' + problems);
      }
      this.log(context);
      return result;
    } catch (error) {
      this.log(context, error);
      throw error;
    }
  }
}

module.exports = { Broker };
