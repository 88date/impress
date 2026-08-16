'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Broker, EventBroker } = require('./broker.js');
const { Place } = require('./place.js');

const SERVICE_CONFIG = '.service.js';
const SERVICE_EVENTS = '.events.js';

class Service extends Place {
  constructor(name, application) {
    super(name, application);
    this.collection = {};
    this.signatures = {};
    this.configs = {};
    this.events = {};
  }

  async createScript(fileName) {
    try {
      const code = await node.fsp.readFile(fileName, 'utf8');
      if (!code) return null;
      const src = 'context => ' + code;
      const options = { context: this.application.sandbox };
      const { MetaScript } = metarhia.metavm;
      const { exports } = new MetaScript(fileName, src, options);
      return exports;
    } catch (error) {
      if (error.code !== 'ENOENT') {
        this.application.console.error(error.stack);
      }
      return null;
    }
  }

  cacheSignature(unitName, methodName, method) {
    const name = node.path.basename(unitName, '.js');
    let unitMethods = this.signatures[name];
    if (!unitMethods) {
      this.signatures[name] = unitMethods = {};
    }
    unitMethods[methodName] = metarhia.metautil.getSignature(method);
  }

  delete(filePath) {
    const relPath = filePath.substring(this.path.length + 1);
    if (!relPath.includes(node.path.sep)) return;
    const [unitName, methodFile] = relPath.split(node.path.sep);
    if (!methodFile.endsWith('.js')) return;
    const metadataName = unitName.includes('.') ? unitName : unitName + '.1';
    if (methodFile === SERVICE_CONFIG) {
      delete this.configs[metadataName];
      if (this.application.nats) this.application.nats.subscribeServices();
      return;
    }
    if (methodFile === SERVICE_EVENTS) {
      const [name] = metadataName.split('.');
      const eventBroker = this.events[name];
      if (eventBroker) eventBroker.load({});
      return;
    }
    const methodName = node.path.basename(methodFile, '.js');
    const [name, ver] = metadataName.split('.');
    const version = parseInt(ver, 10);
    const unit = this.collection[name];
    if (!unit) return;
    const methods = unit[version.toString()];
    const broker = methods?.[methodName];
    if (broker && this.application.nats) {
      this.application.nats.unsubscribeService(broker.subject);
    }
    if (methods) delete methods[methodName];
    const internalUnit = this.application.sandbox.service[name];
    if (internalUnit) {
      let replacement = null;
      for (const currentVersion of Object.keys(unit)) {
        if (currentVersion === 'default') continue;
        const currentBroker = unit[currentVersion][methodName];
        if (!currentBroker) continue;
        replacement = currentBroker;
        break;
      }
      if (replacement) {
        internalUnit[methodName] = replacement.call.bind(replacement);
      } else {
        delete internalUnit[methodName];
      }
    }
    const cache = this.signatures[metadataName];
    if (cache) delete cache[methodName];
  }

  async change(filePath) {
    if (!filePath.endsWith('.js')) return;

    const script = await this.createScript(filePath);
    if (!script) return;
    const relPath = filePath.substring(this.path.length + 1);
    const [unitDir, methodFile] = relPath.split(node.path.sep);
    const unitName = unitDir.includes('.') ? unitDir : unitDir + '.1';
    if (methodFile === SERVICE_CONFIG) {
      this.configs[unitName] = script();
      this.prepareUnit(unitName);
      if (this.application.nats) this.application.nats.subscribeServices();
      return;
    }
    if (methodFile === SERVICE_EVENTS) {
      const events = script();
      const { eventBroker } = this.prepareUnit(unitName);
      eventBroker.load(events);
      return;
    }
    const proc = new Broker(script, 'method', unitName, this.application);
    const unit = proc.exports;
    if (methodFile) {
      const name = node.path.basename(methodFile, '.js');
      return void this.changeUnit(unitName, name, proc);
    }
    if (unit.plugin) {
      return void this.loadPlugin(unitName, unit);
    }
    for (const name of Object.keys(unit)) {
      const proc = new Broker(script, name, unitName, this.application);
      this.changeUnit(unitName, name, proc);
    }
  }

  loadPlugin(unitName, unit) {
    const [library, name] = unit.plugin.split('/');
    const lib = metarhia[library] || npm[library];
    if (!lib || !lib.plugins) return;
    const pluginSrc = lib.plugins[name];
    if (!pluginSrc) return;
    const context = this.application.sandbox;
    const options = { context };
    const { exports } = metarhia.metavm.createScript(name, pluginSrc, options);
    const plugin = exports(unit);
    for (const [name, script] of Object.entries(plugin)) {
      const proc = new Broker(script, name, unitName, this.application);
      this.changeUnit(unitName, name, proc);
    }
  }

  changeUnit(unitName, name, proc) {
    const { internalUnit, methods } = this.prepareUnit(unitName);
    proc.actionName = name;
    methods[name] = proc;
    const { method } = proc;
    internalUnit[name] = proc.call.bind(proc);
    if (method) this.cacheSignature(unitName, name, method);
    if (this.application.nats) this.application.nats.subscribeService(proc);
  }

  prepareUnit(unitName) {
    const [name, ver] = unitName.split('.');
    let unit = this.collection[name];
    const { service } = this.application.sandbox;
    let internalUnit = service[name];
    let eventBroker = this.events[name];
    if (!unit) {
      this.collection[name] = unit = { default: 1 };
      this.events[name] = eventBroker = new EventBroker(name, this.application);
      service[name] = internalUnit = {
        emit: eventBroker.emit.bind(eventBroker),
        on: eventBroker.on.bind(eventBroker),
      };
    }
    let methods = unit[ver];
    if (!methods) unit[ver] = methods = {};
    return { internalUnit, methods, eventBroker };
  }
}

module.exports = { Service };
