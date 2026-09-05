'use strict';

const { node, metarhia } = require('./deps.js');
const { Broker } = require('./broker.js');
const { Place } = require('./place.js');

class Service extends Place {
  constructor(name, application) {
    super(name, application);
    this.collection = {};
    this.signatures = {};
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

  getBroker(name, actionName, version = this.collection[name]?.default ?? 1) {
    const broker = this.collection[name]?.[version]?.[actionName];
    if (broker) return broker;
    const subject = `${name}.${version}.${actionName}`;
    throw new metarhia.metautil.Error(
      `Service action is not available: ${subject}`,
    );
  }

  describe(name) {
    const actions = [];
    const unit = this.collection[name];
    if (!unit) return { name, actions };
    for (const version of Object.keys(unit)) {
      if (version === 'default') continue;
      const methods = unit[version];
      for (const broker of Object.values(methods)) {
        if (broker.method) actions.push(broker.describe());
      }
    }
    return { name, actions };
  }

  isRemote(name) {
    const unit = this.collection[name];
    if (!unit) return false;
    for (const version of Object.keys(unit)) {
      if (version === 'default') continue;
      const methods = unit[version];
      for (const broker of Object.values(methods)) {
        if (!broker.method) return true;
      }
    }
    return false;
  }

  loadRemote(name, actions) {
    for (const action of actions) {
      const unitName = `${name}.${action.version}`;
      const broker = Broker.fromContract(action, unitName, this.application);
      const current = this.collection[name]?.[action.version]?.[action.name];
      if (!current?.method) this.changeUnit(unitName, action.name, broker);
    }
  }

  delete(filePath) {
    const relPath = filePath.substring(this.path.length + 1);
    if (!relPath.includes(node.path.sep)) return;
    const [unitName, methodFile] = relPath.split(node.path.sep);
    if (!methodFile.endsWith('.js')) return;
    const metadataName = unitName.includes('.') ? unitName : unitName + '.1';
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
    if (methods) {
      delete methods[methodName];
      if (Object.keys(methods).length === 0) {
        delete unit[version];
        while (unit.default > 1 && !unit[unit.default]) unit.default--;
      }
    }
    const internalUnit = this.application.sandbox.service[name];
    if (internalUnit) {
      let available = false;
      for (const currentVersion of Object.keys(unit)) {
        if (currentVersion === 'default') continue;
        const currentBroker = unit[currentVersion][methodName];
        if (!currentBroker) continue;
        available = true;
        break;
      }
      if (!available) delete internalUnit[methodName];
    }
    const cache = this.signatures[metadataName];
    if (cache) delete cache[methodName];
    if (broker && !broker.discovered && this.application.nats) {
      this.application.nats.updateDiscovery();
    }
  }

  async change(filePath) {
    if (!filePath.endsWith('.js')) return;
    const relPath = filePath.substring(this.path.length + 1);
    const [unitDir, methodFile] = relPath.split(node.path.sep);
    const unitName = unitDir.includes('.') ? unitDir : unitDir + '.1';
    const script = await this.createScript(filePath);
    if (!script) return;
    const proc = new Broker(script, 'method', unitName, this.application);
    if (!proc.transports.includes('nats')) {
      this.delete(filePath);
      return;
    }
    if (!methodFile) return;
    const name = node.path.basename(methodFile, '.js');
    this.changeUnit(unitName, name, proc);
  }

  changeUnit(unitName, name, proc) {
    const { internalUnit, methods } = this.prepareUnit(unitName);
    proc.actionName = name;
    methods[name] = proc;
    const { method } = proc;
    if (!internalUnit[name]) {
      const serviceName = proc.serviceName;
      internalUnit[name] = async (args = {}, { version } = {}) => {
        const broker = this.getBroker(serviceName, name, version);
        return broker.call(args);
      };
    }
    if (method) this.cacheSignature(unitName, name, method);
    if (this.application.nats) {
      this.application.nats.subscribeService(proc);
      if (!proc.discovered) {
        this.application.nats.updateDiscovery();
      }
    }
  }

  prepareUnit(unitName) {
    const [name, ver] = unitName.split('.');
    const version = parseInt(ver, 10);
    let unit = this.collection[name];
    const { service } = this.application.sandbox;
    let internalUnit = service[name];
    if (!unit) {
      this.collection[name] = unit = { default: 1 };
      service[name] = internalUnit = {};
    }
    let methods = unit[ver];
    if (!methods) unit[ver] = methods = {};
    if (version > unit.default) unit.default = version;
    return { internalUnit, methods };
  }
}

module.exports = { Service };
