'use strict';

const { node, npm, metarhia } = require('./deps.js');
const { Procedure } = require('./procedure.js');
const { Place } = require('./place.js');

class Api extends Place {
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

  updateDefault(name, methodName) {
    const unit = this.collection[name];
    const methods = unit[unit.default];
    const internalUnit = this.application.sandbox.api[name];
    const unitName = name + '.' + unit.default;
    delete methods[methodName];
    delete internalUnit[methodName];
    const cache = this.signatures[unitName];
    if (cache) delete cache[methodName];
    for (const version of Object.keys(unit).reverse()) {
      if (version === 'default') continue;
      const proc = unit[version][methodName];
      if (!proc) continue;
      methods[methodName] = proc;
      const { method, exports } = proc;
      internalUnit[methodName] = method || exports;
      if (method) this.cacheSignature(unitName, methodName, method);
      break;
    }
  }

  delete(filePath) {
    const relPath = filePath.substring(this.path.length + 1);
    if (!relPath.includes(node.path.sep)) return;
    const [unitName, methodFile] = relPath.split(node.path.sep);
    if (!methodFile.endsWith('.js')) return;
    const methodName = node.path.basename(methodFile, '.js');
    const [name, ver = '1'] = unitName.split('.');
    const version = parseInt(ver, 10);
    const unit = this.collection[name];
    if (!unit) return;
    const methods = unit[version.toString()];
    if (methods) delete methods[methodName];
    const cache = this.signatures[name + '.' + version];
    if (cache) delete cache[methodName];
    this.updateDefault(name, methodName);
  }

  async change(filePath) {
    if (!filePath.endsWith('.js')) return;
    const script = await this.createScript(filePath);
    if (!script) return;
    const proc = new Procedure(script, 'method', this.application);
    const unit = proc.exports;
    const relPath = filePath.substring(this.path.length + 1);
    const [unitDir, methodFile] = relPath.split(node.path.sep);
    const unitName = unitDir.includes('.') ? unitDir : unitDir + '.1';
    if (methodFile) {
      const name = node.path.basename(methodFile, '.js');
      return void this.changeUnit(unitName, name, proc);
    }
    if (unit.plugin) {
      return void this.loadPlugin(unitName, unit);
    }
    for (const name of Object.keys(unit)) {
      const proc = new Procedure(script, name, this.application);
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
      const proc = new Procedure(script, name, this.application);
      this.changeUnit(unitName, name, proc);
    }
  }

  changeUnit(unitName, name, proc) {
    const { methods } = this.prepareUnit(unitName);
    methods[name] = proc;
    const { method } = proc;
    if (method) this.cacheSignature(unitName, name, method);
    const [unit] = unitName.split('.');
    this.updateDefault(unit, name);
  }

  prepareUnit(unitName) {
    const [name, ver] = unitName.split('.');
    let unit = this.collection[name];
    const { api } = this.application.sandbox;
    let internalUnit = api[name];
    if (!unit) {
      this.collection[name] = unit = { default: '*' };
      Object.defineProperty(unit, '*', { value: {} });
      api[name] = internalUnit = {};
    }
    let methods = unit[ver];
    if (!methods) unit[ver] = methods = {};
    return { internalUnit, methods };
  }
}

module.exports = { Api };
