'use strict';

const { stripTypeScriptTypes } = require('node:module');
const { node, metarhia } = require('./deps.js');
const { Place } = require('./place.js');
const bus = require('./bus.js');

const JAVASCRIPT_EXTENSION = '.js';
const TYPESCRIPT_EXTENSION = '.ts';

const parsePath = (relPath) => {
  const extension = node.path.extname(relPath);
  const name = node.path.basename(relPath, extension);
  const names = relPath.split(node.path.sep);
  names[names.length - 1] = name;
  return names;
};

const readScript = async (filePath, options) => {
  if (!filePath.endsWith(TYPESCRIPT_EXTENSION)) {
    return metarhia.metavm.readScript(filePath, options);
  }
  const source = await node.fsp.readFile(filePath, 'utf8');
  if (source === '') throw new SyntaxError(`File ${filePath} is empty`);
  const code = stripTypeScriptTypes(source, { mode: 'strip' });
  return metarhia.metavm.createScript(filePath, code, options);
};

class Code extends Place {
  constructor(name, application, options = {}) {
    super(name, application);
    this.typescript = options.typescript || false;
    this.tree = {};
    this.internal = {};
    this.contexts = {};
  }

  #isModule(filePath) {
    if (filePath.endsWith('.d.ts')) return false;
    if (filePath.startsWith('.eslint')) return false;
    const extension = node.path.extname(filePath);
    if (extension === JAVASCRIPT_EXTENSION) return true;
    return this.typescript && extension === TYPESCRIPT_EXTENSION;
  }

  async stop() {
    for (const moduleName of Object.keys(this.tree)) {
      const module = this.tree[moduleName];
      if (typeof module.stop === 'function') {
        await this.application.execute(module.stop);
      }
    }
  }

  stopModule(name, module) {
    const timeout = this.application.config.server.timeouts.watch;
    setTimeout(() => {
      if (this.tree[name] !== undefined) return;
      this.application.execute(module.stop);
    }, timeout);
  }

  set(relPath, unit, isInternal) {
    const names = parsePath(relPath);
    let level = isInternal ? this.internal : this.tree;
    const last = names.length - 1;
    for (let depth = 0; depth <= last; depth++) {
      const name = names[depth].replace('#', '');
      let next = level[name];
      if (depth === last) {
        if (unit === null) {
          if (name === 'stop') this.stopModule(names[0], level);
          delete level[name];
          return;
        }
        next = unit;
        unit.parent = level;
      }
      if (next === undefined) next = { parent: level };
      level[name] = next;
      let { start } = unit;
      if (start) start = start.bind(unit);
      if (depth === 1 && name === 'start') start = unit;
      if (start) {
        if (start.constructor.name === 'AsyncFunction') {
          this.application.starts.push(start);
        } else {
          const msg = `${relPath}/start expected to be async function`;
          this.application.console.error(msg);
        }
      }
      level = next;
    }
  }

  delete(filePath) {
    if (!this.#isModule(filePath)) return;
    const relPath = filePath.substring(this.path.length + 1);
    const isInternal = relPath.includes('#');
    this.set(relPath, null, isInternal);
  }

  getModuleContext(moduleName) {
    if (!this.contexts[moduleName]) {
      if (!this.internal[moduleName]) this.internal[moduleName] = {};
      const context = Object.assign({}, this.application.sandbox, {
        internal: this.internal[moduleName],
      });
      const sandbox = metarhia.metavm.createContext(context);
      this.contexts[moduleName] = sandbox;
    }
    return this.contexts[moduleName];
  }

  async change(filePath, isInternal) {
    if (!this.#isModule(filePath)) return;
    const { application, path, name } = this;
    const extension = node.path.extname(filePath);
    const isTest = filePath.endsWith('.test' + extension);
    if (isTest && application.mode !== 'test') return;

    const relPath = filePath.substring(path.length + 1);
    const moduleName = parsePath(relPath)[0];

    const context = this.getModuleContext(moduleName);

    const options = { context, filename: filePath };
    try {
      const { exports } = await readScript(filePath, options);
      const exp = name === 'bus' ? bus.prepare(exports, application) : exports;
      this.set(relPath, exp, isInternal);
      if (isTest) application.tests.push(exp);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        application.console.error(error.stack);
      }
    }
  }
}

module.exports = { Code };
