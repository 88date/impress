'use strict';

const { metarhia } = require('./deps.js');
const { Place } = require('./place.js');
const bus = require('./bus.js');

class Code extends Place {
  constructor(name, application) {
    super(name, application);
    this.tree = {};
    this.internal = {};
    this.contexts = {};
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
    const names = metarhia.metautil.parsePath(relPath);
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
      if (depth === 1 && name === 'start') {
        if (unit.constructor.name === 'AsyncFunction') {
          this.application.starts.push(unit);
        } else {
          const msg = `${relPath} expected to be async function`;
          this.application.console.error(msg);
        }
      }
      level = next;
    }
  }

  delete(filePath) {
    const relPath = filePath.substring(this.path.length + 1);
    const isInternal = relPath.startsWith(relPath);
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
    if (!filePath.endsWith('.js')) return;
    if (filePath.startsWith('.eslint')) return;
    const { application, path, name } = this;
    const isTest = filePath.endsWith('.test.js');
    if (isTest && application.mode !== 'test') return;

    const relPath = filePath.substring(path.length + 1);
    const moduleName = metarhia.metautil.parsePath(relPath)[0];

    const context = isInternal
      ? application.sandbox
      : this.getModuleContext(moduleName);

    const options = { context, filename: filePath };
    try {
      const { exports } = await metarhia.metavm.readScript(filePath, options);
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
