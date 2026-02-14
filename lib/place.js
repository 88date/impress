'use strict';

const { node, metarhia } = require('./deps.js');

class Place {
  constructor(name, application) {
    this.name = name;
    this.path = application.absolute(name);
    this.application = application;
  }

  async load(targetPath = this.path) {
    await metarhia.metautil.ensureDirectory(this.path);
    this.application.watcher.watch(targetPath);
    try {
      const files = await node.fsp.readdir(targetPath, { withFileTypes: true });
      const privateFiles = files.filter(({ name }) => name.startsWith('#'));
      const publicFiles = files.filter(({ name }) => !name.startsWith('#'));
      for (const file of privateFiles) {
        const { name } = file;
        const isPrivate = true;
        const filePath = node.path.join(targetPath, name);
        if (file.isDirectory()) await this.load(filePath);
        else await this.change(filePath, isPrivate);
      }
      for (const file of publicFiles) {
        const { name } = file;
        if (name.startsWith('.eslint')) continue;
        const filePath = node.path.join(targetPath, name);
        if (file.isDirectory()) await this.load(filePath);
        else await this.change(filePath);
      }
    } catch (error) {
      const console = this.application.console || global.console;
      console.error(error.stack);
    }
  }
}

module.exports = { Place };
