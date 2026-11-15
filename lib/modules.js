'use strict';

const { relative, sep } = require('node:path');
const { Code } = require('./code.js');

class Modules extends Code {
  constructor(application) {
    super('modules', application, { typescript: true });
    this.loading = 0;
  }

  getDeclarationLoader(filePath) {
    const parts = relative(this.path, filePath).split(sep);
    const name = parts[1];
    if (name === 'events' || name === 'subscribers') {
      return this.application[name];
    }
    return null;
  }

  async load(targetPath = this.path) {
    const loader = this.getDeclarationLoader(targetPath);
    if (!loader) {
      this.loading++;
      try {
        await super.load(targetPath);
      } finally {
        this.loading--;
      }
    }
    if (this.loading > 0 || this.application.initialization) return;
    try {
      if (loader) {
        await loader.load(targetPath);
      } else {
        await this.application.events.load();
        await this.application.subscribers.load();
      }
    } catch (error) {
      this.application.console.error(error);
    }
  }

  async change(filePath, isInternal) {
    const loader = this.getDeclarationLoader(filePath);
    if (loader) return loader.change(filePath);
    return super.change(filePath, isInternal);
  }

  async delete(filePath) {
    const loader = this.getDeclarationLoader(filePath);
    const { events, subscribers } = this.application;
    try {
      if (loader) {
        await loader.delete(filePath);
      } else {
        super.delete(filePath);
        const parts = relative(this.path, filePath).split(sep);
        if (parts.length > 1) return;
        await Promise.all([
          events.delete(filePath),
          subscribers.delete(filePath),
        ]);
      }
    } catch (error) {
      this.application.console.error(error);
    }
  }
}

module.exports = { Modules };
