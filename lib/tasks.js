'use strict';

const { Code } = require('./code.js');
const { metarhia, wt } = require('./deps.js');
const { TASK_NAMESPACE } = require('./scheduler.js');
const { request } = require('./thread.js');

const sendDeclarations = (declarations) =>
  request(wt.parentPort, { name: 'tasks', declarations });

const createDeclaration = (unit, taskPath, timeout) => ({
  name: TASK_NAMESPACE + taskPath,
  path: taskPath,
  cron: unit.cron,
  tz: unit.tz ?? 'UTC',
  data: unit.data ?? {},
  maxMessages: unit.maxMessages ?? 5,
  retryLimit: unit.retryLimit ?? 2,
  retryDelay: unit.retryDelay ?? 0,
  timeout: unit.timeout ?? timeout,
});

const collect = (level, parentPath, result, timeout) => {
  for (const [name, unit] of Object.entries(level)) {
    if (name === 'parent' || !unit) continue;
    const taskPath = parentPath ? `${parentPath}/${name}` : name;
    const isTask = unit.cron !== undefined || unit.method !== undefined;
    if (isTask) {
      result.push(createDeclaration(unit, taskPath, timeout));
      continue;
    }
    if (typeof unit === 'object') {
      collect(unit, taskPath, result, timeout);
    }
  }
};

const declarations = (tree, timeout = 30000) => {
  const result = [];
  collect(tree, '', result, timeout);
  return result.sort((first, second) => first.name.localeCompare(second.name));
};

const invoke = async (handler, args, timeout) => {
  let promise = Promise.resolve().then(() => handler(...args));
  if (timeout) promise = metarhia.metautil.timeoutify(promise, timeout);
  return promise;
};

class Tasks extends Code {
  constructor(application, synchronizeTasks = sendDeclarations) {
    super('tasks', application);
    this.synchronizeTasks = synchronizeTasks;
    this.loading = 0;
  }

  get enabled() {
    return this.application.config.server.scheduler.enabled;
  }

  async load(targetPath = this.path) {
    if (!this.enabled) return;
    this.loading++;
    try {
      await super.load(targetPath);
    } finally {
      this.loading--;
    }
    if (this.loading === 0) await this.synchronize();
  }

  async change(filePath, isInternal) {
    if (!this.enabled) return;
    await super.change(filePath, isInternal);
    if (this.loading === 0) await this.synchronize();
  }

  async delete(filePath) {
    if (!this.enabled) return;
    super.delete(filePath);
    await this.synchronize();
  }

  async synchronize() {
    let units;
    try {
      const timeout = this.application.config.server.workers.timeout || 30000;
      units = declarations(this.tree, timeout);
    } catch (error) {
      this.application.console.error(error.stack);
      return;
    }
    await this.synchronizeTasks(units);
  }

  getTask(taskPath) {
    let task = this.tree;
    for (const name of taskPath.split('/')) {
      task = task?.[name];
    }
    if (!task) {
      throw new Error(`Task not found: ${taskPath}`);
    }
    return task;
  }

  async notify(handler, args, name) {
    if (!handler) return;
    try {
      const { timeout } = this.application.config.server.workers;
      await invoke(handler, args, timeout);
    } catch (error) {
      const details = error?.stack || error?.message || String(error);
      this.application.console.error(`Failed to execute ${name}`, details);
    }
  }

  async execute(declaration, job) {
    const task = this.getTask(declaration.path);
    try {
      const timeout =
        declaration.timeout ?? this.application.config.server.workers.timeout;
      const result = await invoke(task.method, [job.data, job], timeout);
      const completed = `${declaration.path}.onCompleted`;
      await this.notify(task.onCompleted, [result, job], completed);
      return result;
    } catch (error) {
      const reason = error?.message || String(error);
      const failed = `${declaration.path}.onFailed`;
      await this.notify(task.onFailed, [reason, job], failed);
      throw error;
    }
  }

  async handle({ declaration, job, port }) {
    try {
      const result = await this.execute(declaration, job);
      port.postMessage({ result });
    } catch (error) {
      const details = error?.stack || error?.message || String(error);
      this.application.console.error('Failed to execute task', details);
      const message = error?.message || String(error);
      port.postMessage({ error: { message } });
    } finally {
      port.close();
    }
  }
}

module.exports = { Tasks, declarations };
