'use strict';

const { Code } = require('./code.js');
const { metarhia, wt } = require('./deps.js');
const { TASK_NAMESPACE } = require('./scheduler.js');
const { request } = require('./thread.js');

const sendDeclarations = (declarations) =>
  request(wt.parentPort, { name: 'tasks', declarations });

const JOB_OPTIONS = [
  'priority',
  'retryLimit',
  'retryDelay',
  'retryBackoff',
  'retryDelayMax',
  'expireInSeconds',
  'retentionSeconds',
  'deleteAfterSeconds',
  'heartbeatSeconds',
  'singletonKey',
  'singletonSeconds',
  'singletonNextSlot',
  'group',
  'deadLetter',
];

const WORK_OPTIONS = [
  'pollingIntervalSeconds',
  'notifyPollingIntervalSeconds',
  'burstWhenReadyExceeds',
  'burstWhenBatchFull',
  'orderByCreatedOn',
  'minPriority',
  'maxPriority',
  'localConcurrency',
  'localGroupConcurrency',
  'groupConcurrency',
  'heartbeatRefreshSeconds',
];

const TASK_EVENTS = ['onCompleted', 'onFailed'];

const selectOptions = (unit, fields) => {
  const options = {};
  for (const field of fields) {
    if (unit[field] !== undefined) options[field] = unit[field];
  }
  return options;
};

const createDeclaration = (unit, taskPath) => {
  if (!unit || typeof unit.method !== 'function') {
    throw new Error(`Task "${taskPath}": method expected`);
  }
  if (typeof unit.cron !== 'string') {
    throw new Error(`Task "${taskPath}": cron expected`);
  }

  const name = TASK_NAMESPACE + taskPath;
  const options = selectOptions(unit, JOB_OPTIONS);
  if (unit.tz !== undefined) options.tz = unit.tz;
  const worker = selectOptions(unit, WORK_OPTIONS);
  const declaration = {
    name,
    path: taskPath,
    cron: unit.cron,
    data: unit.data ?? {},
    options,
    worker,
  };
  for (const event of TASK_EVENTS) {
    const handler = unit[event];
    if (handler === undefined) continue;
    if (typeof handler !== 'function') {
      throw new Error(`Task "${taskPath}": ${event} handler expected`);
    }
  }
  return declaration;
};

const collect = (level, parentPath, result) => {
  for (const [name, unit] of Object.entries(level)) {
    if (name === 'parent' || !unit) continue;
    const taskPath = parentPath ? `${parentPath}/${name}` : name;
    const isTask = unit.cron !== undefined || unit.method !== undefined;
    if (isTask) {
      result.push(createDeclaration(unit, taskPath));
      continue;
    }
    if (typeof unit === 'object') {
      collect(unit, taskPath, result);
    }
  }
};

const declarations = (tree) => {
  const result = [];
  collect(tree, '', result);
  return result.sort((first, second) => first.name.localeCompare(second.name));
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
      units = declarations(this.tree);
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
    if (!task || typeof task.method !== 'function') {
      throw new Error(`Task not found: ${taskPath}`);
    }
    return task;
  }

  async invoke(handler, args) {
    let promise = Promise.resolve().then(() => handler(...args));
    const { timeout } = this.application.config.server.workers;
    if (timeout) promise = metarhia.metautil.timeoutify(promise, timeout);
    return promise;
  }

  async notify(handler, args, name) {
    if (typeof handler !== 'function') return;
    try {
      await this.invoke(handler, args);
    } catch (error) {
      const details = error?.stack || error?.message || String(error);
      this.application.console.error(`Failed to execute ${name}`, details);
    }
  }

  async execute(declaration, job) {
    const task = this.getTask(declaration.path);
    try {
      const result = await this.invoke(task.method, [job.data, job]);
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
