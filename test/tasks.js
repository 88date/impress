'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { Tasks } = require('../lib/tasks.js');

const root = process.cwd();
const createApplication = () => ({
  path: path.join(root, 'test'),
  console,
  config: {
    server: {
      scheduler: { enabled: true },
      workers: { timeout: 1000 },
    },
  },
  starts: [],
  watcher: { watch() {} },
  absolute(relative) {
    return path.join(this.path, relative);
  },
});

test('lib/tasks - should ignore files when disabled', async () => {
  const updates = [];
  const application = createApplication();
  application.config.server.scheduler.enabled = false;
  const synchronizeTasks = async (declarations) => updates.push(declarations);
  const tasks = new Tasks(application, synchronizeTasks);
  const cleanupPath = path.join(tasks.path, 'cleanup.js');

  await tasks.load();
  await tasks.change(cleanupPath);
  tasks.tree.cleanup = { method: async () => {} };
  await tasks.delete(cleanupPath);

  assert.strictEqual(typeof tasks.tree.cleanup.method, 'function');
  assert.strictEqual(updates.length, 0);
});

test('lib/tasks - should load tasks through Code', async () => {
  const updates = [];
  const application = createApplication();
  const synchronizeTasks = async (declarations) =>
    updates.push(structuredClone(declarations));
  const tasks = new Tasks(application, synchronizeTasks);

  await tasks.load();

  assert.strictEqual(typeof tasks.tree.cleanup.method, 'function');
  assert.strictEqual(typeof tasks.tree.reports.daily.method, 'function');
  assert.strictEqual(updates.length, 1);
  assert.deepStrictEqual(updates[0], [
    {
      name: 'tasks/cleanup',
      path: 'cleanup',
      cron: '0 3 * * *',
      data: { automatic: true },
      options: { retryLimit: 2, tz: 'Europe/Moscow' },
      worker: { localConcurrency: 1 },
    },
    {
      name: 'tasks/reports/daily',
      path: 'reports/daily',
      cron: '0 8 * * *',
      data: {},
      options: {},
      worker: {},
    },
  ]);
});

test('lib/tasks - should update declarations after deletion', async () => {
  const updates = [];
  const application = createApplication();
  const synchronizeTasks = async (declarations) => updates.push(declarations);
  const tasks = new Tasks(application, synchronizeTasks);
  await tasks.load();
  updates.length = 0;

  await tasks.delete(path.join(tasks.path, 'cleanup.js'));

  assert.strictEqual(tasks.tree.cleanup, undefined);
  assert.strictEqual(updates.length, 1);
  assert.strictEqual(updates[0].length, 1);
  assert.strictEqual(updates[0][0].path, 'reports/daily');
});

test('lib/tasks - should execute task and completion handler', async () => {
  const calls = [];
  const application = createApplication();
  const tasks = new Tasks(application, async () => {});
  tasks.tree.cleanup = {
    method: async (data, job) => {
      calls.push(['method', data, job.id]);
      return { cleaned: true };
    },
    onCompleted: async (result, job) => {
      calls.push(['onCompleted', result, job.id]);
    },
  };
  const declaration = { path: 'cleanup' };
  const job = { id: 'job-1', data: { automatic: true } };

  const result = await tasks.execute(declaration, job);

  assert.deepStrictEqual(result, { cleaned: true });
  assert.deepStrictEqual(calls, [
    ['method', { automatic: true }, 'job-1'],
    ['onCompleted', { cleaned: true }, 'job-1'],
  ]);
});

test('lib/tasks - should execute failure handler and reject', async () => {
  const calls = [];
  const application = createApplication();
  const tasks = new Tasks(application, async () => {});
  tasks.tree.cleanup = {
    method: async () => {
      throw new Error('Cleanup failed');
    },
    onFailed: async (reason, job) => {
      calls.push([reason, job.id]);
    },
  };
  const declaration = { path: 'cleanup' };
  const job = { id: 'job-2', data: {} };

  await assert.rejects(tasks.execute(declaration, job), {
    message: 'Cleanup failed',
  });
  assert.deepStrictEqual(calls, [['Cleanup failed', 'job-2']]);
});
