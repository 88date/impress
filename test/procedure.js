'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { AsyncLocalStorage } = require('node:async_hooks');
const metautil = require('metautil');
const { Procedure } = require('../lib/procedure.js');

const contextStorage = new AsyncLocalStorage();

test('lib/procedure - should create procedure correctly', async () => {
  const script = () => ({
    transports: ['http', 'ws'],
    method: async ({ a, b }) => a + b,
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: {} } },
  };

  const procedure = new Procedure(script, 'method', application);

  assert.strictEqual(procedure.constructor.name, 'Procedure');
  assert.strictEqual(typeof procedure.exports, 'object');
  assert.strictEqual(typeof procedure.exports.method, 'function');
  assert.strictEqual(typeof procedure.script, 'function');
  assert.strictEqual(procedure.methodName, 'method');
  assert.strictEqual(typeof procedure.application, 'object');
  assert.strictEqual(typeof procedure.method, 'function');
  assert.strictEqual(procedure.method.constructor.name, 'AsyncFunction');
  assert.strictEqual(procedure.parameters, null);
  assert.strictEqual(procedure.returns, null);
  assert.strictEqual(procedure.errors, null);
  assert.ok(procedure.locks instanceof Map);
  assert.strictEqual(procedure.locks.size, 0);
  assert.strictEqual(procedure.caption, '');
  assert.strictEqual(procedure.description, '');
  assert.strictEqual(procedure.access, '');
  assert.strictEqual(procedure.validate, null);
  assert.strictEqual(typeof procedure.timeout, 'number');
  assert.strictEqual(procedure.serializer, null);
  assert.strictEqual(procedure.protocols, null);
  assert.deepStrictEqual(procedure.transports, ['http', 'ws']);
  assert.strictEqual(procedure.deprecated, false);
  assert.strictEqual(procedure.assert, null);
  assert.strictEqual(procedure.examples, null);

  const result = await procedure.invoke({}, { a: 4, b: 6 });
  assert.strictEqual(result, 10);
});

test('lib/procedure - should prepare transports', () => {
  const application = {
    contextStorage,
    config: { server: { timeouts: {} } },
  };

  const defaultProcedure = new Procedure(
    () => ({ method: async () => {} }),
    'method',
    application,
  );
  const restrictedProcedure = new Procedure(
    () => ({ transports: ['centrifugo'], method: async () => {} }),
    'method',
    application,
  );

  assert.deepStrictEqual(defaultProcedure.transports, []);
  assert.deepStrictEqual(restrictedProcedure.transports, ['centrifugo']);
});

test('lib/procedure - should validate procedure correctly', async () => {
  const script = () => ({
    transports: ['http', 'ws'],

    validate: ({ a, b }) => {
      if (a % 3 === 0) throw new Error('Expected `a` to be multiple of 3');
      if (b % 5 === 0) throw new Error('Expected `b` to be multiple of 5');
    },

    method: async ({ a, b }) => {
      const result = a + b;
      return result;
    },
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: {} } },
  };
  const procedure = new Procedure(script, 'method', application);

  await assert.rejects(
    () => procedure.invoke({}, { a: 3, b: 6 }),
    new Error('Expected `a` to be multiple of 3'),
  );

  const result = await procedure.invoke({}, { a: 4, b: 6 });
  assert.strictEqual(result, 10);
});

test('lib/procedure - should validate procedure async', async () => {
  const script = () => ({
    transports: ['http', 'ws'],

    validate: async ({ a, b }) => {
      await metautil.delay(100);
      if (a % 3 === 0) {
        throw new Error('Expected `a` not to be multiple of 3');
      }
      if (b % 5 === 0) {
        throw new Error('Expected `b` not to be multiple of 5');
      }
    },

    method: async ({ a, b }) => a + b,
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: {} } },
  };
  const procedure = new Procedure(script, 'method', application);

  await assert.rejects(
    () => procedure.invoke({}, { a: 4, b: 10 }),
    new Error('Expected `b` not to be multiple of 5'),
  );

  const result = await procedure.invoke({}, { a: 4, b: 6 });
  assert.strictEqual(result, 10);
});

test('lib/procedure - should handle timeout correctly', async () => {
  const DONE = 'success';

  const script = () => ({
    transports: ['http', 'ws'],
    timeout: 100,

    method: async ({ waitTime }) =>
      new Promise((resolve) => {
        setTimeout(() => resolve(DONE), waitTime);
      }),
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: { request: 20 } } },
  };

  const procedure = new Procedure(script, 'method', application);

  await assert.rejects(
    () => procedure.invoke({}, { waitTime: 150 }),
    new Error('Timeout of 100ms reached'),
  );

  const result = await procedure.invoke({}, { waitTime: 50 });
  assert.strictEqual(result, DONE);
});

test('lib/procedure - should handle queue correctly', async () => {
  const DONE = 'success';

  const script = () => ({
    transports: ['http', 'ws'],

    queue: {
      concurrency: 1,
      size: 1,
      timeout: 15,
    },

    method: async ({ waitTime }) =>
      new Promise((resolve) => {
        setTimeout(() => resolve(DONE), waitTime);
      }),
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: {} } },
  };

  const rpc = async (proc, args) => {
    let result = null;
    await proc.enter();
    try {
      result = await proc.invoke({}, args);
    } catch {
      throw new Error('Procedure.invoke failed. Check your script.method');
    }
    proc.leave();
    return result;
  };

  const procedure = new Procedure(script, 'method', application);

  const invokes = await Promise.allSettled([
    rpc(procedure, { waitTime: 2 }),
    rpc(procedure, { waitTime: 1 }),
  ]);
  const last = invokes[1];
  assert.strictEqual(last.value, DONE);

  await assert.rejects(async () => {
    const invokes = await Promise.allSettled([
      rpc(procedure, { waitTime: 16 }),
      rpc(procedure, { waitTime: 1 }),
    ]);
    const last = invokes[1];
    if (last.status === 'rejected') throw last.reason;
    return last.value;
  }, new Error('Semaphore timeout'));

  await assert.rejects(async () => {
    const invokes = await Promise.allSettled([
      rpc(procedure, { waitTime: 1 }),
      rpc(procedure, { waitTime: 1 }),
      rpc(procedure, { waitTime: 1 }),
    ]);
    const last = invokes[2];
    if (last.status === 'rejected') throw last.reason;
    return last.value;
  }, new Error('Semaphore queue is full'));
});

test('lib/procedure - should preserve queue limits after handoff', async () => {
  const script = () => ({
    queue: { concurrency: 1, size: 1, timeout: 1000 },
    method: async () => {},
  });
  const application = {
    semaphore: new metautil.Semaphore({ concurrency: 3 }),
    config: { server: { timeouts: {} } },
  };
  const procedure = new Procedure(script, 'method', application);
  const ip = '127.0.0.1';

  await procedure.enter(ip);
  const waiting = procedure.enter(ip);
  await new Promise(setImmediate);
  procedure.leave(ip);
  await waiting;

  let entered = false;
  const next = procedure.enter(ip).then(() => {
    entered = true;
  });
  await new Promise(setImmediate);
  assert.strictEqual(entered, false);

  procedure.leave(ip);
  await next;
  procedure.leave(ip);
  assert.strictEqual(procedure.locks.size, 0);
  assert.strictEqual(application.semaphore.empty, true);
});

test('lib/procedure - should retain limits while requests run', async () => {
  const script = () => ({
    queue: { concurrency: 2, size: 0, timeout: 1000 },
    method: async () => {},
  });
  const application = {
    semaphore: new metautil.Semaphore({ concurrency: 3 }),
    config: { server: { timeouts: {} } },
  };
  const procedure = new Procedure(script, 'method', application);
  const ip = '127.0.0.1';

  await procedure.enter(ip);
  await procedure.enter(ip);
  procedure.leave(ip);
  await procedure.enter(ip);

  await assert.rejects(procedure.enter(ip), {
    message: 'Semaphore queue is full',
  });

  procedure.leave(ip);
  procedure.leave(ip);
  assert.strictEqual(procedure.locks.size, 0);
  assert.strictEqual(application.semaphore.empty, true);
});

test('lib/procedure - should handle global timeouts.request', async () => {
  const DONE = 'success';

  const script = () => ({
    transports: ['http', 'ws'],
    timeout: undefined,

    method: async ({ waitTime }) =>
      new Promise((resolve) => {
        setTimeout(() => resolve(DONE), waitTime);
      }),
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: { request: 10 } } },
  };

  const procedure = new Procedure(script, 'method', application);

  await assert.rejects(
    () => procedure.invoke({}, { waitTime: 20 }),
    new Error('Timeout of 10ms reached'),
  );
});

test('lib/procedure - should preserve async context', async () => {
  const script = () => ({
    transports: ['http', 'ws'],

    method: async ({ waitTime }) => {
      await metautil.delay(waitTime);
      return contextStorage.getStore().id;
    },
  });

  const application = {
    Error,
    contextStorage,
    semaphore: {
      async enter() {},
      leave() {},
    },
    config: { server: { timeouts: {} } },
  };

  const procedure = new Procedure(script, 'method', application);
  const first = procedure.invoke({ id: 'first' }, { waitTime: 10 });
  const second = procedure.invoke({ id: 'second' }, { waitTime: 1 });
  const results = await Promise.all([first, second]);

  assert.deepStrictEqual(results, ['first', 'second']);
  assert.strictEqual(contextStorage.getStore(), undefined);
});
