'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { mkdir, mkdtemp, rm, writeFile } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const { Worker } = require('node:worker_threads');
const { connect } = require('@nats-io/transport-node');
const { MQ } = require('../lib/mq.js');
const { Nats } = require('../lib/nats.js');

const binary = process.env.NATS_SERVER_BINARY;

const waitFor = async (predicate, signal) => {
  while (true) {
    if (await predicate()) return;
    await delay(10, undefined, { signal });
  }
};

const startServer = async (resources) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'impress-mq-'));
  resources.directory = directory;
  const server = spawn(
    binary,
    ['-js', '-a', '127.0.0.1', '-p', '-1', '-sd', directory],
    { windowsHide: true, stdio: ['ignore', 'ignore', 'pipe'] },
  );
  resources.server = server;
  return new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => {
      reject(new Error(`NATS test server did not start: ${output}`));
    }, 5000);
    const fail = (error) => {
      clearTimeout(timer);
      reject(error);
    };
    server.once('error', fail);
    server.once('exit', (code) => {
      fail(new Error(`NATS test server exited (${code}): ${output}`));
    });
    server.stderr.on('data', (chunk) => {
      output = (output + chunk.toString()).slice(-4096);
      const match = output.match(
        /Listening for client connections on .*:(\d+)/,
      );
      if (match && output.includes('Server is ready')) {
        clearTimeout(timer);
        resolve(`nats://127.0.0.1:${match[1]}`);
      }
    });
  });
};

const stopResources = async (resources) => {
  for (const gate of resources.gates) gate.resolve();
  try {
    for (const worker of resources.workers) await worker.terminate();
    for (const consumer of resources.consumers) await consumer.stop();
    for (const nats of resources.connections) await nats.close();
  } finally {
    const { server, directory } = resources;
    if (server?.pid && server.exitCode === null && server.signalCode === null) {
      const exited = once(server, 'exit');
      server.kill();
      await exited;
    }
    if (directory) {
      const resolved = path.resolve(directory);
      assert.strictEqual(path.dirname(resolved), path.resolve(tmpdir()));
      assert.ok(path.basename(resolved).startsWith('impress-mq-'));
      await rm(resolved, { recursive: true, force: true });
    }
  }
};

test(
  'mq integration - consumes tasks on an isolated NATS server',
  { skip: !binary, timeout: 30000 },
  async (t) => {
    const resources = {
      directory: null,
      server: null,
      connections: [],
      consumers: [],
      workers: [],
      gates: [],
    };
    t.after(() => stopResources(resources));
    const servers = await startServer(resources);
    const runtime = async (kind = 'server', sandbox = {}, logger = console) => {
      const root = path.join(
        resources.directory,
        'application-' + resources.connections.length,
      );
      const application = {
        absolute: (name) => path.join(root, name),
        kind,
        sandbox,
        console: logger,
        watcher: { watch() {} },
      };
      const nats = new Nats(application);
      nats.connection = await connect({ servers, reconnect: false });
      application.nats = nats;
      resources.connections.push(nats);
      const mq = new MQ('mq', application);
      application.sandbox.mq = mq.tree;
      resources.consumers.push(mq);
      return { nats, mq };
    };
    const publisher = await runtime();
    const { manager } = await publisher.nats.getJetStream();
    const receive = async (queue, method, options = {}, logger = console) => {
      const { mq } = await runtime('worker', { execute: method }, logger);
      const directory = path.join(mq.path, queue);
      await mkdir(directory, { recursive: true });
      await Promise.all([
        writeFile(
          path.join(directory, '.queue.js'),
          `(${JSON.stringify(options)})`,
        ),
        writeFile(
          path.join(directory, 'run.js'),
          '({ method: async (data, job) => execute(data, job) })',
        ),
      ]);
      await mq.load();
      assert.deepStrictEqual(Object.keys(mq.consumers), []);
      await mq.start();
      return mq;
    };
    const gate = () => {
      const result = Promise.withResolvers();
      resources.gates.push(result);
      return result;
    };
    const settled = async (queue, signal) => {
      await waitFor(async () => {
        const info = await manager.consumers.info('MQ', `mq-${queue}`);
        return info.num_ack_pending === 0 && info.num_pending === 0;
      }, signal);
    };

    await manager.streams.add({
      name: 'BULK',
      subjects: ['mq.bulk.>'],
      retention: 'workqueue',
      storage: 'file',
      allow_atomic: true,
    });
    await manager.streams.add({
      name: 'SECOND',
      subjects: ['mq.cross.>'],
      retention: 'workqueue',
      storage: 'file',
    });
    const stored = async (stream) =>
      (await manager.streams.info(stream)).state.messages;

    await t.test(
      'rejects whole batches when features are disabled',
      async () => {
        await assert.rejects(
          publisher.mq.tree.addBulk([
            { name: 'bulk:run', data: { id: 'first' } },
            {
              name: 'bulk:run',
              data: { id: 'last' },
              options: { delay: '1h' },
            },
          ]),
        );
        assert.strictEqual(await stored('BULK'), 0);
        await assert.rejects(
          publisher.mq.tree.addBulk([
            { name: 'cross:run', data: { id: 'first' } },
            { name: 'cross:run', data: { id: 'last' } },
          ]),
        );
        assert.strictEqual(await stored('SECOND'), 0);
      },
    );

    await manager.streams.update('BULK', { allow_msg_schedules: true });
    await manager.streams.update('SECOND', { allow_atomic: true });
    await t.test(
      'publishes native batches with delayed tasks in every position',
      { timeout: 8000 },
      async (t) => {
        const expected = new Map();
        const inspect = async (jobs, ack) => {
          assert.strictEqual(ack.stream, 'BULK');
          assert.strictEqual(ack.count, jobs.length);
          assert.ok(ack.batch);
          for (const [index, job] of jobs.entries()) {
            const message = await manager.streams.getMessage('BULK', {
              seq: ack.seq - jobs.length + index + 1,
            });
            assert.deepStrictEqual(message.json(), job.data);
            assert.strictEqual(
              message.header.get('Nats-Msg-Id'),
              job.options.msgID,
            );
            let deadline = 0;
            if (job.options.delay) {
              assert.match(message.subject, /^mq\.bulk\.run\.[^.]+$/);
              assert.strictEqual(
                message.header.get('Nats-Schedule-Target'),
                'mq.bulk.run',
              );
              deadline = Date.parse(
                message.header.get('Nats-Schedule').slice(4),
              );
              assert.ok(Number.isFinite(deadline));
            } else {
              assert.strictEqual(message.subject, 'mq.bulk.run');
            }
            expected.set(job.data.id, { msgID: job.options.msgID, deadline });
          }
        };
        assert.strictEqual(await publisher.mq.tree.addBulk([]), null);
        for (let position = 0; position < 3; position++) {
          const jobs = [0, 1, 2].map((index) => {
            const id = `${position}:${index}`;
            const options = { msgID: 'bulk:' + id };
            if (index === position) options.delay = '2s';
            return { name: 'bulk:run', data: { id }, options };
          });
          await inspect(jobs, await publisher.mq.tree.addBulk(jobs));
        }
        for (const delay of [undefined, '2s']) {
          const id = delay ? 'single-delayed' : 'single';
          const jobs = [
            {
              name: 'bulk:run',
              data: { id },
              options: { msgID: 'bulk:' + id, delay },
            },
          ];
          await inspect(jobs, await publisher.mq.tree.addBulk(jobs));
        }
        assert.strictEqual(await stored('BULK'), 11);
        const observed = [];
        const worker = await receive(
          'bulk',
          async (data, job) => {
            observed.push({
              id: data.id,
              receivedAt: Date.now(),
              msgID: job.headers.get('Nats-Msg-Id'),
            });
          },
          { stream: 'BULK' },
        );
        t.after(() => worker.stop());
        const jobs = ['short-first', 'short-last'].map((id) => ({
          name: 'run',
          data: { id },
          options: { msgID: 'bulk:' + id },
        }));
        for (const job of jobs) {
          expected.set(job.data.id, { msgID: job.options.msgID, deadline: 0 });
        }
        const ack = await worker.tree.bulk.addBulk(jobs);
        assert.strictEqual(ack.stream, 'BULK');
        assert.strictEqual(ack.count, 2);
        await waitFor(() => observed.length === expected.size, t.signal);
        assert.strictEqual(new Set(observed.map((item) => item.id)).size, 13);
        for (const item of observed) {
          const { deadline, msgID } = expected.get(item.id);
          assert.ok(item.receivedAt >= deadline);
          assert.strictEqual(item.msgID, deadline ? '' : msgID);
        }
        await waitFor(async () => {
          const count = await stored('BULK');
          return count === 0;
        }, t.signal);
        await worker.stop();
      },
    );

    await t.test('rejects duplicate IDs and cross-stream batches', async () => {
      const original = await publisher.mq.tree.add(
        'bulk:run',
        { original: true },
        { msgID: 'bulk:existing' },
      );
      const cases = [
        [
          { name: 'bulk:run', data: {}, options: { msgID: 'bulk:unique' } },
          { name: 'bulk:run', data: {}, options: { msgID: 'bulk:existing' } },
        ],
        [
          { name: 'bulk:run', data: {}, options: { msgID: 'bulk:repeat' } },
          { name: 'bulk:run', data: {}, options: { msgID: 'bulk:repeat' } },
        ],
        [
          { name: 'bulk:run', data: {} },
          { name: 'cross:run', data: {} },
        ],
      ];
      for (const jobs of cases) {
        await assert.rejects(publisher.mq.tree.addBulk(jobs));
        assert.strictEqual(await stored('BULK'), 1);
        assert.strictEqual(await stored('SECOND'), 0);
      }
      const message = await manager.streams.getMessage('BULK', {
        seq: original.seq,
      });
      assert.deepStrictEqual(message.json(), { original: true });
    });
    await manager.streams.delete('BULK');
    await manager.streams.delete('SECOND');
    await manager.streams.add({
      name: 'MQ',
      subjects: ['mq.>'],
      retention: 'workqueue',
      storage: 'file',
    });

    await t.test(
      'rejects delays when stream scheduling is disabled',
      async () => {
        await assert.rejects(
          publisher.mq.tree.add('delayed:run', {}, { delay: '1s' }),
          { code: 10188 },
        );
        assert.strictEqual(
          (await manager.streams.info('MQ')).state.messages,
          0,
        );
      },
    );

    await t.test(
      'stores before a worker starts and ACKs after completion',
      {
        timeout: 5000,
      },
      async (t) => {
        const data = { value: 42 };
        const ack = await publisher.mq.tree.add('ack:run', data);
        assert.strictEqual(
          (await manager.streams.info('MQ')).state.messages,
          1,
        );
        assert.deepStrictEqual(await publisher.mq.tree.getStatus('ack'), {
          stream: 'MQ',
          consumer: null,
          running: false,
          pending: 1,
          unacknowledged: 0,
        });
        assert.strictEqual(
          (await publisher.mq.tree.getStatus('empty')).pending,
          0,
        );
        assert.strictEqual(
          (await manager.streams.info('MQ')).state.consumer_count,
          0,
        );
        const entered = Promise.withResolvers();
        const finish = gate();
        const consumer = await receive(
          'ack',
          async (received, job) => {
            assert.deepStrictEqual(received, data);
            assert.strictEqual(job.name, 'run');
            assert.strictEqual(job.id, `MQ:${ack.seq}`);
            assert.strictEqual(job.retryCount, 0);
            entered.resolve();
            await finish.promise;
          },
          { maxAckPending: 1 },
        );
        await entered.promise;
        const info = await manager.consumers.info('MQ', 'mq-ack');
        assert.strictEqual(info.num_ack_pending, 1);
        const status = await consumer.tree.getStatus();
        assert.deepStrictEqual(status.ack, {
          stream: 'MQ',
          consumer: 'mq-ack',
          running: true,
          pending: 0,
          unacknowledged: 1,
        });
        assert.deepStrictEqual(await publisher.mq.tree.getStatus('ack'), {
          ...status.ack,
          running: false,
        });
        let stopped = false;
        const stopping = consumer.stop().then(() => {
          stopped = true;
        });
        await delay(20);
        assert.strictEqual(stopped, false);
        finish.resolve();
        await stopping;
        await settled('ack', t.signal);
        const finished = await publisher.mq.tree.getStatus('ack');
        assert.strictEqual(finished.pending, 0);
        assert.strictEqual(finished.unacknowledged, 0);
        assert.strictEqual(
          (await manager.streams.info('MQ')).state.messages,
          0,
        );
      },
    );

    await t.test(
      'deduplicates publications across the stream and preserves the payload',
      { timeout: 5000 },
      async (t) => {
        const options = { msgID: 'dedup:42' };
        const first = await publisher.mq.tree.add(
          'dedup:run',
          { value: 42 },
          options,
        );
        const duplicate = await publisher.mq.tree.add(
          'dedup:run',
          { value: 99 },
          options,
        );
        const otherQueue = await publisher.mq.tree.add(
          'other:run',
          { value: 100 },
          options,
        );
        assert.strictEqual(first.duplicate, false);
        for (const ack of [duplicate, otherQueue]) {
          assert.strictEqual(ack.duplicate, true);
          assert.strictEqual(ack.seq, first.seq);
        }
        assert.strictEqual(
          (await manager.streams.info('MQ')).state.messages,
          1,
        );
        const observed = [];
        const worker = await receive('dedup', async (data) => {
          observed.push(data);
        });
        await settled('dedup', t.signal);
        assert.deepStrictEqual(observed, [{ value: 42 }]);
        await worker.stop();
      },
    );

    await t.test(
      'delays tasks without holding workers or publisher connections',
      { timeout: 6000 },
      async (t) => {
        await manager.streams.update('MQ', { allow_msg_schedules: true });
        const observed = [];
        const completed = Promise.withResolvers();
        const method = async (data, job) => {
          observed.push({ data, job, receivedAt: Date.now() });
          if (observed.length === 2) completed.resolve();
        };
        const worker = await receive('delayed', method, { maxAckPending: 1 });
        const source = await runtime();
        const first = await source.mq.tree.add(
          'delayed:run',
          { id: 1 },
          { msgID: 'delayed:1', delay: '1500ms' },
        );
        const second = await worker.tree.delayed.run(
          { id: 2 },
          { msgID: 'delayed:2', delay: 2000 },
        );
        const duplicate = await worker.tree.delayed.add(
          'run',
          { id: 99 },
          { msgID: 'delayed:1', delay: '1h' },
        );
        assert.strictEqual(first.duplicate, false);
        assert.strictEqual(duplicate.duplicate, true);
        assert.strictEqual(duplicate.seq, first.seq);
        const deadlines = [];
        for (const ack of [first, second]) {
          const message = await manager.streams.getMessage('MQ', {
            seq: ack.seq,
          });
          assert.match(message.subject, /^mq\.delayed\.run\.[^.]+$/);
          assert.strictEqual(
            message.header.get('Nats-Schedule-Target'),
            'mq.delayed.run',
          );
          const timestamp = message.header.get('Nats-Schedule').slice(4);
          deadlines.push(Date.parse(timestamp));
        }
        const status = (await worker.tree.getStatus()).delayed;
        assert.strictEqual(status.pending, 0);
        assert.strictEqual(status.unacknowledged, 0);
        assert.strictEqual(observed.length, 0);
        await worker.stop();
        await source.mq.stop();
        await source.nats.close();
        const replacement = await receive('delayed', method);
        await completed.promise;
        await settled('delayed', t.signal);
        assert.deepStrictEqual(
          observed.map((item) => item.data.id).sort(),
          [1, 2],
        );
        for (const item of observed) {
          const index = item.data.id - 1;
          assert.ok(item.receivedAt >= deadlines[index]);
          assert.notStrictEqual(
            item.job.id,
            'MQ:' + [first, second][index].seq,
          );
        }
        await waitFor(
          async () => (await manager.streams.info('MQ')).state.messages === 0,
          t.signal,
        );
        await replacement.stop();
      },
    );

    await t.test(
      'redelivers a failed task with the same identity',
      {
        timeout: 5000,
      },
      async (t) => {
        const attempts = [];
        const errors = [];
        const completed = Promise.withResolvers();
        const consumer = await receive(
          'retry',
          async (data, job) => {
            attempts.push(job);
            if (attempts.length === 1) throw new Error('Retry this task');
            completed.resolve();
          },
          { maxAckPending: 1, retryDelay: 50 },
          {
            error: (...args) => errors.push(args),
          },
        );
        await publisher.mq.tree.add('retry:run', { value: 42 });
        await completed.promise;
        await settled('retry', t.signal);
        assert.strictEqual(attempts.length, 2);
        assert.strictEqual(attempts[0].id, attempts[1].id);
        assert.strictEqual(attempts[1].retryCount, 1);
        assert.strictEqual(errors.length, 1);
        const file = path.join(consumer.path, 'retry', '.queue.js');
        await writeFile(file, '({ retryLimit: 0 })');
        await consumer.change(file);
        const updated = await manager.consumers.info('MQ', 'mq-retry');
        assert.strictEqual(updated.config.max_deliver, 1);
        await consumer.stop();
      },
    );

    await t.test(
      'limits deliveries and retains exhausted tasks across worker restarts',
      { timeout: 8000 },
      async (t) => {
        for (const retryLimit of [0, 2]) {
          const queue = 'attempts' + retryLimit;
          const attempts = [];
          const completed = Promise.withResolvers();
          const method = async (data, job) => {
            if (data.success) {
              completed.resolve();
              return;
            }
            attempts.push(job.retryCount);
            throw new Error('Persistent failure');
          };
          const logger = { error() {} };
          const first = await receive(
            queue,
            method,
            { retryLimit, retryDelay: 20, maxAckPending: 1 },
            logger,
          );
          const ack = await publisher.mq.tree.add(queue + ':run', {});
          await settled(queue, t.signal);
          assert.deepStrictEqual(
            attempts,
            Array.from({ length: retryLimit + 1 }, (value, index) => index),
          );
          await first.stop();

          // Omitting retryLimit on another worker preserves the native limit.
          const second = await receive(queue, method, {}, logger);
          const info = await manager.consumers.info('MQ', 'mq-' + queue);
          assert.strictEqual(info.config.max_deliver, retryLimit + 1);
          await second.tree[queue].run({ success: true });
          await completed.promise;
          await settled(queue, t.signal);
          assert.strictEqual(attempts.length, retryLimit + 1);
          const stored = await manager.streams.getMessage('MQ', {
            seq: ack.seq,
          });
          assert.strictEqual(stored.subject, 'mq.' + queue + '.run');
          assert.deepStrictEqual(stored.json(), {});
          await second.stop();
        }
      },
    );

    await t.test(
      'shares maxAckPending across two workers',
      {
        timeout: 5000,
      },
      async (t) => {
        const finish = gate();
        const entered = Promise.withResolvers();
        let active = 0;
        let maximum = 0;
        let completed = 0;
        const method = async () => {
          active++;
          maximum = Math.max(maximum, active);
          if (active === 2) entered.resolve();
          await finish.promise;
          active--;
          completed++;
        };
        const options = { maxMessages: 50, maxAckPending: 2 };
        const first = await receive('limited', method, options);
        const second = await receive('limited', method, options);
        await waitFor(async () => {
          const info = await manager.consumers.info('MQ', 'mq-limited');
          return info.num_waiting >= 2;
        }, t.signal);
        for (let index = 0; index < 6; index++) {
          if (index % 2 === 0) await first.tree.limited.run({ index });
          else await first.tree.limited.add('run', { index });
        }
        await entered.promise;
        await delay(50);
        const info = await manager.consumers.info('MQ', 'mq-limited');
        assert.strictEqual(info.num_ack_pending, 2);
        assert.strictEqual(info.num_pending, 4);
        assert.strictEqual(maximum, 2);
        finish.resolve();
        await settled('limited', t.signal);
        assert.strictEqual(completed, 6);
        assert.strictEqual(maximum, 2);
        await Promise.all([first.stop(), second.stop()]);
      },
    );

    await t.test(
      'redelivers after a worker terminates without ACK',
      {
        timeout: 8000,
      },
      async (t) => {
        const ready = Promise.withResolvers();
        const received = Promise.withResolvers();
        const worker = new Worker(
          `
        const { parentPort, workerData } = require('node:worker_threads');
        const { connect } = require(workerData.transport);
        const { Nats } = require(workerData.nats);
        const { MQConsumer } = require(workerData.consumer);
        (async () => {
          const nats = new Nats({});
          nats.connection = await connect({
            servers: workerData.servers, reconnect: false,
          });
          const method = async (data, job) => {
            parentPort.postMessage({
              type: 'received', id: job.id, retryCount: job.retryCount,
            });
            await new Promise(() => {});
          };
          const consumer = await MQConsumer.create(
            nats, 'recovery', { run: { method } },
            { timeout: 30000, maxAckPending: 1 },
          );
          consumer.start();
          parentPort.postMessage({ type: 'ready' });
        })();
      `,
          {
            eval: true,
            workerData: {
              servers,
              transport: require.resolve('@nats-io/transport-node'),
              nats: require.resolve('../lib/nats.js'),
              consumer: require.resolve('../lib/mq-consumer.js'),
            },
          },
        );
        resources.workers.push(worker);
        worker.on('message', (message) => {
          if (message.type === 'ready') ready.resolve();
          if (message.type === 'received') received.resolve(message);
        });
        const failure = Promise.withResolvers();
        worker.once('error', (error) => failure.reject(error));
        await Promise.race([ready.promise, failure.promise]);
        // Redelivery must follow worker termination, not its local timeout.
        await manager.consumers.update('MQ', 'mq-recovery', {
          ack_wait: 1_000_000_000,
        });
        await publisher.mq.tree.add('recovery:run', { value: 42 });
        const original = await Promise.race([
          received.promise,
          failure.promise,
        ]);
        await worker.terminate();
        const completed = Promise.withResolvers();
        const consumer = await receive('recovery', async (data, job) => {
          completed.resolve(job);
        });
        const retried = await completed.promise;
        await settled('recovery', t.signal);
        assert.strictEqual(retried.id, original.id);
        assert.strictEqual(retried.retryCount, 1);
        assert.strictEqual(original.retryCount, 0);
        await consumer.stop();
      },
    );
    await t.test(
      'counts jobs before consumer creation, excluding schedules',
      async () => {
        await publisher.mq.tree.add('unstarted:run', {}, { delay: '1h' });
        assert.strictEqual(
          (await publisher.mq.tree.getStatus('unstarted')).pending,
          0,
        );
        await publisher.mq.tree.add('unstarted:run', {});
        await publisher.mq.tree.add('unstarted:other', {});
        const status = await publisher.mq.tree.getStatus('unstarted');
        assert.strictEqual(status.pending, 2);
        assert.strictEqual(status.consumer, null);
        await assert.rejects(manager.consumers.info('MQ', 'mq-unstarted'), {
          code: 10014,
        });
        await assert.rejects(
          publisher.mq.tree.getStatus('unstarted', { stream: 'MISSING' }),
          { code: 10059 },
        );
      },
    );
  },
);
