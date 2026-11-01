'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs/promises');
const path = require('node:path');
const { tmpdir } = require('node:os');
const { MQ } = require('../lib/mq.js');
const { MQConsumer } = require('../lib/mq-consumer.js');
const { Nats } = require('../lib/nats.js');

const acknowledgement = { stream: 'MQ', seq: 42, duplicate: false };

const createMQ = (request = () => assert.fail('Unexpected publication')) => {
  const application = { absolute: (name) => name };
  const nats = new Nats(application);
  nats.connection = { options: {}, request };
  application.nats = nats;
  return new MQ('mq', application);
};

test('mq - waits for storage ACK without local declarations', async () => {
  const entered = Promise.withResolvers();
  const response = Promise.withResolvers();
  const calls = [];
  const mq = createMQ((subject, data) => {
    calls.push([subject, data]);
    entered.resolve();
    return response.promise;
  });
  const data = { profile: { id: 42 } };
  let completed = false;
  const publishing = mq.tree
    .add('analysisProfile:profile', data)
    .then((result) => {
      completed = true;
      return result;
    });
  await entered.promise;
  try {
    assert.strictEqual(completed, false);
    assert.deepStrictEqual(calls, [
      ['mq.analysisProfile.profile', JSON.stringify(data)],
    ]);
    assert.strictEqual(mq.application.nats.jetstreamManager, null);
    assert.deepStrictEqual(Object.keys(mq.consumers), []);
  } finally {
    response.resolve({ data: Buffer.from(JSON.stringify(acknowledgement)) });
  }
  assert.deepStrictEqual(await publishing, acknowledgement);
  assert.deepStrictEqual(await mq.tree.getStatus(), {});
});

test('mq - preserves JSON, Unicode names and publish result', async () => {
  const mq = createMQ();
  const calls = [];
  mq.application.nats.jetstream = {
    publish: async (subject, data) => {
      calls.push([subject, data]);
      return acknowledgement;
    },
  };
  const values = [null, false, 0, 'text', [1, 2], { name: null }];
  for (const data of values) {
    const result = await mq.add('analysis:сomparison', data);
    assert.strictEqual(result, acknowledgement);
  }
  assert.deepStrictEqual(
    calls,
    values.map((data) => ['mq.analysis.сomparison', JSON.stringify(data)]),
  );
});

test('mq - sends message IDs through native SDK headers', async (t) => {
  const now = Date.parse('2026-11-01T10:00:00Z');
  t.mock.method(Date, 'now', () => now);
  const calls = [];
  const mq = createMQ(async (...args) => {
    calls.push(args);
    return { data: Buffer.from(JSON.stringify(acknowledgement)) };
  });
  for (const delay of [undefined, '30s']) {
    const result = await mq.add(
      'queue:job',
      { id: 42 },
      {
        msgID: 'profile:42',
        delay,
      },
    );
    assert.deepStrictEqual(result, acknowledgement);
    const [, data, { headers }] = calls.at(-1);
    assert.strictEqual(data, '{"id":42}');
    assert.strictEqual(headers.get('Nats-Msg-Id'), 'profile:42');
  }
  assert.strictEqual(calls[0][0], 'mq.queue.job');
  assert.strictEqual(calls[0][2].headers.has('Nats-Schedule'), false);
  const scheduled = calls[1][2].headers;
  assert.strictEqual(scheduled.get('Nats-Schedule-Target'), 'mq.queue.job');
  assert.strictEqual(
    scheduled.get('Nats-Schedule'),
    '@at 2026-11-01T10:00:30.000Z',
  );
});

test('mq - schedules unique messages through native SDK headers', async (t) => {
  const now = Date.parse('2026-11-01T10:00:00Z');
  t.mock.method(Date, 'now', () => now);
  const calls = [];
  const mq = createMQ(async (...args) => {
    calls.push(args);
    return { data: Buffer.from(JSON.stringify(acknowledgement)) };
  });
  const cases = [
    [25, 25],
    ['125ms', 125],
    ['30s', 30_000],
    ['15m', 900_000],
    ['2h', 7_200_000],
    ['7d', 604_800_000],
    ['30d', 2_592_000_000],
    ['30s', 30_000],
  ];
  for (const [delay, milliseconds] of cases) {
    const result = await mq.tree.add('queue:job', { id: 42 }, { delay });
    assert.deepStrictEqual(result, acknowledgement);
    const [subject, data, options] = calls.at(-1);
    assert.match(subject, /^mq\.queue\.job\.[0-9a-f-]{36}$/);
    assert.strictEqual(data, '{"id":42}');
    assert.strictEqual(options.headers.has('Nats-Msg-Id'), false);
    assert.strictEqual(
      options.headers.get('Nats-Schedule-Target'),
      'mq.queue.job',
    );
    assert.strictEqual(
      options.headers.get('Nats-Schedule'),
      '@at ' + new Date(now + milliseconds).toISOString(),
    );
  }
  assert.strictEqual(
    new Set(calls.map(([subject]) => subject)).size,
    cases.length,
  );
  assert.strictEqual(mq.application.nats.jetstreamManager, null);
});

test('mq - zero or absent delay publishes directly', async () => {
  const mq = createMQ(async (subject, data, { headers }) => {
    assert.strictEqual(subject, 'mq.queue.job');
    assert.strictEqual(data, '{"id":42}');
    assert.strictEqual(headers.has('Nats-Schedule'), false);
    assert.strictEqual(headers.has('Nats-Schedule-Target'), false);
    assert.strictEqual(headers.has('Nats-Msg-Id'), false);
    return { data: Buffer.from(JSON.stringify(acknowledgement)) };
  });
  for (const options of [
    undefined,
    {},
    { delay: undefined },
    { delay: 0 },
    { delay: '0ms' },
    { delay: '0s' },
  ]) {
    assert.deepStrictEqual(
      await mq.add('queue:job', { id: 42 }, options),
      acknowledgement,
    );
  }
});

test('mq - bulk preserves message headers in one native batch', async (t) => {
  const now = Date.parse('2026-11-01T10:00:00Z');
  t.mock.method(Date, 'now', () => now);
  const entries = [
    ['first', '30s', 30_000],
    ['plain', undefined, 0],
    ['middle', '15m', 900_000],
    ['last', '2h', 7_200_000],
  ];
  const items = entries.map(([name, delay], index) => ({
    name: 'queue:' + name,
    data: { index },
    options: { delay, msgID: name },
  }));
  const ack = { ...acknowledgement, batch: '', count: items.length };
  const calls = [];
  const mq = createMQ(async (subject, payload, { headers }) => {
    calls.push(['request', subject, payload, headers]);
    if (!headers.has('Nats-Batch-Commit')) {
      ack.batch = headers.get('Nats-Batch-Id');
      return { data: Buffer.alloc(0) };
    }
    return { data: Buffer.from(JSON.stringify(ack)), json: () => ack };
  });
  mq.application.nats.connection.publish = (subject, payload, { headers }) => {
    calls.push(['publish', subject, payload, headers]);
  };
  assert.strictEqual(await mq.tree.addBulk(items), ack);
  assert.ok(ack.batch);
  assert.deepStrictEqual(
    calls.map(([method]) => method),
    ['request', 'publish', 'publish', 'request'],
  );
  for (const [index, [, subject, payload, headers]] of calls.entries()) {
    const [name, , delay] = entries[index];
    const target = 'mq.queue.' + name;
    assert.strictEqual(payload, JSON.stringify({ index }));
    assert.strictEqual(headers.get('Nats-Msg-Id'), name);
    assert.strictEqual(headers.get('Nats-Batch-Id'), ack.batch);
    assert.strictEqual(headers.get('Nats-Batch-Sequence'), String(index + 1));
    assert.strictEqual(headers.has('Nats-Batch-Commit'), index === 3);
    if (delay) {
      assert.ok(subject.startsWith(target + '.'));
      assert.strictEqual(headers.get('Nats-Schedule-Target'), target);
      assert.strictEqual(
        headers.get('Nats-Schedule'),
        '@at ' + new Date(now + delay).toISOString(),
      );
    } else {
      assert.strictEqual(subject, target);
      assert.strictEqual(headers.has('Nats-Schedule'), false);
    }
  }
  assert.strictEqual(calls.at(-1)[3].get('Nats-Batch-Commit'), '1');
  assert.strictEqual(mq.application.nats.jetstreamManager, null);
});

test('mq - empty bulk skips NATS and one item remains a batch', async (t) => {
  const ack = { ...acknowledgement, batch: '', count: 1 };
  const request = t.mock.fn(async (subject, payload, { headers }) => {
    ack.batch = headers.get('Nats-Batch-Id');
    return { data: Buffer.from(JSON.stringify(ack)), json: () => ack };
  });
  const mq = createMQ(request);
  assert.strictEqual(await mq.tree.addBulk([]), null);
  assert.strictEqual(request.mock.callCount(), 0);
  const result = await mq.addBulk([{ name: 'queue:job', data: { id: 42 } }]);
  assert.deepStrictEqual(result, ack);
  assert.ok(ack.batch);
  assert.strictEqual(request.mock.callCount(), 1);
  const [subject, payload, { headers }] = request.mock.calls[0].arguments;
  assert.strictEqual(subject, 'mq.queue.job');
  assert.strictEqual(payload, '{"id":42}');
  assert.strictEqual(headers.get('Nats-Batch-Sequence'), '1');
  assert.strictEqual(headers.get('Nats-Batch-Commit'), '1');
  assert.strictEqual(mq.application.nats.jetstreamManager, null);
});

test('mq - bulk validates every item before publication', async (t) => {
  const request = t.mock.fn(() => assert.fail('Unexpected request'));
  const mq = createMQ(request);
  const publish = t.mock.fn(() => assert.fail('Unexpected publication'));
  mq.application.nats.connection.publish = publish;
  const valid = { name: 'queue:job', data: {} };
  const invalid = [
    { ...valid, name: 'invalid' },
    { ...valid, data: undefined },
    { ...valid, options: { delay: -1 } },
    { ...valid, options: { priority: 1 } },
  ];
  for (const item of invalid) {
    await assert.rejects(mq.addBulk([valid, item]), { name: 'Error' });
  }
  assert.strictEqual(request.mock.callCount(), 0);
  assert.strictEqual(publish.mock.callCount(), 0);
});

test('mq - rejects invalid delays before publishing', async (t) => {
  const request = t.mock.fn(async () => ({
    data: Buffer.from(JSON.stringify(acknowledgement)),
  }));
  const mq = createMQ(request);
  const delays = [
    -1,
    NaN,
    Infinity,
    1.5,
    8_640_000_000_000_000,
    Number.MAX_SAFE_INTEGER + 1,
    null,
    false,
    {},
    [],
    '',
    '0',
    '30',
    '-1s',
    '1.5ms',
    '0.5s',
    '1.001s',
    '0.0001s',
    '1h30m',
    ' 1s',
    '1s ',
    '1s\n',
    '1S',
    'forever',
    '999999999999999999999999999999d',
  ];
  for (const delay of delays) {
    await assert.rejects(mq.add('queue:job', {}, { delay }), { name: 'Error' });
  }
  assert.strictEqual(request.mock.callCount(), 0);
});

test('mq - rejects malformed task names before publication', async () => {
  const mq = createMQ();
  const names = [
    undefined,
    null,
    42,
    '',
    'queue',
    ':job',
    'queue:',
    'queue:job:extra',
    'queue.name:job',
    'queue:job.name',
    'queue:*',
    'queue:>',
    'queue:jo b',
    'queue:job\n',
  ];
  for (const name of names) {
    await assert.rejects(mq.add(name, {}), {
      name: 'Error',
      message: 'MQ task name must have the form queue:job',
    });
  }
});

test('mq - rejects data that cannot produce a JSON message', async () => {
  const mq = createMQ();
  for (const data of [undefined, () => {}, Symbol(), { toJSON: () => {} }]) {
    await assert.rejects(mq.add('queue:job', data), {
      name: 'Error',
      message: 'MQ data must be JSON-serializable',
    });
  }
  const circular = {};
  circular.self = circular;
  for (const data of [circular, 1n]) {
    await assert.rejects(mq.add('queue:job', data), Error);
  }
});

test('mq - propagates missing stream and permission failures', async () => {
  for (const message of ['Stream not found', 'Permission denied']) {
    const failure = new Error(message);
    const mq = createMQ(async () => {
      throw failure;
    });
    await assert.rejects(mq.add('queue:job', {}), (error) => error === failure);
  }
});

test('mq - rejects when disabled, disconnected or stopping', async () => {
  const mq = createMQ();
  const nats = mq.application.nats;
  mq.application.nats = null;
  await assert.rejects(mq.add('queue:job', {}), /NATS is disabled/);
  const items = [{ name: 'queue:job', data: {} }];
  await assert.rejects(mq.addBulk(items), /NATS is disabled/);
  mq.application.nats = nats;
  nats.isStopping = true;
  await assert.rejects(mq.add('queue:job', {}), /not available/);
  await assert.rejects(mq.addBulk(items), /not available/);
  nats.isStopping = false;
  nats.connection = null;
  await assert.rejects(mq.add('queue:job', {}), /not available/);
  mq.closing = true;
  await assert.rejects(mq.add('queue:job', {}), /MQ is stopping/);
  await assert.rejects(mq.addBulk(items), /MQ is stopping/);
});

test('mq - named status selects stream and local reader', async (t) => {
  const mq = createMQ();
  const info = t.mock.fn(async () => ({
    name: 'mq-caf%C3%A9',
    num_pending: 7,
    num_ack_pending: 2,
  }));
  t.mock.method(mq.application.nats, 'getJetStream', async () => ({
    manager: { consumers: { info } },
  }));
  assert.deepStrictEqual(await mq.tree.getStatus('café'), {
    stream: 'MQ',
    consumer: 'mq-caf%C3%A9',
    running: false,
    pending: 7,
    unacknowledged: 2,
  });
  assert.deepStrictEqual(info.mock.calls[0].arguments, ['MQ', 'mq-caf%C3%A9']);
  assert.deepStrictEqual(Object.keys(mq.consumers), []);
  mq.configs.café = { stream: 'CUSTOM' };
  mq.consumers.café = { stream: 'CUSTOM', running: true };
  assert.strictEqual((await mq.tree.getStatus('café')).running, true);
  const overridden = await mq.tree.getStatus('café', { stream: 'OTHER' });
  assert.strictEqual(overridden.stream, 'OTHER');
  assert.strictEqual(overridden.running, false);
  mq.consumers.café.running = false;
  assert.strictEqual((await mq.tree.getStatus('café')).running, false);
  assert.deepStrictEqual(
    info.mock.calls.slice(1).map((call) => call.arguments),
    [
      ['CUSTOM', 'mq-caf%C3%A9'],
      ['OTHER', 'mq-caf%C3%A9'],
      ['CUSTOM', 'mq-caf%C3%A9'],
    ],
  );
});

test('mq - counts queued subjects without a consumer', async (t) => {
  const mq = createMQ();
  const missing = Object.assign(new Error('Consumer not found'), {
    code: 10014,
  });
  let subjects;
  const inspect = t.mock.fn(async () => ({ state: { subjects } }));
  t.mock.method(mq.application.nats, 'getJetStream', async () => ({
    manager: {
      consumers: {
        info: async () => {
          throw missing;
        },
      },
      streams: { info: inspect },
    },
  }));
  const cases = [
    [undefined, 0],
    [{}, 0],
    [{ 'mq.queue.work': 3, 'mq.queue.other': 5 }, 8],
  ];
  for (const [counts, pending] of cases) {
    subjects = counts;
    assert.deepStrictEqual(await mq.tree.getStatus('queue'), {
      stream: 'MQ',
      consumer: null,
      running: false,
      pending,
      unacknowledged: 0,
    });
    assert.deepStrictEqual(inspect.mock.calls.at(-1).arguments, [
      'MQ',
      { subjects_filter: 'mq.queue.*' },
    ]);
  }
  assert.deepStrictEqual(Object.keys(mq.consumers), []);
});

test('mq - named status propagates lookup failures', async (t) => {
  const missing = Object.assign(new Error('Consumer not found'), {
    code: 10014,
  });
  const streamError = Object.assign(new Error('Stream not found'), {
    code: 10059,
  });
  const cases = [
    ['consumer', new Error('Permission denied')],
    ['consumer', streamError],
    ['consumer', Object.assign(new Error('Other error'), { code: '10014' })],
    ['stream', streamError],
  ];
  for (const [target, error] of cases) {
    const mq = createMQ();
    const inspect = t.mock.fn(async () => {
      throw error;
    });
    t.mock.method(mq.application.nats, 'getJetStream', async () => ({
      manager: {
        consumers: {
          info: async () => {
            throw target === 'consumer' ? error : missing;
          },
        },
        streams: { info: inspect },
      },
    }));
    await assert.rejects(
      mq.tree.getStatus('queue'),
      (reason) => reason === error,
    );
    assert.strictEqual(inspect.mock.callCount(), target === 'stream' ? 1 : 0);
  }
});

test('mq - invalid status names fail before NATS access', async (t) => {
  const mq = createMQ();
  const access = t.mock.method(mq.application.nats, 'getJetStream', () => {
    assert.fail('Unexpected NATS access');
  });
  for (const name of ['', 'queue:job', 'queue.name', 'queue*', 'queue >']) {
    await assert.rejects(mq.tree.getStatus(name), { name: 'Error' });
  }
  assert.strictEqual(access.mock.callCount(), 0);
  mq.application.nats = null;
  await assert.rejects(mq.tree.getStatus('queue'), {
    name: 'Error',
    message: 'NATS is disabled',
  });
});

const loadMQ = async (t, files, kind = 'worker') => {
  const directory = await fs.mkdtemp(path.join(tmpdir(), 'impress-mq-'));
  const errors = [];
  const publish = t.mock.fn(async () => acknowledgement);
  const application = {
    absolute: (name) => path.join(directory, name),
    kind,
    sandbox: {},
    watcher: { watch: () => {} },
    console: { error: (error) => errors.push(error) },
    nats: { getJetStreamClient: () => ({ publish }) },
  };
  const mq = new MQ('mq', application);
  t.after(async () => {
    await mq.stop();
    assert.strictEqual(path.dirname(directory), path.resolve(tmpdir()));
    assert.ok(path.basename(directory).startsWith('impress-mq-'));
    await fs.rm(directory, { recursive: true, force: true });
  });
  const write = async (name, source) => {
    const file = path.join(mq.path, name);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, source);
    return file;
  };
  for (const [name, source] of Object.entries(files)) await write(name, source);
  const readers = [];
  const createReader = async (nats, name, handlers, options = {}) => {
    const reader = {
      handlers,
      stream: options.stream ?? 'MQ',
      running: false,
      consumer: {
        info: async () => ({
          name: 'mq-' + name,
          num_pending: 7,
          num_ack_pending: 2,
        }),
        delete: t.mock.fn(),
      },
      start: t.mock.fn(() => {
        reader.running = true;
      }),
      stop: t.mock.fn(async () => {
        reader.running = false;
      }),
    };
    readers.push(reader);
    return reader;
  };
  const create = t.mock.method(MQConsumer, 'create', createReader);
  return { mq, write, readers, errors, create, createReader, publish };
};

test('mq - loads queue settings and publishes both APIs', async (t) => {
  const { mq, create, publish } = await loadMQ(t, {
    'direct/send.js': '({ method: async () => 42 })',
    'custom/.queue.js': '({ stream: "CUSTOM", maxAckPending: 2 })',
    'custom/process.js': '({ method: async () => {} })',
    'producer/.queue.js': '({ maxMessages: 10 })',
  });
  await mq.load();
  assert.strictEqual(create.mock.callCount(), 0);
  await mq.start();
  assert.strictEqual(create.mock.callCount(), 2);
  assert.deepStrictEqual(Object.keys(mq.consumers).sort(), [
    'custom',
    'direct',
  ]);
  const custom = create.mock.calls.find(
    (call) => call.arguments[1] === 'custom',
  );
  assert.strictEqual(custom.arguments[3].maxAckPending, 2);
  const data = { id: 42 };
  assert.strictEqual(await mq.tree.direct.send(data), acknowledgement);
  assert.strictEqual(await mq.tree.direct.add('send', data), acknowledgement);
  assert.strictEqual(await mq.tree.add('direct:send', data), acknowledgement);
  for (const call of publish.mock.calls) {
    assert.deepStrictEqual(call.arguments.slice(0, 2), [
      'mq.direct.send',
      '{"id":42}',
    ]);
  }
  assert.strictEqual(publish.mock.callCount(), 3);
  assert.deepStrictEqual((await mq.tree.getStatus()).custom, {
    stream: 'CUSTOM',
    consumer: 'mq-custom',
    running: true,
    pending: 7,
    unacknowledged: 2,
  });
});

test('mq - only workers consume local handlers', async (t) => {
  for (const kind of ['server', 'balancer']) {
    await t.test(kind, async (t) => {
      const { mq, create } = await loadMQ(
        t,
        { 'queue/work.js': '({ method: async () => {} })' },
        kind,
      );
      await mq.load();
      await mq.start();
      assert.strictEqual(typeof mq.tree.queue.work, 'function');
      assert.strictEqual(create.mock.callCount(), 0);
    });
  }
});

test('mq - keeps the active handler after an invalid reload', async (t) => {
  const { mq, write, readers, create, errors } = await loadMQ(t, {
    'queue/work.js': '({ method: async () => 1 })',
  });
  await mq.load();
  await mq.start();
  const reader = readers[0];
  const file = await write('queue/work.js', '({ method: async () => 2 })');
  await mq.change(file);
  assert.strictEqual(await reader.handlers.work.method(), 2);
  assert.strictEqual(create.mock.callCount(), 1);
  assert.strictEqual(reader.stop.mock.callCount(), 0);
  await write('queue/work.js', '({ method:');
  await mq.change(file);
  assert.strictEqual(errors.length, 1);
  assert.strictEqual(await reader.handlers.work.method(), 2);
  assert.strictEqual(mq.consumers.queue, reader);
  const config = await write('queue/.queue.js', '({ concurrency: 2 })');
  await mq.change(config);
  assert.strictEqual(errors.length, 2);
  assert.strictEqual(mq.consumers.queue, reader);
  await write('queue/.queue.js', '({ maxAckPending: 2 })');
  await mq.change(config);
  assert.strictEqual(reader.stop.mock.callCount(), 1);
  assert.strictEqual(create.mock.callCount(), 2);
  assert.strictEqual(await readers[1].handlers.work.method(), 2);
});

test('mq - failed reconfiguration preserves the active consumer', async (t) => {
  const { mq, write, readers, create, errors } = await loadMQ(t, {
    'queue/.queue.js': '({ maxAckPending: 2 })',
    'queue/work.js': '({ method: async () => 42 })',
  });
  await mq.load();
  await mq.start();
  const config = mq.configs.queue;
  const reader = readers[0];
  create.mock.mockImplementation(async () => {
    throw new Error('Stream not found');
  });
  const file = await write('queue/.queue.js', '({ stream: "MISSING" })');
  await mq.change(file);
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0].message, /Stream not found/);
  assert.strictEqual(mq.configs.queue, config);
  assert.strictEqual(mq.consumers.queue, reader);
  assert.strictEqual(reader.stop.mock.callCount(), 0);
  assert.strictEqual(reader.running, true);
  assert.strictEqual(await reader.handlers.work.method(), 42);
});

test('mq - rejects unsupported declarations before starting', async (t) => {
  const cases = [
    ['queue/.queue.js', '({ concurrency: 1 })', /Unsupported MQ queue option/],
    [
      'queue/work.js',
      '({ attempts: 3, method: async () => {} })',
      /Unsupported MQ handler option/,
    ],
    ['add/work.js', '({ method: async () => {} })', /reserved queue name/],
    ['addBulk/work.js', '({ method: async () => {} })', /reserved queue name/],
    [
      'queue/addBulk.js',
      '({ method: async () => {} })',
      /reserved MQ job name/,
    ],
  ];
  for (const [file, source, error] of cases) {
    await t.test(file, async (t) => {
      const { mq, create } = await loadMQ(t, { [file]: source });
      await mq.load();
      await assert.rejects(mq.start(), error);
      assert.strictEqual(create.mock.callCount(), 0);
    });
  }
});

test('mq - forwards valid retry limits from queue declarations', async (t) => {
  for (const retryLimit of [-1, 0, 2, Number.MAX_SAFE_INTEGER - 1]) {
    await t.test(String(retryLimit), async (t) => {
      const { mq, create } = await loadMQ(t, {
        'queue/.queue.js': `({ retryLimit: ${retryLimit} })`,
        'queue/work.js': '({ method: async () => {} })',
      });
      await mq.load();
      await mq.start();
      assert.strictEqual(create.mock.callCount(), 1);
      assert.strictEqual(
        create.mock.calls[0].arguments[3].retryLimit,
        retryLimit,
      );
    });
  }
});

test('mq - rejects invalid retry limits before starting', async (t) => {
  const values = [
    '-2',
    '1.5',
    '"2"',
    'NaN',
    'Infinity',
    'Number.MAX_SAFE_INTEGER',
  ];
  for (const value of values) {
    await t.test(value, async (t) => {
      const { mq, create } = await loadMQ(t, {
        'queue/.queue.js': `({ retryLimit: ${value} })`,
        'queue/work.js': '({ method: async () => {} })',
      });
      await mq.load();
      await assert.rejects(mq.start(), /Invalid MQ queue option: retryLimit/);
      assert.strictEqual(create.mock.callCount(), 0);
    });
  }
});

test('mq - short APIs forward string and numeric delays', async (t) => {
  const now = Date.parse('2026-11-01T10:00:00Z');
  t.mock.method(Date, 'now', () => now);
  const { mq, publish } = await loadMQ(t, {
    'queue/work.js': '({ method: async () => {} })',
  });
  await mq.load();
  const data = { id: 42 };
  await mq.tree.queue.work(data, { delay: '15m' });
  await mq.tree.queue.add('work', data, { delay: 250 });
  assert.strictEqual(publish.mock.callCount(), 2);
  assert.deepStrictEqual(
    publish.mock.calls.map((call) => call.arguments[2].schedule),
    [900_000, 250].map((delay) => ({
      specification: { at: new Date(now + delay) },
      target: 'mq.queue.work',
    })),
  );
});

test('mq - all publication APIs forward message IDs unchanged', async (t) => {
  const { mq, publish } = await loadMQ(t, {
    'queue/work.js': '({ method: async () => {} })',
  });
  await mq.load();
  for (const delay of [undefined, '30s']) {
    const options = { msgID: 'profile:42', delay };
    await mq.tree.add('queue:work', {}, options);
    await mq.tree.queue.work({}, options);
    await mq.tree.queue.add('work', {}, options);
  }
  assert.strictEqual(publish.mock.callCount(), 6);
  for (const call of publish.mock.calls) {
    assert.strictEqual(call.arguments[2].msgID, 'profile:42');
  }
});

test('mq - short bulk API prefixes names and forwards options', async (t) => {
  const { mq } = await loadMQ(t, { 'queue/.queue.js': '({})' });
  await mq.load();
  const ack = { ...acknowledgement, batch: 'batch-1', count: 2 };
  const add = t.mock.method(mq, 'addBulk', async () => ack);
  const items = [
    { name: 'first', data: { id: 42 }, options: { delay: '1s', msgID: 'one' } },
    { name: 'other', data: null },
  ];
  assert.strictEqual(await mq.tree.queue.addBulk(items), ack);
  assert.strictEqual(add.mock.callCount(), 1);
  const [forwarded] = add.mock.calls[0].arguments;
  assert.deepStrictEqual(
    forwarded.map(({ name }) => name),
    ['queue:first', 'queue:other'],
  );
  assert.strictEqual(forwarded[0].data, items[0].data);
  assert.strictEqual(forwarded[0].options, items[0].options);
  assert.strictEqual(forwarded[1].data, null);
  assert.deepStrictEqual(
    items.map(({ name }) => name),
    ['first', 'other'],
  );
});

test('mq - rejects unsupported publication options in both APIs', async (t) => {
  const { mq, publish } = await loadMQ(t, {
    'queue/work.js': '({ method: async () => {} })',
  });
  await mq.load();
  for (const options of [{ priority: 1 }, { jobId: 'custom' }]) {
    const error = /MQ publication option/;
    await assert.rejects(mq.tree.add('queue:work', {}, options), error);
    await assert.rejects(mq.tree.queue.work({}, options), error);
    await assert.rejects(mq.tree.queue.add('work', {}, options), error);
  }
  assert.strictEqual(publish.mock.callCount(), 0);
});

test('mq - stopping during consumer creation never starts it', async (t) => {
  const { mq, create, createReader, readers } = await loadMQ(t, {
    'queue/work.js': '({ method: async () => {} })',
  });
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  create.mock.mockImplementation(async (...args) => {
    const reader = await createReader(...args);
    entered.resolve();
    await release.promise;
    return reader;
  });
  await mq.load();
  const starting = mq.start();
  await entered.promise;
  const stopping = mq.stop();
  release.resolve();
  await Promise.all([starting, stopping]);
  assert.strictEqual(readers[0].start.mock.callCount(), 0);
  assert.strictEqual(readers[0].stop.mock.callCount(), 1);
  assert.deepStrictEqual(Object.keys(mq.consumers), []);
});

test('mq - deleting a folder or last job stops only its reader', async (t) => {
  for (const target of ['queue', 'queue/work.js']) {
    await t.test(target, async (t) => {
      const { mq, readers } = await loadMQ(t, {
        'queue/work.js': '({ method: async () => {} })',
      });
      await mq.load();
      await mq.start();
      await mq.delete(path.join(mq.path, target));
      assert.strictEqual(readers[0].stop.mock.callCount(), 1);
      assert.strictEqual(readers[0].consumer.delete.mock.callCount(), 0);
      assert.strictEqual(mq.tree.queue, undefined);
      assert.deepStrictEqual(Object.keys(mq.consumers), []);
    });
  }
});
