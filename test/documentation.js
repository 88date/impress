'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const {
  describeApi,
  describeServices,
  describeSchemas,
  describeQueues,
  describeEvents,
} = require('../lib/documentation.js');

const createProcedure = (exp, options = {}) => ({
  exports: exp,
  method: options.method,
  transports: exp.transports,
  discovered: options.discovered || false,
});

test('lib/documentation - should describe application interfaces', () => {
  const apiMethod = createProcedure(
    {
      caption: 'Add numbers',
      description: 'Returns the sum',
      protocols: ['http'],
      transports: ['http'],
      roles: ['admin'],
      access: 'public',
      parameters: { a: 'number', b: 'number' },
      deprecated: false,
      returns: 'number',
      errors: { EADD: 'Addition failed' },
      examples: [{ parameters: { a: 2, b: 3 } }],
    },
    { method: () => 5 },
  );
  const apiMetadata = createProcedure({ raw: true }, { method: true });
  const api = describeApi({
    example: {
      default: 1,
      1: { add: apiMethod, raw: apiMetadata },
    },
  });

  assert.deepStrictEqual(api, {
    example: {
      1: {
        add: {
          origin: 'local',
          caption: 'Add numbers',
          description: 'Returns the sum',
          protocols: ['http'],
          transports: ['http'],
          roles: ['admin'],
          access: 'public',
          parameters: { a: 'number', b: 'number' },
          deprecated: false,
          returns: 'number',
          errors: { EADD: 'Addition failed' },
          example: { a: 2, b: 3 },
        },
      },
    },
  });

  const local = createProcedure(
    {
      caption: 'Send message',
      transports: ['nats'],
      access: 'public',
      parameters: { text: 'string' },
      returns: 'string',
    },
    { method: () => 'sent' },
  );
  const remote = createProcedure(
    {
      caption: 'Create conversation',
      transports: ['nats'],
      access: 'logged',
      parameters: { userId: 'string' },
      returns: { conversationId: 'string' },
    },
    { discovered: true },
  );
  const unavailable = createProcedure({ access: 'logged' });
  const serviceCollection = {
    chat: {
      default: 1,
      1: { sendMessage: local },
      2: { createConversation: remote, unavailable },
    },
  };
  const services = describeServices(serviceCollection);

  assert.deepStrictEqual(services, {
    chat: {
      1: {
        sendMessage: {
          origin: 'local',
          caption: 'Send message',
          description: undefined,
          protocols: undefined,
          transports: ['nats'],
          roles: undefined,
          access: 'public',
          parameters: { text: 'string' },
          deprecated: undefined,
          returns: 'string',
          errors: undefined,
          example: null,
        },
      },
      2: {
        createConversation: {
          origin: 'remote',
          caption: 'Create conversation',
          description: undefined,
          protocols: undefined,
          transports: ['nats'],
          roles: undefined,
          access: 'logged',
          parameters: { userId: 'string' },
          deprecated: undefined,
          returns: { conversationId: 'string' },
          errors: undefined,
          example: null,
        },
      },
    },
  });

  const discovered = new Map([
    ['chat', new Map([['1.sendMessage', local.exports]])],
  ]);
  const availableServices = describeServices(serviceCollection, discovered);

  assert.deepStrictEqual(availableServices, {
    chat: { 1: { sendMessage: services.chat[1].sendMessage } },
  });

  const schemas = describeSchemas(
    new Map([
      ['ChatMessage', { id: 'string', text: '?string' }],
      [
        'Friend',
        {
          Projection: {
            schema: 'Profile',
            fields: ['id', 'firstName'],
          },
        },
      ],
    ]),
  );

  assert.deepStrictEqual(schemas, {
    ChatMessage: { id: 'string', text: '?string' },
    Friend: {
      Projection: {
        schema: 'Profile',
        fields: ['id', 'firstName'],
      },
    },
  });

  const queues = describeQueues(
    {
      analysis: {
        concurrency: 3,
        defaultJobOptions: {
          removeOnComplete: 1000,
          removeOnFail: 5000,
        },
        schedules: [{ name: 'enqueueRating', every: 1200000 }],
        events: { completed: () => {} },
      },
    },
    {
      analysis: {
        updateRating: {
          caption: 'Save rating analysis',
          parameters: { userId: 'string', rating: 'number' },
          method: async () => {},
          onCompleted: () => {},
        },
        enqueueRating: { method: async () => {} },
      },
    },
  );

  assert.deepStrictEqual(queues, {
    analysis: {
      concurrency: 3,
      defaultJobOptions: {
        removeOnComplete: 1000,
        removeOnFail: 5000,
      },
      schedules: [{ name: 'enqueueRating', every: 1200000 }],
      events: {},
      workers: {
        updateRating: {
          caption: 'Save rating analysis',
          parameters: { userId: 'string', rating: 'number' },
        },
        enqueueRating: {},
      },
    },
  });
});

test('lib/documentation - event origin is relative to the application', () => {
  const local = Object.freeze({
    name: 'chat:1:created',
    transports: ['local', 'nats'],
    caption: 'Local declaration',
  });
  const sameName = { ...local, caption: 'Discovered declaration' };
  const remote = Object.freeze({
    name: 'profile:1:updated',
    origin: 'local',
  });
  const discovered = new Map([
    [remote.name, remote],
    [sameName.name, sameName],
  ]);

  assert.deepStrictEqual(describeEvents([local], discovered), [
    { ...local, origin: 'local' },
    { ...remote, transports: ['nats'], origin: 'remote' },
  ]);
  assert.strictEqual(discovered.get(local.name), sameName);
  assert.deepStrictEqual(describeEvents(), []);
});
