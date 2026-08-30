'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const {
  describeApi,
  describeServices,
  describeSchemas,
  describeQueues,
} = require('../lib/documentation.js');

const createProcedure = (exp, options = {}) => ({
  exports: exp,
  method: options.method,
  discovered: options.discovered || false,
});

test('lib/documentation - should describe application interfaces', () => {
  const apiMethod = createProcedure(
    {
      caption: 'Add numbers',
      description: 'Returns the sum',
      protocols: ['http'],
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
          caption: 'Add numbers',
          description: 'Returns the sum',
          protocols: ['http'],
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
      access: 'public',
      parameters: { text: 'string' },
      returns: 'string',
    },
    { method: () => 'sent' },
  );
  const remote = createProcedure(
    {
      caption: 'Create conversation',
      access: 'logged',
      parameters: { userId: 'string' },
      returns: { conversationId: 'string' },
    },
    { discovered: true },
  );
  const unavailable = createProcedure({ access: 'logged' });
  const services = describeServices({
    chat: {
      default: 1,
      1: { sendMessage: local },
      2: { createConversation: remote, unavailable },
    },
  });

  assert.deepStrictEqual(services, {
    chat: {
      1: {
        sendMessage: {
          caption: 'Send message',
          description: undefined,
          protocols: undefined,
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
          caption: 'Create conversation',
          description: undefined,
          protocols: undefined,
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
