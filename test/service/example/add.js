({
  transports: ['nats'],
  access: 'public',
  timeout: 5000,

  parameters: {
    a: 'number',
    b: 'number',
  },

  method: async ({ a, b }) => a + b,

  returns: 'number',
});
