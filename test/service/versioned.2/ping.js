({
  transports: ['nats'],
  access: 'public',
  timeout: 5000,

  method: async () => 'pong',

  returns: 'string',
});
