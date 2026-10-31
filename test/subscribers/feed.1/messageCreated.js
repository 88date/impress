/** @type {SubscriberDeclaration<{ messageId: number }>} */
({
  event: 'chat:1:message:created',
  maxMessages: 5,
  retryLimit: 5,
  retryDelay: 1000,
  timeout: 30000,

  method: async () => {},
});
