/** @type {SubscriberDeclaration<{ payload: { messageId: number } }>} */
({
  stream: 'EVENTS',
  subject: 'events.chat.1.message.created',
  maxMessages: 5,
  maxAckPending: 50,
  retryLimit: 5,
  retryDelay: 1000,
  timeout: 30000,

  method: async () => {},
});
