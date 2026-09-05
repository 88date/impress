/** @type {EventDeclaration<{ messageId: number }>} */
({
  caption: 'Message created',
  description: 'A new chat message was created',
  examples: [{ messageId: 42 }],
  transports: ['local', 'nats'],
});
