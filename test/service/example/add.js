({
  access: 'public',

  parameters: {
    a: 'number',
    b: 'number',
  },

  method: async ({ a, b }) => a + b,

  returns: 'number',
});
