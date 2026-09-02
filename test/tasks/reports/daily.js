({
  cron: '0 8 * * *',

  method: async (data) => ({ completed: true, data }),
});
