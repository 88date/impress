({
  retryLimit: 3,
  retryDelay: 1000,
  maxMessages: 5,
  cron: '0 3 * * *',
  tz: 'Europe/Moscow',
  data: { automatic: true },

  method: async (data) => ({ completed: true, data }),
  onCompleted: async () => {},
  onFailed: async () => {},
});
