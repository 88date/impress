({
  retryLimit: 3,
  priority: 1,
  localConcurrency: 1,
  cron: '0 3 * * *',
  tz: 'Europe/Moscow',
  data: { automatic: true },

  method: async (data) => ({ completed: true, data }),
  onCompleted: async () => {},
  onFailed: async () => {},
});
