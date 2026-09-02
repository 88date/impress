({
  cron: '0 3 * * *',
  tz: 'Europe/Moscow',
  data: { automatic: true },
  retryLimit: 2,
  localConcurrency: 1,

  method: async (data) => ({ completed: true, data }),
  onCompleted: async () => {},
  onFailed: async () => {},
});
