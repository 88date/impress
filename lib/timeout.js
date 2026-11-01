'use strict';

const withTimeout = async (method, timeoutMs, errorMessage) => {
  const controller = new AbortController();
  let timer;
  try {
    const expired = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        const error = new Error(errorMessage);
        controller.abort(error);
        reject(error);
      }, timeoutMs);
    });
    const operation = Promise.resolve().then(() => method(controller.signal));
    return await Promise.race([operation, expired]);
  } finally {
    clearTimeout(timer);
  }
};

module.exports = { withTimeout };
