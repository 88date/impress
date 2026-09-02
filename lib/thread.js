'use strict';

const { once } = require('node:events');
const { MessageChannel } = require('node:worker_threads');

const receive = async (port) => {
  const message = once(port, 'message');
  const disconnected = once(port, 'close').then(() => {
    throw new Error('Thread disconnected');
  });
  const args = await Promise.race([message, disconnected]);
  return args[0];
};

const request = async (thread, message) => {
  const { port1, port2 } = new MessageChannel();
  try {
    thread.postMessage({ ...message, port: port1 }, [port1]);
    const { error, result } = await receive(port2);
    if (error) throw new Error(error.message);
    return result;
  } finally {
    port2.close();
  }
};

module.exports = { request };
