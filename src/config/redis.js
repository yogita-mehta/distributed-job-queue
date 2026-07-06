require('dotenv').config();
const Redis = require('ioredis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

function createClient(label = 'client') {
  const client = new Redis(REDIS_URL, {
    retryStrategy(times) {
      const delay = Math.min(times * 200, 5000);
      return delay;
    },
    maxRetriesPerRequest: 3,
  });

  client.on('error', (err) => {
    console.error(`[redis:${label}] connection error:`, err.message);
  });

  return client;
}

module.exports = { createClient, REDIS_URL };
