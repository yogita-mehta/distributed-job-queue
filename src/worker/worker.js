require('dotenv').config();
const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;
app.get('/health', (req, res) => res.status(200).send('OK'));
app.listen(PORT, () => console.log(`Worker health check on port ${PORT}`));

const { createClient } = require('../config/redis');
const { JobQueue } = require('../queue/JobQueue');
const { runHandler } = require('./jobHandlers');

const POLL_INTERVAL_MS = Number(process.env.WORKER_POLL_INTERVAL_MS || 250);
const STALLED_TIMEOUT_MS = Number(process.env.STALLED_JOB_TIMEOUT_MS || 15000);
const STALLED_CHECK_INTERVAL_MS = Number(process.env.STALLED_CHECK_INTERVAL_MS || 5000);

const WORKER_ID = `worker-${process.pid}`;
let running = true;

async function main() {
  const redis = createClient(WORKER_ID);
  const queue = new JobQueue(redis, {
    baseRetryDelayMs: Number(process.env.BASE_RETRY_DELAY_MS || 1000),
    maxRetryDelayMs: Number(process.env.MAX_RETRY_DELAY_MS || 30000),
  });

  console.log(`[${WORKER_ID}] started, polling every ${POLL_INTERVAL_MS}ms`);

  // Background maintenance loop: promote ready delayed jobs, recover stalled ones.
  const maintenance = setInterval(async () => {
    try {
      const promoted = await queue.promoteDelayedJobs();
      const recovered = await queue.recoverStalledJobs(STALLED_TIMEOUT_MS);
      if (promoted) console.log(`[${WORKER_ID}] promoted ${promoted} delayed job(s) back to pending`);
      if (recovered) console.log(`[${WORKER_ID}] recovered ${recovered} stalled job(s)`);
    } catch (err) {
      console.error(`[${WORKER_ID}] maintenance error:`, err.message);
    }
  }, STALLED_CHECK_INTERVAL_MS);

  while (running) {
    const job = await queue.dequeue();
    if (!job) {
      await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
      continue;
    }

    const startedAt = Date.now();
    try {
      const result = await runHandler(job.type, job.payload);
      const duration = Date.now() - startedAt;
      await queue.ack(job.id, duration);
      console.log(`[${WORKER_ID}] OK   ${job.type} (${job.id.slice(0, 8)}) in ${duration}ms`);
    } catch (err) {
      await queue.fail(job.id, err.message);
      console.log(`[${WORKER_ID}] FAIL ${job.type} (${job.id.slice(0, 8)}) attempt ${Number(job.attempts) + 1}: ${err.message}`);
    }
  }

  clearInterval(maintenance);
}

process.on('SIGINT', () => {
  console.log(`[${WORKER_ID}] shutting down...`);
  running = false;
  setTimeout(() => process.exit(0), 500);
});

main().catch((err) => {
  console.error(`[${WORKER_ID}] fatal error:`, err);
  process.exit(1);
});
