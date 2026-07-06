require('dotenv').config();
const { createClient } = require('../src/config/redis');
const { JobQueue } = require('../src/queue/JobQueue');
const { MetricsCollector } = require('../src/queue/MetricsCollector');

const JOB_TYPES = ['sendEmail', 'generateReport', 'processPayment', 'resizeImage', 'updateInventory'];
const TOTAL_JOBS = Number(process.argv[2]) || 500;

function randomPriority() {
  const r = Math.random();
  if (r < 0.2) return 1; // HIGH
  if (r < 0.7) return 2; // MEDIUM
  return 3; // LOW
}

async function main() {
  const redis = createClient('load-test');
  const queue = new JobQueue(redis);
  const metrics = new MetricsCollector(redis);

  console.log(`\n=== Distributed Job Queue — Load Test ===`);
  console.log(`Enqueuing ${TOTAL_JOBS} jobs across ${JOB_TYPES.length} job types...\n`);

  const startEnqueue = Date.now();
  for (let i = 0; i < TOTAL_JOBS; i++) {
    const type = JOB_TYPES[Math.floor(Math.random() * JOB_TYPES.length)];
    await queue.enqueue({
      type,
      payload: { sample: i },
      priority: randomPriority(),
      maxRetries: 3,
    });
  }
  const enqueueDuration = Date.now() - startEnqueue;
  console.log(`Enqueued ${TOTAL_JOBS} jobs in ${enqueueDuration}ms.`);
  console.log(`Waiting for worker(s) to drain the queue... (make sure "npm run start:worker" is running)\n`);

  const startProcessing = Date.now();
  let lastReport = 0;

  // Poll until pending, processing, and delayed queues are all empty (or timeout).
  const TIMEOUT_MS = 120000;
  while (Date.now() - startProcessing < TIMEOUT_MS) {
    const stats = await queue.getStats();
    const inFlight = stats.queues.pending + stats.queues.processing + stats.queues.delayed;

    if (Date.now() - lastReport > 2000) {
      process.stdout.write(
        `\r  in-flight: ${inFlight}  |  succeeded: ${stats.counters.succeeded}  |  retried: ${stats.counters.retried}  |  dead: ${stats.counters.dead}   `
      );
      lastReport = Date.now();
    }

    if (inFlight === 0 && stats.counters.succeeded + stats.counters.dead >= TOTAL_JOBS) {
      break;
    }
    await new Promise((r) => setTimeout(r, 300));
  }

  const totalDuration = Date.now() - startProcessing;
  const finalStats = await queue.getStats();
  const latency = await metrics.getLatencyPercentiles();

  const successRate = ((finalStats.counters.succeeded / TOTAL_JOBS) * 100).toFixed(1);
  const throughputPerSec = (finalStats.counters.succeeded / (totalDuration / 1000)).toFixed(2);

  console.log(`\n\n=== Results ===`);
  console.log(`Total jobs submitted     : ${TOTAL_JOBS}`);
  console.log(`Total processing time    : ${(totalDuration / 1000).toFixed(2)}s`);
  console.log(`Succeeded                : ${finalStats.counters.succeeded}`);
  console.log(`Sent to dead-letter queue: ${finalStats.counters.dead}`);
  console.log(`Retries triggered        : ${finalStats.counters.retried}`);
  console.log(`Stalled jobs recovered   : ${finalStats.counters.recoveredStalled}`);
  console.log(`Success rate             : ${successRate}%`);
  console.log(`Sustained throughput     : ${throughputPerSec} jobs/sec`);
  console.log(`Latency p50 / p95 / p99  : ${latency.p50}ms / ${latency.p95}ms / ${latency.p99}ms`);
  console.log(`Average latency          : ${latency.avg}ms`);
  console.log(`\nThese are real numbers from this run — safe to quote on a resume.\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error('Load test failed:', err);
  process.exit(1);
});
