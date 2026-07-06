require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { createClient } = require('../config/redis');
const { JobQueue } = require('../queue/JobQueue');

const app = express();
app.use(express.json());
app.use(cors());

const redis = createClient('api');
const queue = new JobQueue(redis, {
  baseRetryDelayMs: Number(process.env.BASE_RETRY_DELAY_MS || 1000),
  maxRetryDelayMs: Number(process.env.MAX_RETRY_DELAY_MS || 30000),
});

const VALID_TYPES = ['sendEmail', 'generateReport', 'processPayment', 'resizeImage', 'updateInventory'];

app.post('/jobs', async (req, res) => {
  try {
    const { type, payload, priority, maxRetries } = req.body;

    if (!VALID_TYPES.includes(type)) {
      return res.status(400).json({ error: `type must be one of: ${VALID_TYPES.join(', ')}` });
    }
    if (priority !== undefined && ![1, 2, 3].includes(priority)) {
      return res.status(400).json({ error: 'priority must be 1 (HIGH), 2 (MEDIUM), or 3 (LOW)' });
    }

    const id = await queue.enqueue({
      type,
      payload: payload || {},
      priority: priority || 2,
      maxRetries: maxRetries || 3,
    });

    res.status(201).json({ id, status: 'pending' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/jobs/:id', async (req, res) => {
  const job = await queue.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });
  res.json(job);
});

app.get('/stats', async (req, res) => {
  const stats = await queue.getStats();
  res.json(stats);
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.API_PORT || 4000;
app.listen(PORT, () => {
  console.log(`Producer API listening on port ${PORT}`);
  console.log(`  POST   /jobs        - submit a job`);
  console.log(`  GET    /jobs/:id    - check job status`);
  console.log(`  GET    /stats       - queue statistics`);

  // Demo Traffic Generator: Enqueues a random job every 30 seconds
  if (process.env.DEMO_MODE === 'true') {
    console.log('--- DEMO MODE ENABLED ---');
    setInterval(async () => {
      try {
        const type = VALID_TYPES[Math.floor(Math.random() * VALID_TYPES.length)];
        await queue.enqueue({
          type,
          payload: { isDemo: true, timestamp: new Date().toISOString() },
          priority: Math.floor(Math.random() * 3) + 1,
          maxRetries: 3
        });
        console.log(`[Demo] Auto-enqueued ${type} job`);
      } catch (err) {
        console.error('[Demo] Failed to auto-enqueue:', err.message);
      }
    }, 30000);
  }

  // Keep-Alive Worker Ping: Prevents Render Free Tier from sleeping the worker
  const WORKER_HEALTH_URL = process.env.WORKER_HEALTH_URL || `http://localhost:8080/health`;
  const PING_INTERVAL = 10 * 60 * 1000; // 10 minutes

  setInterval(() => {
    const lib = WORKER_HEALTH_URL.startsWith('https') ? require('https') : require('http');
    lib.get(WORKER_HEALTH_URL, (res) => {
      console.log(`[Keep-Alive] Pinged worker at ${WORKER_HEALTH_URL} - Status: ${res.statusCode}`);
    }).on('error', (err) => {
      console.error(`[Keep-Alive] Failed to ping worker: ${err.message}`);
    });
  }, PING_INTERVAL);
});
