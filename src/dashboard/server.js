require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const { createClient } = require('../config/redis');
const { JobQueue } = require('../queue/JobQueue');
const { MetricsCollector } = require('../queue/MetricsCollector');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const redis = createClient('dashboard');
const queue = new JobQueue(redis);
const metrics = new MetricsCollector(redis);

app.get('/api/stats', async (req, res) => {
  try {
    const [queueStats, latency, throughput, recentErrors] = await Promise.all([
      queue.getStats(),
      metrics.getLatencyPercentiles(),
      metrics.getThroughput(60000),
      metrics.getRecentErrors(10),
    ]);
    res.json({ ...queueStats, latency, throughput, recentErrors, timestamp: Date.now() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.DASHBOARD_PORT || 5000;
app.listen(PORT, () => {
  console.log(`Dashboard running at http://localhost:${PORT}`);
});
