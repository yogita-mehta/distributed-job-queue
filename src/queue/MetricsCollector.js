class MetricsCollector {
  constructor(redis) {
    this.redis = redis;
  }

  /** Percentiles from the last 1000 job completion latencies (ms). */
  async getLatencyPercentiles() {
    const raw = await this.redis.lrange('metrics:latencies', 0, -1);
    if (raw.length === 0) return { p50: 0, p95: 0, p99: 0, avg: 0, samples: 0 };

    const values = raw.map(Number).sort((a, b) => a - b);
    const pct = (p) => values[Math.min(values.length - 1, Math.floor((p / 100) * values.length))];
    const avg = values.reduce((a, b) => a + b, 0) / values.length;

    return {
      p50: Math.round(pct(50)),
      p95: Math.round(pct(95)),
      p99: Math.round(pct(99)),
      avg: Math.round(avg),
      samples: values.length,
    };
  }

  /** Jobs completed per second, computed from the rolling throughput ZSET over the last `windowMs`. */
  async getThroughput(windowMs = 60000) {
    const now = Date.now();
    const cutoff = now - windowMs;
    await this.redis.zremrangebyscore('metrics:throughput', 0, cutoff - windowMs); // trim old entries
    const count = await this.redis.zcount('metrics:throughput', cutoff, now);
    return {
      windowSeconds: windowMs / 1000,
      completed: count,
      perSecond: Number((count / (windowMs / 1000)).toFixed(2)),
    };
  }
}

module.exports = { MetricsCollector };
