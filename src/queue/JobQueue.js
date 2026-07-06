const { v4: uuidv4 } = require('uuid');

/**
 * Redis keys used by the queue:
 *
 *  queue:pending      (ZSET)  score = priority*1e13 + enqueueTimestamp -> jobId
 *                              (lower score = processed first: priority then FIFO)
 *  queue:processing   (ZSET)  score = processing start timestamp -> jobId
 *                              (used to detect and recover stalled jobs)
 *  queue:delayed      (ZSET)  score = nextRunAt timestamp -> jobId
 *                              (jobs waiting for their retry backoff to elapse)
 *  queue:dead         (SET)   jobId -> jobs that exhausted all retries
 *  job:{id}           (HASH)  full job document
 *  metrics:*                  counters and rolling stats, see MetricsCollector
 *
 * Priority convention: 1 = HIGH, 2 = MEDIUM (default), 3 = LOW
 */

const PENDING = 'queue:pending';
const PROCESSING = 'queue:processing';
const DELAYED = 'queue:delayed';
const DEAD = 'queue:dead';
const PRIORITY_MULTIPLIER = 1e13;

class JobQueue {
  constructor(redis, options = {}) {
    this.redis = redis;
    this.baseRetryDelayMs = options.baseRetryDelayMs || 1000;
    this.maxRetryDelayMs = options.maxRetryDelayMs || 30000;
  }

  _score(priority, timestamp) {
    return priority * PRIORITY_MULTIPLIER + timestamp;
  }

  async enqueue({ type, payload = {}, priority = 2, maxRetries = 3 }) {
    if (!type) throw new Error('Job type is required');
    const id = uuidv4();
    const now = Date.now();

    const job = {
      id,
      type,
      payload: JSON.stringify(payload),
      priority: String(priority),
      maxRetries: String(maxRetries),
      attempts: '0',
      status: 'pending',
      createdAt: String(now),
      updatedAt: String(now),
      error: '',
    };

    const multi = this.redis.multi();
    multi.hset(`job:${id}`, job);
    multi.zadd(PENDING, this._score(priority, now), id);
    multi.incr('metrics:jobs_enqueued');
    await multi.exec();

    return id;
  }

  /** Atomically pop the highest-priority, oldest job and mark it as processing. */
  async dequeue() {
    const result = await this.redis.zpopmin(PENDING, 1);
    if (!result || result.length === 0) return null;
    const [id] = result;

    const now = Date.now();
    const multi = this.redis.multi();
    multi.zadd(PROCESSING, now, id);
    multi.hset(`job:${id}`, 'status', 'processing', 'processingStartedAt', String(now), 'updatedAt', String(now));
    await multi.exec();

    const job = await this.redis.hgetall(`job:${id}`);
    if (!job || Object.keys(job).length === 0) return null;
    job.payload = JSON.parse(job.payload || '{}');
    return job;
  }

  async ack(id, durationMs) {
    const multi = this.redis.multi();
    multi.zrem(PROCESSING, id);
    multi.hset(`job:${id}`, 'status', 'completed', 'updatedAt', String(Date.now()));
    multi.incr('metrics:jobs_succeeded');
    multi.lpush('metrics:latencies', durationMs);
    multi.ltrim('metrics:latencies', 0, 999); // keep last 1000 samples for percentile calc
    multi.zadd('metrics:throughput', Date.now(), `${id}:${Date.now()}`);
    await multi.exec();
  }

  async fail(id, errorMessage) {
    const job = await this.redis.hgetall(`job:${id}`);
    if (!job || Object.keys(job).length === 0) return;

    const attempts = parseInt(job.attempts || '0', 10) + 1;
    const maxRetries = parseInt(job.maxRetries || '3', 10);
    const now = Date.now();

    const multi = this.redis.multi();
    multi.zrem(PROCESSING, id);
    multi.hset(`job:${id}`, 'attempts', String(attempts), 'error', String(errorMessage).slice(0, 500), 'updatedAt', String(now));

    if (attempts >= maxRetries) {
      multi.hset(`job:${id}`, 'status', 'dead');
      multi.sadd(DEAD, id);
      multi.incr('metrics:jobs_dead');
    } else {
      const delay = Math.min(this.baseRetryDelayMs * 2 ** (attempts - 1), this.maxRetryDelayMs);
      const nextRunAt = now + delay;
      multi.hset(`job:${id}`, 'status', 'delayed', 'nextRunAt', String(nextRunAt));
      multi.zadd(DELAYED, nextRunAt, id);
      multi.incr('metrics:jobs_retried');
    }
    multi.incr('metrics:jobs_failed');
    await multi.exec();
  }

  /** Move any delayed jobs whose backoff has elapsed back into the pending queue. Call this on a timer. */
  async promoteDelayedJobs() {
    const now = Date.now();
    const ready = await this.redis.zrangebyscore(DELAYED, 0, now);
    if (ready.length === 0) return 0;

    const multi = this.redis.multi();
    for (const id of ready) {
      const job = await this.redis.hgetall(`job:${id}`);
      const priority = parseInt(job.priority || '2', 10);
      multi.zrem(DELAYED, id);
      multi.zadd(PENDING, this._score(priority, now), id);
      multi.hset(`job:${id}`, 'status', 'pending', 'updatedAt', String(now));
    }
    await multi.exec();
    return ready.length;
  }

  /** Recover jobs stuck in "processing" longer than `timeoutMs` (crashed worker recovery). */
  async recoverStalledJobs(timeoutMs) {
    const cutoff = Date.now() - timeoutMs;
    const stalled = await this.redis.zrangebyscore(PROCESSING, 0, cutoff);
    if (stalled.length === 0) return 0;

    for (const id of stalled) {
      await this.redis.zrem(PROCESSING, id);
      await this.fail(id, 'stalled: worker did not complete job in time');
      // fail() already routes to delayed or dead based on attempts
    }
    await this.redis.incrby('metrics:jobs_recovered_stalled', stalled.length);
    return stalled.length;
  }

  async getJob(id) {
    const job = await this.redis.hgetall(`job:${id}`);
    if (!job || Object.keys(job).length === 0) return null;
    job.payload = JSON.parse(job.payload || '{}');
    return job;
  }

  async getStats() {
    const [pending, processing, delayed, dead, enqueued, succeeded, failed, retried, deadCount, recovered] =
      await Promise.all([
        this.redis.zcard(PENDING),
        this.redis.zcard(PROCESSING),
        this.redis.zcard(DELAYED),
        this.redis.scard(DEAD),
        this.redis.get('metrics:jobs_enqueued'),
        this.redis.get('metrics:jobs_succeeded'),
        this.redis.get('metrics:jobs_failed'),
        this.redis.get('metrics:jobs_retried'),
        this.redis.get('metrics:jobs_dead'),
        this.redis.get('metrics:jobs_recovered_stalled'),
      ]);

    return {
      queues: { pending, processing, delayed, dead },
      counters: {
        enqueued: Number(enqueued || 0),
        succeeded: Number(succeeded || 0),
        failed: Number(failed || 0),
        retried: Number(retried || 0),
        dead: Number(deadCount || 0),
        recoveredStalled: Number(recovered || 0),
      },
    };
  }
}

module.exports = { JobQueue, PENDING, PROCESSING, DELAYED, DEAD };
