# Distributed Job Queue System

A Redis-backed distributed job queue built for horizontal scale: multiple worker
processes pull from a shared priority queue, failures are retried with
exponential backoff, permanently-failing jobs land in a dead-letter queue, and
crashed-worker jobs are automatically detected and recovered. A live dashboard
shows queue depth, throughput, and latency percentiles in real time.

---

## Why this design

Real production job queues (think: order confirmation emails, report
generation, payment webhooks, image processing pipelines) need three things
that a naive "list + worker" setup doesn't give you:

1. **Priority ordering** — a payment retry shouldn't wait behind a bulk report job.
2. **Resilience to transient failures** — a flaky downstream API shouldn't kill the job outright.
3. **Resilience to worker crashes** — if a worker dies mid-job, the job must not be lost silently.

This project implements all three directly on top of Redis primitives (sorted
sets + hashes), rather than hiding them inside a library, so the mechanics are
visible and defensible in a system-design interview.

## Architecture

```
                      ┌─────────────────┐
   POST /jobs ───────▶│  Producer API   │
                      │  (Express)      │
                      └────────┬────────┘
                               │ ZADD (priority, timestamp)
                               ▼
                     ┌───────────────────┐
                     │  queue:pending    │  (Redis ZSET, score = priority*1e13 + ts)
                     └─────────┬─────────┘
                               │ ZPOPMIN (atomic pop of highest priority, oldest job)
                               ▼
                     ┌───────────────────┐        stalled > 15s
                     │ queue:processing  │───────────────────────┐
                     └─────────┬─────────┘                       │
                success        │  failure                        │
                ▼               ▼                                │
        ┌──────────────┐  ┌───────────────────┐                  │
        │  completed   │  │  attempts < max?   │                 │
        └──────────────┘  └────────┬──────────┘                  │
                            yes │      │ no                       │
                                ▼      ▼                          │
                     ┌───────────────┐ ┌──────────┐               │
                     │ queue:delayed │ │queue:dead│◀──────────────┘
                     │ (backoff)     │ │(DLQ)     │  (after exhausting retries)
                     └───────┬───────┘ └──────────┘
                             │ backoff elapsed
                             ▼
                       back to queue:pending
```

**Worker processes** (you can run any number of them — they only coordinate
through Redis, so this is horizontally scalable with zero shared state) each
run two loops:
- a tight poll loop that dequeues and executes jobs
- a maintenance loop (every 5s) that promotes ready delayed jobs and detects/recovers stalled jobs

## Components

| Component | File | Responsibility |
|---|---|---|
| Producer API | `src/producer/api.js` | REST endpoint to submit jobs and check status |
| Job Queue core | `src/queue/JobQueue.js` | Priority queue, retry/backoff, dead-letter, stalled-job recovery |
| Metrics | `src/queue/MetricsCollector.js` | Latency percentiles (p50/p95/p99), rolling throughput |
| Worker | `src/worker/worker.js` | Dequeues, executes, acks/fails jobs; runs maintenance loop |
| Job handlers | `src/worker/jobHandlers.js` | Simulated realistic job types with randomized latency/failure |
| Dashboard | `src/dashboard/` | Live-updating ops view (queue depth, throughput chart, latency) |
| Load test | `scripts/loadTest.js` | Generates load and reports real, measured performance numbers |


## Setup (Manual)

### Prerequisites
- Node.js (v18+)
- Redis Server

### Installation
```bash
npm install
cp .env.example .env
redis-server
```

### Running Services
Run each in its own terminal:
```bash
npm run start:api          # Producer API on :4000
npm run start:worker       # Job Worker
npm run start:dashboard    # Ops Dashboard on :5000
```

## Possible Extensions
- **Redis Streams**: Swap the polling loop for Redis Streams consumer groups.
- **Idempotency**: Add idempotency keys to prevent double-execution of jobs.
- **Auto-scaling**: Scale worker replicas dynamically based on queue depth.
