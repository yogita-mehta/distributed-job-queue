# Distributed Job Queue System

![Node.js](https://img.shields.io/badge/Node.js-18%2B-green) ![Redis](https://img.shields.io/badge/Redis-7.0%2B-red) ![Express](https://img.shields.io/badge/Express-4.19-lightgrey) ![Docker](https://img.shields.io/badge/Docker-Enabled-blue)

A high-performance, fault-tolerant job orchestration system designed to handle background tasks with strict priority requirements and guaranteed delivery. 

## 🎯 The Problem
Standard "list-based" queues often fail at three things:
1. **Priority Starvation**: High-priority tasks (like payments) getting stuck behind bulk reports.
2. **Silent Failures**: Jobs disappearing when a worker crashes mid-task.
3. **Flaky Downstream APIs**: No built-in mechanism to retry with intelligent backoff.

This system solves these issues by utilizing **Redis primitives** to build a robust state machine for every job.

---

## 🏗️ System Architecture

The system is built on a "Stateless Worker" pattern. Workers coordinate entirely through Redis, allowing for instant horizontal scaling.

```
                      ┌─────────────────┐
   POST /jobs ───────▶│  Producer API   │ (Submission & Monitoring)
                      └────────┬────────┘
                               │ ZADD (Priority + Timestamp)
                               ▼
                      ┌───────────────────┐
                      │  [Pending Queue]  │ (Redis Sorted Set)
                      └─────────┬─────────┘
                               │ ZPOPMIN (Atomic Fetch)
                               ▼
                      ┌───────────────────┐        Stalled Job Monitor
                      │ [Processing Set]  │───────────────────┐
                      └─────────┬─────────┘                   │
                Success        │  Failure                     │
                ▼               ▼                             │
        ┌──────────────┐  ┌───────────────────┐               │
        │  Completed   │  │  Retry < Max?     │               │
        └──────────────┘  └────────┬──────────┘               │
                             Yes   │     No                   │
                                   ▼     ▼                    │
                      ┌───────────────┐ ┌────────────────┐    │
                      │ [Delayed Set] │ │ [Dead Letter]  │◀───┘
                      │ (Backoff)     │ └────────────────┘
                      └───────┬───────┘
                              ▼
                        Back to Pending
```

---

## 🛠️ Performance & Resilience Features

### ⚖️ Smart Priority Scheduling
Jobs are assigned a priority (1-3). The system uses a weighted score in Redis Sorted Sets, ensuring that **High Priority** tasks are always processed first, while still respecting the order of submission within that priority level.

### 🛡️ Crash-Proof Processing
Every job is tracked in a "Processing" set. If a worker process crashes or the server loses power, a dedicated **Maintenance Loop** detects the "stalled" job after 15 seconds and automatically re-queues it. **No job is ever lost silently.**

### 📈 Intelligent Retries
Failures happen. Instead of spamming a failing API, the system implements **Exponential Backoff**. If a job fails, it moves to a "Delayed" set and is only retried after an increasing wait time ($2^n$ seconds).

---

## 📊 Monitoring Dashboard
The project includes a real-time ops dashboard providing visibility into:
- **Queue Depth**: Real-time count of pending vs. processing jobs.
- **Throughput**: Calculated rolling average of jobs processed per second.
- **Latency Percentiles**: Integrated tracking of **p50, p95, and p99** response times to identify performance bottlenecks.

---

## 🚀 Quick Start

### 🐳 Docker Compose (Global Setup)
Start the entire infrastructure (Redis + API + Workers + Dashboard) instantly:
```bash
docker-compose up --build
```

### 🚀 Manual Setup
1. **Install Dependencies**: `npm install`
2. **Environment**: `cp .env.example .env`
3. **Start Services**:
   - `npm run start:api` (API on :4000)
   - `npm run start:worker` (Start 2-3 of these)
   - `npm run start:dashboard` (Dashboard on :5000)

## 🧪 Benchmarking Results
Measured on a local machine with 2 concurrent workers and 1,000 jobs:
- **Success Rate**: 99.7% (with simulated API flakiness)
- **Sustained Throughput**: ~8.5 jobs/sec
- **p99 Latency**: 573ms

---

**Developed for high-scale backend environments where data integrity and task prioritization are non-negotiable.**
