/**
 * Simulated job handlers. Each represents a realistic backend task with a
 * randomized processing time and a configurable failure probability, so the
 * retry / dead-letter machinery has real failures to react to.
 */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function maybeFail(failureRate, message) {
  if (Math.random() < failureRate) {
    throw new Error(message);
  }
}

const handlers = {
  async sendEmail(payload) {
    await sleep(50 + Math.random() * 150);
    maybeFail(0.15, 'SMTP timeout while sending email');
    return { delivered: true, to: payload.to || 'unknown' };
  },

  async generateReport(payload) {
    await sleep(200 + Math.random() * 400);
    maybeFail(0.2, 'Report generation failed: upstream data source unavailable');
    return { reportId: `rpt_${Date.now()}` };
  },

  async processPayment(payload) {
    await sleep(80 + Math.random() * 120);
    maybeFail(0.1, 'Payment gateway declined transaction (transient)');
    return { transactionId: `txn_${Date.now()}`, amount: payload.amount || 0 };
  },

  async resizeImage(payload) {
    await sleep(100 + Math.random() * 250);
    maybeFail(0.12, 'Image processing worker ran out of memory');
    return { resizedUrl: `${payload.url || 'image'}_resized.jpg` };
  },

  async updateInventory(payload) {
    await sleep(30 + Math.random() * 90);
    maybeFail(0.08, 'Inventory DB write conflict, retry required');
    return { sku: payload.sku || 'unknown', newQuantity: payload.quantity ?? 0 };
  },
};

async function runHandler(type, payload) {
  const handler = handlers[type];
  if (!handler) throw new Error(`No handler registered for job type "${type}"`);
  return handler(payload);
}

module.exports = { runHandler, handlers };
