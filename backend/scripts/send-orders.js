import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../src/config.js';
import { pool } from '../src/db.js';

const args = process.argv.slice(2);

if (args.includes('--help') || args.includes('-h')) {
  console.log('Usage: npm run orders:bulk --workspace backend -- [count] [concurrency]');
  console.log('Defaults: 1000 orders, 10 concurrent API requests. This script only runs when you execute it.');
  process.exit(0);
}
function positiveInteger(value, fallback, label, maximum) {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(label + ' must be a whole number from 1 to ' + maximum + '.');
  }
  return parsed;
}
function csvCell(value) { return '"' + String(value ?? '').replaceAll('"', '""') + '"'; }
function delay(milliseconds) { return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds)); }
function createReportPath() {
  const scriptDirectory = dirname(fileURLToPath(import.meta.url));
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  return resolve(scriptDirectory, '../reports', 'orders-' + timestamp + '.csv');
}
async function writeReport(attempts) {
  const reportPath = createReportPath();
  await mkdir(dirname(reportPath), { recursive: true });
  const columns = ['sequence', 'batchId', 'orderId', 'productId', 'product', 'quantity', 'submission', 'status', 'error'];
  const lines = [columns.join(','), ...attempts.map((attempt) => [
    attempt.sequence, attempt.batchId, attempt.orderId, attempt.productId, attempt.product,
    attempt.quantity, attempt.submission, attempt.status, attempt.error,
  ].map(csvCell).join(','))];
  await writeFile(reportPath, lines.join('\r\n') + '\r\n', 'utf8');
  return reportPath;
}
async function checkServices(apiBaseUrl) {
  const response = await fetch(apiBaseUrl + '/api/health', { signal: AbortSignal.timeout(5000) });
  const health = await response.json().catch(() => null);
  if (!response.ok || !health?.ok || !health.services?.database?.ok || !health.services?.kafka?.ok) {
    throw new Error('The API, PostgreSQL, or Kafka is not ready at ' + apiBaseUrl + '. Start the lab services first.');
  }
}
async function getProducts(apiBaseUrl) {
  const response = await fetch(apiBaseUrl + '/api/products', { signal: AbortSignal.timeout(5000) });
  const products = await response.json().catch(() => []);
  if (!response.ok || !Array.isArray(products) || products.length === 0) {
    throw new Error('The product catalog is not available from ' + apiBaseUrl + '.');
  }
  return products;
}
async function submitOrder(apiBaseUrl, attempt) {
  try {
    const response = await fetch(apiBaseUrl + '/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: attempt.productId, quantity: attempt.quantity }),
      signal: AbortSignal.timeout(15000),
    });
    const result = await response.json().catch(() => ({}));
    if (response.status === 201 && typeof result.id === 'string') {
      attempt.orderId = result.id;
      attempt.submission = 'accepted';
      attempt.status = result.status ?? 'pending';
      return;
    }
    attempt.submission = 'rejected';
    attempt.status = result.orderId ? 'publish_failed' : 'not_created';
    attempt.orderId = result.orderId ?? '';
    attempt.error = result.error ?? 'API returned HTTP ' + response.status + '.';
  } catch (error) {
    attempt.submission = 'request_error';
    attempt.status = 'unknown';
    attempt.error = error.message;
  }
}
async function sendOrders(apiBaseUrl, attempts, concurrency) {
  let nextIndex = 0;
  let finished = 0;
  const sendWorker = async () => {
    while (true) {
      const index = nextIndex++;
      if (index >= attempts.length) return;
      await submitOrder(apiBaseUrl, attempts[index]);
      finished += 1;
      if (finished % 50 === 0 || finished === attempts.length) {
        const accepted = attempts.filter((attempt) => attempt.submission === 'accepted').length;
        console.log('Requests ' + finished + '/' + attempts.length + ' (' + accepted + ' accepted so far).');
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, attempts.length) }, () => sendWorker()));
}
async function readStatuses(orderIds) {
  if (orderIds.length === 0) return [];
  const result = await pool.query('SELECT id::text AS id, status FROM orders WHERE id = ANY($1::uuid[])', [orderIds]);
  return result.rows;
}
function applyStatuses(attempts, rows) {
  const statusesById = new Map(rows.map((row) => [row.id, row.status]));
  for (const attempt of attempts) {
    if (attempt.orderId) attempt.status = statusesById.get(attempt.orderId) ?? 'not_found';
  }
}
function summarize(attempts, rows) {
  const tracked = attempts.filter((attempt) => attempt.orderId).length;
  const counts = { pending: 0, confirmed: 0, out_of_stock: 0, publish_failed: 0, other: 0 };
  for (const row of rows) {
    if (Object.hasOwn(counts, row.status)) counts[row.status] += 1;
    else counts.other += 1;
  }
  counts.missing = Math.max(0, tracked - rows.length);
  return counts;
}
function formatSummary(counts, tracked) {
  return 'Order states: confirmed ' + counts.confirmed + ', out_of_stock ' + counts.out_of_stock +
    ', pending ' + counts.pending + ', publish_failed ' + counts.publish_failed +
    ', missing ' + counts.missing + ' (' + tracked + ' tracked).';
}
async function watchStatuses(attempts, initialRows) {
  const orderIds = attempts.map((attempt) => attempt.orderId).filter(Boolean);
  let rows = initialRows;
  let lastSummary = '';
  const deadline = Date.now() + 120000;
  while (true) {
    applyStatuses(attempts, rows);
    const counts = summarize(attempts, rows);
    const summary = formatSummary(counts, orderIds.length);
    if (summary !== lastSummary) { console.log(summary); lastSummary = summary; }
    if (counts.pending === 0 && counts.missing === 0) return counts;
    if (Date.now() >= deadline) {
      console.log('Stopped waiting after 120 seconds. Check that the order-processing worker is running.');
      return counts;
    }
    await delay(1000);
    rows = await readStatuses(orderIds);
  }
}
async function main() {
  if (args.length > 2) throw new Error('Use at most two arguments: order count and concurrency. Run with --help for usage.');
  const count = positiveInteger(args[0], 1000, 'Order count', 5000);
  const concurrency = positiveInteger(args[1], 10, 'Concurrency', 50);
  const apiBaseUrl = (process.env.API_BASE_URL ?? 'http://localhost:' + config.port).replace(/\/+$/, '');
  const batchId = randomUUID().slice(0, 8);

  try {
    await checkServices(apiBaseUrl);
    const products = await getProducts(apiBaseUrl);
    const attempts = Array.from({ length: count }, (_, index) => {
      const product = products[index % products.length];
      return {
        sequence: index + 1, batchId, orderId: '', productId: product.product_id,
        product: product.name, quantity: (index % 5) + 1,
        submission: 'not_submitted', status: 'not_submitted', error: '',
      };
    });
    console.log('API, PostgreSQL, and Kafka are ready.');
    console.log('Sending ' + count + ' orders to ' + apiBaseUrl + ' with concurrency ' + concurrency + '.');
    console.log('Inspect order_events > Messages in Kafbat UI.');
    await sendOrders(apiBaseUrl, attempts, concurrency);

    const accepted = attempts.filter((attempt) => attempt.submission === 'accepted').length;
    console.log('Submission finished: ' + accepted + '/' + count + ' API requests accepted.');
    console.log('Watching saved order states in PostgreSQL for up to 120 seconds.');
    const orderIds = attempts.map((attempt) => attempt.orderId).filter(Boolean);
    const counts = await watchStatuses(attempts, await readStatuses(orderIds));
    const reportPath = await writeReport(attempts);
    const failures = attempts.filter((attempt) =>
      !attempt.orderId || ['pending', 'publish_failed', 'not_found', 'unknown', 'not_created'].includes(attempt.status),
    ).length;
    console.log('Per-order CSV report: ' + reportPath);
    if (failures > 0 || counts.pending > 0 || counts.missing > 0 || counts.publish_failed > 0) {
      console.log('Some orders did not reach a final state. The CSV lists each request and its final observed state.');
      process.exitCode = 1;
    } else {
      console.log('All tracked orders reached a final state.');
    }
  } finally {
    await pool.end().catch(() => {});
  }
}
main().catch((error) => {
  console.error('Bulk order run failed:', error.message);
  process.exitCode = 1;
});
