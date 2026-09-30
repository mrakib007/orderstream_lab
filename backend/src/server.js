import { config } from './config.js';
import { pool } from './db.js';
import { admin, producer } from './kafka.js';
import { createApp } from './app.js';

const app = createApp({ pool, admin, producer, config });

let server;
let shuttingDown = false;

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;

  if (server) await new Promise((resolve) => server.close(resolve));
  await admin.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await pool.query('SELECT 1');
  await producer.connect();
  await admin.connect();
  server = app.listen(config.port, () => {
    console.log('OrderStream API listening at http://localhost:' + config.port);
  });
  console.log(
    'Connected to PostgreSQL and Kafka topics ' +
    config.ordersTopic + ' and ' + config.notificationsTopic + '.',
  );
} catch (error) {
  console.error('Startup connection failed. Start Kafka and PostgreSQL, then initialize the database first.');
  console.error(error.message);
  await shutdown();
  process.exitCode = 1;
}
