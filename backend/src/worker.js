import { config } from './config.js';
import { pool } from './db.js';
import { kafka } from './kafka.js';

const consumer = kafka.consumer({ groupId: 'order-workers' });

async function shutdown() {
  await consumer.stop().catch(() => {});
  await consumer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await consumer.connect();
  await consumer.subscribe({ topic: config.ordersTopic, fromBeginning: true });
  console.log(`Worker listening to ${config.ordersTopic} as group order-workers.`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      if (!event.orderId) {
        console.error('Skipped an event without an orderId.');
        return;
      }

      const result = await pool.query(
        `UPDATE orders SET status = 'processed', processed_at = NOW()
         WHERE id = $1 AND status = 'pending'`,
        [event.orderId],
      );

      if (result.rowCount > 0) {
        console.log(`Processed order ${event.orderId} (${event.product} × ${event.quantity}).`);
      }
    },
  });
} catch (error) {
  console.error('Worker failed to start or process an event:', error.message);
  await shutdown();
  process.exitCode = 1;
}
