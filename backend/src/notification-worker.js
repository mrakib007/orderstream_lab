import { config } from './config.js';
import { pool } from './db.js';
import { kafka } from './kafka.js';

const consumer = kafka.consumer({ groupId: config.notificationGroupId });

async function shutdown() {
  await consumer.stop().catch(() => {});
  await consumer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await consumer.connect();
  await consumer.subscribe({ topic: config.notificationsTopic, fromBeginning: true });
  console.log(
    'Notification worker listening to ' + config.notificationsTopic +
    ' as ' + config.notificationGroupId + '.',
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      if (!event.orderId || !event.eventId || !event.status || !event.message) {
        console.error('Skipped an incomplete notification event at ' + topic + '[' + partition + '].');
        return;
      }

      const result = await pool.query(
        'INSERT INTO notifications (order_id, event_id, status, message) ' +
        'VALUES ($1, $2, $3, $4) ON CONFLICT (order_id) DO NOTHING',
        [event.orderId, event.eventId, event.status, event.message],
      );

      console.log(
        'group=' + config.notificationGroupId +
        ' topic=' + topic +
        ' partition=' + partition +
        ' offset=' + message.offset +
        ' key=' + message.key?.toString() +
        ' order=' + event.orderId +
        ' result=' + (result.rowCount ? 'stored' : 'duplicate-already-stored'),
      );
    },
  });
} catch (error) {
  console.error('Notification worker failed:', error.message);
  await shutdown();
  process.exitCode = 1;
}
