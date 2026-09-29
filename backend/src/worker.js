import { config } from './config.js';
import { pool } from './db.js';
import { kafka, producer } from './kafka.js';
import { randomUUID } from 'node:crypto';

const consumer = kafka.consumer({ groupId: config.orderGroupId });

async function decideOrder(event) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const orderResult = await client.query(
      'SELECT id, product_id, product, quantity, status, notification_event_id ' +
      'FROM orders WHERE id = $1 FOR UPDATE',
      [event.orderId],
    );
    const order = orderResult.rows[0];

    if (!order) {
      await client.query('COMMIT');
      return null;
    }

    let status = order.status;
    let notificationEventId = order.notification_event_id;

    if (status === 'pending' || status === 'publish_failed') {
      if (!order.product_id) {
        throw new Error('Order ' + order.id + ' has no catalog product ID.');
      }

      const productResult = await client.query(
        'SELECT available_stock FROM products WHERE product_id = $1 FOR UPDATE',
        [order.product_id],
      );
      if (productResult.rowCount === 0) {
        throw new Error('Catalog product ' + order.product_id + ' no longer exists.');
      }

      const availableStock = productResult.rows[0].available_stock;
      if (availableStock >= order.quantity) {
        await client.query(
          'UPDATE products SET available_stock = available_stock - $2, updated_at = NOW() WHERE product_id = $1',
          [order.product_id, order.quantity],
        );
        status = 'confirmed';
      } else {
        status = 'out_of_stock';
      }
      notificationEventId = notificationEventId ?? randomUUID();
      await client.query(
        'UPDATE orders SET status = $2, processed_at = NOW(), notification_event_id = $3 WHERE id = $1',
        [order.id, status, notificationEventId],
      );
    } else if (status === 'confirmed' || status === 'out_of_stock' || status === 'processed') {
      notificationEventId = notificationEventId ?? randomUUID();
      await client.query(
        'UPDATE orders SET notification_event_id = $2 WHERE id = $1 AND notification_event_id IS NULL',
        [order.id, notificationEventId],
      );
    } else {
      await client.query('COMMIT');
      return null;
    }

    await client.query('COMMIT');
    return { ...order, status, notificationEventId };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function shutdown() {
  await consumer.stop().catch(() => {});
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.ordersTopic, fromBeginning: true });
  console.log(
    'Order processor listening to ' + config.ordersTopic +
    ' as ' + config.orderGroupId + '.',
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      if (!event.orderId) {
        console.error('Skipped an order event without an orderId.');
        return;
      }

      const order = await decideOrder(event);
      if (!order) {
        console.log(
          'Skipped order ' + event.orderId + ' at ' + topic +
          '[' + partition + '] offset ' + message.offset + '.',
        );
        return;
      }

      const notificationMessage = order.status === 'confirmed'
        ? 'Order for ' + order.product + ' was confirmed; inventory was reserved.'
        : order.status === 'out_of_stock'
          ? 'Order for ' + order.product + ' could not be fulfilled; inventory is out of stock.'
          : 'Historical order ' + order.id + ' is ' + order.status + '.';
      const notificationEvent = {
        eventId: order.notificationEventId,
        orderId: order.id,
        status: order.status,
        productId: order.product_id,
        product: order.product,
        quantity: order.quantity,
        message: notificationMessage,
        createdAt: new Date().toISOString(),
      };

      await producer.send({
        topic: config.notificationsTopic,
        acks: -1,
        messages: [{ key: order.id, value: JSON.stringify(notificationEvent) }],
      });

      console.log(
        'group=' + config.orderGroupId +
        ' topic=' + topic +
        ' partition=' + partition +
        ' offset=' + message.offset +
        ' key=' + message.key?.toString() +
        ' order=' + order.id +
        ' outcome=' + order.status +
        ' -> ' + config.notificationsTopic,
      );
    },
  });
} catch (error) {
  console.error('Order processor failed:', error.message);
  await shutdown();
  process.exitCode = 1;
}
