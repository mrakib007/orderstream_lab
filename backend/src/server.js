import express from 'express';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';
import { pool } from './db.js';
import { producer } from './kafka.js';

const app = express();
app.use(express.json());

app.get('/api/health', async (_request, response) => {
  try {
    await pool.query('SELECT 1');
    response.json({ ok: true, database: 'connected' });
  } catch {
    response.status(503).json({ ok: false, database: 'unavailable' });
  }
});

app.get('/api/orders', async (_request, response) => {
  try {
    const result = await pool.query(
      `SELECT id, product, quantity, status, created_at, processed_at
       FROM orders ORDER BY created_at DESC LIMIT 50`,
    );
    response.json(result.rows);
  } catch (error) {
    console.error('Could not load orders:', error.message);
    response.status(500).json({ error: 'Could not load orders.' });
  }
});

app.post('/api/orders', async (request, response) => {
  const product = typeof request.body.product === 'string' ? request.body.product.trim() : '';
  const quantity = Number(request.body.quantity);

  if (!product || product.length > 100 || !Number.isInteger(quantity) || quantity < 1 || quantity > 1000) {
    response.status(400).json({ error: 'Enter a product name and a whole-number quantity from 1 to 1000.' });
    return;
  }

  const order = {
    id: randomUUID(),
    product,
    quantity,
    status: 'pending',
  };

  try {
    await pool.query(
      'INSERT INTO orders (id, product, quantity, status) VALUES ($1, $2, $3, $4)',
      [order.id, order.product, order.quantity, order.status],
    );

    const event = {
      eventId: randomUUID(),
      orderId: order.id,
      createdAt: new Date().toISOString(),
      product: order.product,
      quantity: order.quantity,
    };

    try {
      await producer.send({
        topic: config.ordersTopic,
        acks: -1,
        messages: [{ key: order.id, value: JSON.stringify(event) }],
      });
    } catch (error) {
      await pool.query("UPDATE orders SET status = 'publish_failed' WHERE id = $1", [order.id]);
      console.error('Could not publish order event:', error.message);
      response.status(502).json({ error: 'Order was saved, but Kafka could not accept its event.', orderId: order.id });
      return;
    }

    response.status(201).json(order);
  } catch (error) {
    console.error('Could not create order:', error.message);
    response.status(500).json({ error: 'Could not create order. Check PostgreSQL is running and configured.' });
  }
});

let server;
let shuttingDown = false;

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;

  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
  await producer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await pool.query('SELECT 1');
  await producer.connect();
  server = app.listen(config.port, () => {
    console.log(`OrderStream API listening at http://localhost:${config.port}`);
  });
  console.log(`Connected to Kafka topic ${config.ordersTopic} and PostgreSQL.`);
} catch (error) {
  console.error('Startup connection failed. Start Kafka, PostgreSQL, and initialize the database first.');
  console.error(error.message);
  await shutdown();
  process.exitCode = 1;
}
