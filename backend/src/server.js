import express from 'express';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';
import { pool } from './db.js';
import { admin, producer } from './kafka.js';
import { getKafkaOverview } from './kafka-overview.js';

const app = express();
app.use(express.json());

app.get('/api/health', async (_request, response) => {
  const services = {
    api: { ok: true },
    database: { ok: false },
    kafka: { ok: false },
  };

  try {
    await pool.query('SELECT 1');
    services.database = { ok: true };
  } catch (error) {
    services.database = { ok: false, error: error.message };
  }

  try {
    await admin.describeCluster();
    services.kafka = { ok: true };
  } catch (error) {
    services.kafka = { ok: false, error: error.message };
  }

  const ok = services.database.ok && services.kafka.ok;
  response.status(ok ? 200 : 503).json({ ok, services });
});

app.get('/api/products', async (_request, response) => {
  try {
    const result = await pool.query(
      'SELECT product_id, name, available_stock FROM products ORDER BY product_id',
    );
    response.json(result.rows);
  } catch (error) {
    console.error('Could not load products:', error.message);
    response.status(500).json({ error: 'Could not load product catalog.' });
  }
});

app.get('/api/orders', async (_request, response) => {
  try {
    const result = await pool.query(
      'SELECT id, product_id, product, quantity, status, created_at, processed_at, ' +
      'kafka_topic, kafka_key, kafka_partition, kafka_offset ' +
      'FROM orders ORDER BY created_at DESC LIMIT 50',
    );
    response.json(result.rows);
  } catch (error) {
    console.error('Could not load orders:', error.message);
    response.status(500).json({ error: 'Could not load orders.' });
  }
});

app.get('/api/orders/:id', async (request, response) => {
  try {
    const orderResult = await pool.query(
      'SELECT id, product_id, product, quantity, status, created_at, processed_at, ' +
      'event_id, kafka_topic, kafka_key, kafka_partition, kafka_offset ' +
      'FROM orders WHERE id = $1',
      [request.params.id],
    );
    if (orderResult.rowCount === 0) {
      response.status(404).json({ error: 'Order not found.' });
      return;
    }

    const notificationResult = await pool.query(
      'SELECT order_id, event_id, status, message, created_at ' +
      'FROM notifications WHERE order_id = $1',
      [request.params.id],
    );
    response.json({ ...orderResult.rows[0], notification: notificationResult.rows[0] ?? null });
  } catch (error) {
    console.error('Could not load order detail:', error.message);
    response.status(400).json({ error: 'Could not load this order.' });
  }
});

app.get('/api/learning/overview', async (_request, response) => {
  const overview = {
    services: {
      api: { ok: true },
      database: { ok: false },
      kafka: { ok: false },
    },
    totals: { total: 0, pending: 0, confirmed: 0, out_of_stock: 0, publish_failed: 0 },
    products: [],
    topics: [],
    groups: [],
    errors: {},
  };

  try {
    await pool.query('SELECT 1');
    overview.services.database = { ok: true };
    const results = await Promise.all([
      pool.query(
        'SELECT COUNT(*)::int AS total, ' +
        "COUNT(*) FILTER (WHERE status = 'pending')::int AS pending, " +
        "COUNT(*) FILTER (WHERE status = 'confirmed')::int AS confirmed, " +
        "COUNT(*) FILTER (WHERE status = 'out_of_stock')::int AS out_of_stock, " +
        "COUNT(*) FILTER (WHERE status = 'publish_failed')::int AS publish_failed " +
        'FROM orders',
      ),
      pool.query('SELECT product_id, name, available_stock FROM products ORDER BY product_id'),
    ]);
    overview.totals = results[0].rows[0];
    overview.products = results[1].rows;
  } catch (error) {
    overview.errors.database = error.message;
  }

  try {
    const kafkaOverview = await getKafkaOverview(
      admin,
      [config.ordersTopic, config.notificationsTopic],
      [config.orderGroupId, config.notificationGroupId],
    );
    overview.services.kafka = { ok: true };
    overview.topics = kafkaOverview.topics;
    overview.groups = kafkaOverview.groups;
  } catch (error) {
    overview.errors.kafka = error.message;
  }

  response.json(overview);
});

app.post('/api/orders', async (request, response) => {
  const productId = typeof request.body?.productId === 'string' ? request.body.productId.trim() : '';
  const quantity = Number(request.body?.quantity);

  if (!productId || !Number.isInteger(quantity) || quantity < 1 || quantity > 1000) {
    response.status(400).json({ error: 'Choose a catalog product and a whole-number quantity from 1 to 1000.' });
    return;
  }

  let product;
  try {
    const productResult = await pool.query(
      'SELECT product_id, name FROM products WHERE product_id = $1',
      [productId],
    );
    product = productResult.rows[0];
    if (!product) {
      response.status(400).json({ error: 'That product is not in the catalog.' });
      return;
    }
  } catch (error) {
    console.error('Could not validate product:', error.message);
    response.status(503).json({ error: 'Could not read the product catalog.' });
    return;
  }

  const id = randomUUID();
  const eventId = randomUUID();
  const createdAt = new Date().toISOString();
  const event = {
    eventId,
    orderId: id,
    createdAt,
    productId: product.product_id,
    product: product.name,
    quantity,
  };

  try {
    await pool.query(
      "INSERT INTO orders (id, product_id, product, quantity, status, event_id) " +
      "VALUES ($1, $2, $3, $4, 'pending', $5)",
      [id, product.product_id, product.name, quantity, eventId],
    );
  } catch (error) {
    console.error('Could not save order:', error.message);
    response.status(500).json({ error: 'Could not save the order in PostgreSQL.' });
    return;
  }

  let metadata;
  try {
    metadata = await producer.send({
      topic: config.ordersTopic,
      acks: -1,
      messages: [{ key: product.product_id, value: JSON.stringify(event) }],
    });
  } catch (error) {
    await pool.query("UPDATE orders SET status = 'publish_failed' WHERE id = $1", [id]).catch(() => {});
    console.error('Could not publish order event:', error.message);
    response.status(502).json({
      error: 'Order was saved, but Kafka could not confirm its event.',
      orderId: id,
    });
    return;
  }

  const acknowledgement = metadata[0];
  const trace = {
    topic: acknowledgement.topicName ?? config.ordersTopic,
    key: product.product_id,
    partition: acknowledgement.partition,
    offset: acknowledgement.baseOffset,
  };
  try {
    await pool.query(
      'UPDATE orders SET kafka_topic = $2, kafka_key = $3, kafka_partition = $4, kafka_offset = $5 WHERE id = $1',
      [id, trace.topic, trace.key, trace.partition, trace.offset],
    );
  } catch (error) {
    console.error('Kafka accepted order ' + id + ', but its trace could not be saved:', error.message);
  }

  response.status(201).json({
    id,
    productId: product.product_id,
    productName: product.name,
    quantity,
    status: 'pending',
    createdAt,
    kafka: trace,
  });

});
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
