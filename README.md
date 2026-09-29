# OrderStream Lab

A hands-on Kafka project for tracing one order from a React page through a Node.js API, Kafka topics and consumer groups, and PostgreSQL.

## What each part does

- frontend/: the learning dashboard at http://localhost:5173.
- backend/: the API plus two separate Kafka consumer processes.
- infra/compose.yaml: the Podman Compose recipe for Kafka and Kafbat UI.
- PostgreSQL: your existing local PostgreSQL installation.
- Kafbat UI: the Kafka browser at http://localhost:8080.

Podman runs Kafka in one local broker container. Both topics use replication factor 1, so their partitions help you learn distribution and parallel consumption; they do not provide broker redundancy. You do not install Kafka or Java separately. PostgreSQL stays installed on Windows; the Node.js API and workers connect to it at localhost:5432.

## The flow you can follow

    Order form
      -> API saves a pending order in PostgreSQL
      -> order_events (5 partitions, key = productId)
      -> order_processing_group checks stock and saves confirmed/out_of_stock
      -> notification_events (3 partitions, key = orderId)
      -> notification_workers saves one local notification row in PostgreSQL

The notification is a database record only. This project does not send real email or call an external service. The API writes PostgreSQL and Kafka as separate steps, so a crash between those steps can leave a pending database row without a Kafka message; the outbox pattern is a later lesson. Processing is at-least-once: workers can see a message again after a failure. Database locks and unique constraints make the inventory and notification writes safe to retry; this is not an exactly-once guarantee.

A topic is a named Kafka message stream. A partition is an ordered log within a topic. The product key keeps events for the same product routed consistently to one of the five order_events partitions. Notification events use the order ID as their key and are distributed independently across three partitions. Ordering is per partition, not global across all partitions.

A consumer group shares a topic's partitions among active members. With five partitions, up to five members of order_processing_group can own a partition at once; extra members wait without an assignment. A different group gets its own read progress.

## Start the project

Use PowerShell in C:\Projects\orderstream_lab.

### 1. Start Kafka and its visual UI

Make sure Podman Desktop shows its machine as running, then run:

    npm run infra:up

This creates order_events (5 partitions) and notification_events (3 partitions) without deleting the old orders.created topic or its records. If either new topic was auto-created with too few partitions, startup increases it while retaining its records; it never shrinks a topic. Open http://localhost:8080 and inspect the new topics.

### 2. Install project packages if needed

    npm install

### 3. Prepare the database schema

Keep your existing DATABASE_URL in backend/.env. The schema command adds the catalog, trace columns, and notifications table without clearing existing orders:

    npm run db:init

The initial catalog is Coffee (40), Keyboard (10), Mouse (30), Monitor (2), and Headphones (0). Seeding uses ON CONFLICT DO NOTHING, so restarting the app does not reset stock.

### 4. Start the app

For learning, use four PowerShell terminals in the project folder so each role is visible:

| Terminal | Command | What it runs |
|---|---|---|
| 1 | npm run dev:api | HTTP API on port 3000 |
| 2 | npm run dev:worker | order_processing_group |
| 3 | npm run dev:notifications | notification_workers |
| 4 | npm run dev:web | React dashboard on port 5173 |

You can stop a worker with Ctrl+C and restart it later. To stop all four processes, use Ctrl+C in each terminal. npm run dev is also available to launch all four together, but separate terminals make the worker roles easier to see.

## Follow your first order

1. Open http://localhost:5173. The first page load reads data; it does not submit an order.
2. In Step 1, choose a product and quantity, then click Submit exactly one order once.
3. Step 2 shows the actual Kafka topic, message key, partition, and offset returned by Kafka.
4. In Kafbat, open Topics > order_events > Messages. The message value is JSON and its key is the product ID.
5. Step 3 shows order_processing_group, its active members, partition assignments, and lag.
6. The order worker checks stock and publishes a result to notification_events.
7. notification_workers stores the local notification. Step 4 shows the final order status and notification after you click Refresh data.
8. To see out_of_stock, submit a quantity higher than the selected product's remaining stock. Headphones start with zero available.

The dashboard never polls every few seconds. Its explicit refresh button and opening/selecting an order use GET requests only. The form sends one POST only when you submit it; one click submits one order.

## See lag and partition ownership

- Start only the API and web app first. Submit one order and refresh: it stays pending and the consumer group has no active member.
- Start npm run dev:worker in its own terminal and refresh. The group joins and its lag falls as it handles the message.
- Stop the order worker, submit one more order, and refresh to see pending work and lag. Restart the worker to let it catch up.
- Start a second npm run dev:worker in another terminal. Both processes use the same group ID, so Kafka assigns partitions between them. Try up to five members; more than five cannot own a partition in this topic.
- Start a second notification worker only if you want to observe scaling of notification_workers. It shares the three notification partitions.

A group with no committed offsets is shown as not started or unknown; lag is not reported as a negative number.

## Data that stays between runs

npm run infra:down stops the containers but preserves the named kafka-data volume. The existing orders.created topic, messages, and offsets remain, and the new topics are created idempotently. The PostgreSQL migration does not truncate tables or reset stock on restart. Kafka messages and PostgreSQL rows are separate: clearing a database row does not delete its Kafka message. Avoid deleting the Kafka volume while learning unless you intentionally want to erase Kafka's local data.

## Optional batch helper

The helper can submit many requests and writes a CSV status report under backend/reports/. It is not used by the dashboard and does not run automatically. Do not run it until you deliberately want a batch. Its defaults are 1,000 requests with 10 in flight; for a small exercise, pass a small count and concurrency:

    npm run orders:bulk --workspace backend -- 5 1

This helper chooses catalog products and accepts both confirmed and out_of_stock as final outcomes. It creates a report only when you run the command.
