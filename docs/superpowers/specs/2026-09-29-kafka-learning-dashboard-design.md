# Kafka Learning Dashboard and Multi-Topic Order Flow

**Status:** Approved for implementation on 2026-09-29.

## Purpose

Turn OrderStream Lab into a hands-on Kafka teaching project. A learner should be able to place one order, follow its event through real Kafka topics and consumer groups, see its partition and key, and understand the resulting PostgreSQL state from the project interface.

The dashboard should explain the actual running flow, rather than display a simulated Kafka diagram.

## Current project

- The API writes an order to PostgreSQL and publishes to orders.created.
- The worker reads orders.created as consumer group order-workers and changes pending to processed.
- The existing web page polls the order list every two seconds.
- The current persistent Kafka topic has five partitions, but infra/compose.yaml creates orders.created with one partition on a fresh volume.
- Older records remain in orders.created; PostgreSQL and generated reports were cleared separately.

## Proposed flow

Frontend
  -> Order API writes a pending order in PostgreSQL
  -> order_events (5 partitions, key = productId)
  -> order_processing_group checks inventory and sets confirmed or out_of_stock
  -> notification_events (3 partitions, key = orderId)
  -> notification_workers stores a sample notification in PostgreSQL

Kafka runs as one local broker, so topics use replication factor 1. The partition counts demonstrate topic-specific partitioning; they do not provide broker replication or high availability.

## Topics, keys, and consumer groups

| Topic | Partitions | Producer | Key | Consumer group |
|---|---:|---|---|---|
| order_events | 5 | Order API | productId | order_processing_group |
| notification_events | 3 | Order processor | orderId | notification_workers |

The product key keeps events for the same product in one order_events partition. This makes per-product ordering visible and is analogous to using a match ID to keep one game's events together. Other products can be handled by other partitions. The notification key distributes order-specific notifications independently.

A consumer group shares a topic's partitions among its active members. With five partitions, up to five order_processing_group members can own partitions at the same time; additional members wait idle. Separate groups each receive their own copy of the topic they subscribe to.

The topic names are new so retained messages in orders.created are not replayed into the inventory flow. Existing topics, consumer-group offsets, database rows, and the Kafka volume are preserved. Compose will create the new topics idempotently with the specified counts.

## Order and inventory behavior

- Add a five-item catalog with visible stock counts. Seed records only when absent so restarting the app does not reset stock.
- The order form selects a catalog product and quantity.
- An explicit form submission creates exactly one order. Opening or refreshing the page never creates orders.
- The API saves the order as pending, then publishes an order_events message with eventId, orderId, productId, product name, quantity, and timestamp.
- The API records and returns the actual Kafka topic, partition, offset, and key from the producer acknowledgement.
- The order processor uses a PostgreSQL transaction and locks the product inventory row while deciding the outcome. If enough stock exists, it decrements stock and sets the order to confirmed; otherwise it sets out_of_stock.
- The processor publishes a corresponding notification event. The notification worker stores one local notification record per order; it does not send email or call an external service.
- Database changes and notification handling must be safe to retry. A duplicate order event must not decrement stock twice, and duplicate notification events must not create duplicate notification rows.
- Processing is at-least-once. The project will not claim exactly-once delivery. The dashboard and guide will explain that Kafka messages and PostgreSQL state are separate, and that a database/Kafka crash window is a later outbox-pattern lesson.

## Data migration

- Extend the existing schema without dropping the orders table or resetting the user's database.
- Add product inventory, notification history, and nullable Kafka metadata fields needed to show topic/partition/offset for new orders.
- Preserve existing rows and their existing statuses. New orders use pending, confirmed, out_of_stock, or publish_failed; the old processed status remains readable for historical rows.
- Update the bulk-order helper to choose valid catalog products so it remains usable. It will not be run as part of implementation.

## Learning interface

The existing OrderStream page becomes a guided dashboard inspired by the supplied reference:

1. Order submitted: explain the frontend/API step and show the new order ID and pending database row.
2. Partition chosen: show the actual topic, key, partition, and offset returned by Kafka.
3. Consumer group processing: show order_processing_group, its partition assignments and lag, and link to inspect it in Kafbat UI.
4. Result and notification: show the inventory outcome, updated stock, and notification record from PostgreSQL.

The page also includes API/Kafka/PostgreSQL health, total/confirmed/out-of-stock/catalog counts, topic cards with their actual partition counts, and links to the Kafbat UI topic and consumer-group views.

The page will not poll continuously. Initial data loads once; the learner refreshes it explicitly. After submitting an order, the interface shows that the next step is waiting for the worker and provides a manual refresh action. This keeps reads distinct from order-producing POST requests and makes every request visible to the learner.

The order form remains the only order-generation control and sends one order per click. Load/lag experiments are performed deliberately from a separate terminal after the basic flow is understood.

## Local operations and learning checkpoints

The repository documentation will identify the terminal or browser for each component and give these checkpoints:

1. Start Kafka/PostgreSQL and confirm the two new topics have five and three partitions.
2. Start the API, order processor, notification worker, and web app in clearly named terminals.
3. Submit one in-stock order; inspect its key and partition, then watch it become confirmed and create a notification.
4. Submit an order for more than the available stock; observe out_of_stock and its notification.
5. Stop only the order processor, submit one order, refresh the dashboard and inspect consumer lag; restart the processor and observe the group drain the message.
6. Start another order processor in the same group and observe Kafka's partition assignment in Kafbat UI.

The first run uses one worker per group. Worker scaling is a learner-controlled experiment, not an automatic action from the dashboard.

## Acceptance criteria

- A fresh Compose environment creates order_events with 5 partitions and notification_events with 3 partitions, both replication factor 1.
- The app uses the named keys and consumer groups in the table above.
- One manual order can be traced in the UI from API submission through its actual Kafka partition and consumer group to PostgreSQL and the notification record.
- Both in-stock and out-of-stock outcomes are visible.
- A worker pause leaves an order pending and creates measurable group lag; resuming the worker processes the event.
- Refreshing the dashboard performs reads only; only clicking the order form submits an order.
- Existing Kafka data and existing PostgreSQL rows are not deleted or reset.
