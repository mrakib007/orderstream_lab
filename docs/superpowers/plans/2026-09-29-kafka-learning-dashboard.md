# Kafka Learning Dashboard Implementation Plan

> For agentic workers: implement this plan task-by-task in the selected execution mode. The learner-facing checks are manual; do not send orders unless the user explicitly asks.

**Goal:** Implement the approved multi-topic order and inventory flow with a guided interface that shows real Kafka partitions, consumer groups, and PostgreSQL outcomes.

> Code changes are complete. Manual startup and learner checks remain unchecked; no migration or order submission was run during implementation.

**Architecture:** Keep the local Node.js, React, PostgreSQL, and single-broker Podman setup. Add order and notification topics with different partition counts, an inventory-processing group, a notification group, and explicit dashboard refreshes that never create orders.

**Tech Stack:** Node.js, Express, KafkaJS, PostgreSQL, React, Vite, Apache Kafka, Podman Compose, Kafbat UI.

**Spec:** docs/superpowers/specs/2026-09-29-kafka-learning-dashboard-design.md

## Global Constraints

- Keep one local Kafka broker and use replication factor 1 for both new topics.
- Create order_events with 5 partitions and notification_events with 3 partitions.
- Use productId as the order_events key and orderId as the notification_events key.
- Use order_processing_group and notification_workers as the consumer group IDs.
- Preserve existing PostgreSQL rows, Kafka topics, Kafka messages, and consumer offsets.
- The order form sends one POST only when the learner clicks submit; dashboard refreshes perform reads only.
- Keep notification delivery local to PostgreSQL; do not call an external email or messaging service.
- Describe processing as at-least-once; do not claim exactly-once behavior.

## Review Focus

- Unknown product IDs or invalid quantities must return a client error without creating an order or Kafka message. Manual check: submit an invalid product and confirm the order list and topic counts do not change. Task 2.
- Concurrent orders must not oversell a product. Manual check: pause the order processor, submit quantities that exceed available stock, resume it, and confirm stock never becomes negative. Task 3.
- Replayed order events must not decrement inventory twice or create duplicate notification rows. Manual check: replay a completed order event and inspect stock and notifications. Task 3.
- Groups that have not started or have no committed offsets must show a clear not-started/unknown state, not negative or misleading lag. Manual check: load the dashboard before starting each worker. Task 2.
- Starting on the existing persistent Kafka volume must add the new topics without deleting the existing topic or its records. Manual check: compare existing topic/message presence before and after infrastructure startup. Task 1.

---

### Task 1: Add the inventory schema and create the two Kafka topics

**Files:**
- Modify: infra/compose.yaml
- Modify: backend/src/schema.sql
- Modify: backend/src/init-db.js only if schema execution needs migration support

**Interfaces:**
- Produces products(product_id, name, available_stock), nullable orders.product_id and Kafka trace columns, and notifications(order_id, event_id, status, message, created_at).
- Produces idempotent topic creation for order_events (5 partitions) and notification_events (3 partitions), replication factor 1.
- Seeds product IDs and initial stock: coffee/40, keyboard/10, mouse/30, monitor/2, headphones/0. Existing stock values are not overwritten on restart.

- [x] Add idempotent schema changes and catalog seeding without dropping or truncating existing tables or rows.
- [x] Extend the orders status constraint to allow pending, confirmed, out_of_stock, publish_failed, and historical processed values.
- [x] Add nullable product and Kafka metadata columns so old orders remain readable.
- [x] Update the Compose init step to create both new topics with the exact partition counts and --if-not-exists behavior.
- [ ] Manual check: initialize the database and Kafka on the existing volume; confirm product rows and topic counts, and confirm the legacy orders.created topic and its messages remain.

### Task 2: Expose order trace and real Kafka topology data from the API

**Files:**
- Modify: backend/src/config.js
- Modify: backend/src/kafka.js
- Modify: backend/src/server.js
- Create: backend/src/kafka-overview.js
- Modify: backend/.env.example
- Modify only topic settings in: backend/.env, if it explicitly overrides the topic names

**Interfaces:**
- kafka-overview.js exports async getKafkaOverview(admin, topicNames, groupIds), returning topic partition metadata and each group's state, members, partition assignments, committed offsets, log-end offsets, and lag.
- GET /api/products returns the seeded catalog and current stock.
- POST /api/orders accepts { productId, quantity } and returns { id, productId, productName, quantity, status, kafka: { topic, key, partition, offset } }.
- GET /api/orders/:id returns the order, Kafka trace, and notification record if present.
- GET /api/learning/overview returns service health, order totals, catalog stock, topic partition counts, and both consumer-group summaries.
- GET /api/orders remains available for the existing order list.

- [x] Set the default topic names to order_events and notification_events while preserving database credentials and other environment settings.
- [x] Export and manage one KafkaJS admin client with the API lifecycle.
- [x] Implement getKafkaOverview using topic metadata, group descriptions/assignments, committed offsets, and topic log-end offsets; represent missing groups or offsets as not-started/unknown rather than a negative lag.
- [x] Validate productId against the catalog and quantity as an integer from 1 through 1000 before inserting.
- [x] Insert each valid order as pending, publish one event keyed by productId, and save/return the producer's actual topic, partition, and offset.
- [x] Implement the order-detail and learning-overview endpoints with database and Kafka errors represented separately.
- [ ] Manual check: verify valid and invalid API requests, then inspect returned topic/key/partition/offset and initial group states.

### Task 3: Implement the inventory and notification consumer groups

**Files:**
- Modify: backend/src/worker.js
- Create: backend/src/notification-worker.js
- Modify: backend/package.json
- Modify: package.json

**Interfaces:**
- The order worker consumes order_events as order_processing_group.
- The notification worker consumes notification_events as notification_workers.
- The order worker processes one message using a PostgreSQL transaction; it locks the product row, changes only pending orders, and produces a notification event keyed by orderId.
- The notification worker inserts one notification row per order and treats duplicates as already handled.

- [ ] Subscribe the order worker to order_events and log group ID, topic, partition, offset, key, order ID, and outcome.
- [x] In one PostgreSQL transaction, lock inventory, decrement stock only when sufficient, and set the order to confirmed or out_of_stock.
- [x] On replay, read the already-set outcome and republish its notification without decrementing stock again.
- [x] Publish the notification event before the input handler returns so a publication failure leaves the input eligible for retry.
- [x] Add the notification worker with its own group ID; insert notifications idempotently using the order ID uniqueness constraint.
- [x] Add separate root scripts for the API, web app, order worker, and notification worker; include all four in the labeled development command.
- [ ] Manual check: submit one in-stock and one over-stock order; pause and resume the order worker; confirm lag drains and duplicate handling preserves inventory and notification rows.

### Task 4: Replace the order page with the guided Kafka learning dashboard

**Files:**
- Modify: frontend/src/App.jsx
- Modify: frontend/src/styles.css

**Interfaces:**
- The page consumes GET /api/products, GET /api/learning/overview, POST /api/orders, and GET /api/orders/:id.
- The selected-order trace displays the API-created order ID, topic, message key, partition, offset, consumer group, current status, and notification result.
- Refresh actions issue reads only; the order form issues one POST per explicit submit.

- [x] Build a four-step flow: order submitted, partition chosen, consumer group processing, database result/notification.
- [x] Add service health and order/inventory summary cards, topic cards with actual partition counts, consumer-group member/assignment/lag views, and Kafbat UI links.
- [x] Replace the free-text product input with the seeded catalog selector and retain a quantity field.
- [x] Show confirmed, out_of_stock, pending, publish_failed, and historical processed states distinctly.
- [x] Remove interval polling. Load once on entry and refresh only on explicit user action; never submit an order during page load or refresh.
- [ ] Manual check: follow one order from submit through final status, use refresh while the worker is paused, and verify browser network requests show reads only until the learner submits again.

### Task 5: Update the learner runbook and keep the optional batch helper compatible

**Files:**
- Modify: README.md
- Modify: backend/scripts/send-orders.js

**Interfaces:**
- README documents the new topic/group names, key-to-partition behavior, terminal commands, where to inspect messages and lag, and the manual learning checkpoints.
- The optional batch helper retrieves valid products from GET /api/products and continues to report each request and final state; implementation does not run it.

- [x] Update the README with named terminal roles and exact steps for in-stock, out-of-stock, paused-worker lag, and same-group scaling exercises.
- [x] Explain that topic messages remain after PostgreSQL rows are cleared, that same keys stay within a partition, and that ordering is not global across partitions.
- [x] Update the batch helper to choose only catalog product IDs and parse the new order response/statuses without generating requests during implementation.
- [ ] Manual check: inspect the README commands and batch-helper usage; do not launch a batch.

## Plan self-review

- Spec coverage: topics/partitions and group IDs are in Tasks 1 and 3; inventory/order processing and notification persistence are in Tasks 1 and 3; actual Kafka trace and group lag are in Task 2; manual-refresh dashboard is in Task 4; learner runbook and optional helper are in Task 5; preservation of existing records is in Tasks 1 and 2.
- Interface consistency: the API returns productId and Kafka trace fields used by the frontend and batch helper; the worker group names and topic names match the spec table.
- Failure focus: unknown input, overselling, replay, unstarted groups, and existing-volume startup each have a named manual check in the owning task.
- No unresolved placeholders remain. Automated tests are not part of this plan; the repository's learning checkpoints are manual and no requests will be generated by the implementer.
