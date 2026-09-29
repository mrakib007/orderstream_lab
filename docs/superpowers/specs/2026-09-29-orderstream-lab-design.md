# OrderStream Lab design

**Status:** Proposed for review  
**Purpose:** Learn Kafka by building and observing a small full-stack order-processing app.

## What the learner will build

A browser page submits a sample order. A Node.js API publishes an event to Kafka. A separate Node.js worker consumes the event, updates the order in PostgreSQL, and the page shows the order move from `pending` to `processed`.

The goal is to make the Kafka path visible and understandable, not to build a production order system.

## Where things run

- The primary `orderstream-lab` workspace holds the project files directly: `frontend/`, `backend/`, and `infra/`.
- Podman Desktop runs its Linux machine through WSL. Kafka runs inside a Podman container in that machine; Kafka is not installed as a Windows program. The Kafka image and its named data volume are held in Podman's managed storage.
- The existing Windows Node.js and PostgreSQL installations are used for the frontend, API, worker, and database. The API and worker connect to Kafka on `localhost:9092` and PostgreSQL on `localhost:5432`.
- `infra/compose.yaml` starts one Apache Kafka broker in KRaft mode with a persistent named volume. The app does not need a manually created Podman Pod.
- `podman compose` uses the available external Compose provider to talk to Podman. Docker Desktop or a Docker Engine is not required.

## Message and data flow

1. The frontend sends a product and quantity to `POST /api/orders`.
2. The API inserts an order row with status `pending` in PostgreSQL.
3. The API publishes an `order.created` event to the Kafka topic `orders.created`, using the order ID as the message key.
4. A separate worker in the `order-workers` consumer group reads the event, waits briefly so the learner can see the asynchronous step, then updates the PostgreSQL row to `processed`.
5. The frontend polls the API for order status and displays the transition. Polling keeps the first version easy to follow; live push updates can be a later lesson.

The event contains an event ID, order ID, creation time, product, and quantity. The first version uses one Kafka broker, one topic partition, and one replica so the basic producer, topic, consumer, key, and offset concepts are easy to see.

## Components

- **Frontend:** React with Vite; order form and order-status list.
- **API:** Node.js with Express; validates requests, stores pending orders, publishes events, and serves order status.
- **Worker:** A separate Node.js process using KafkaJS; consumes order events and updates PostgreSQL.
- **Kafka:** Apache Kafka's official container image, started by Podman Compose.
- **Database:** The existing local PostgreSQL service; an `orders` table stores order data and processing status.

## First learning checkpoints

1. Start Kafka and inspect the running container.
2. Create the `orders.created` topic and send/consume one message from Kafka's command-line tools.
3. Submit an order from the web page and follow its message through the API, Kafka worker, and PostgreSQL status update.
4. Stop and restart the worker, then inspect its consumer group and offsets.

The first version intentionally leaves out Kubernetes, multiple brokers, authentication, production high availability, a Kafka web console, and an automated test suite. Those can be added as separate lessons later.

## Failure behavior and limits

- The API reports a clear error if Kafka is unavailable and marks the order `publish_failed` when it cannot publish the event.
- The worker updates an order by ID so a repeated event does not create a duplicate order row.
- This beginner version does not implement the transactional outbox pattern. A process crash between the PostgreSQL insert and Kafka publish can leave a pending order; that is a useful later lesson in database/Kafka consistency.
- Kafka is a single local broker. It demonstrates message flow, not production resilience.

## Manual verification

The learner can verify the whole path by starting the services, submitting one order, seeing `pending` then `processed` in the browser, and checking the API, worker, and Kafka logs. No test suite is planned for the initial learning version.
