# OrderStream Lab

A small project for learning Kafka by following an order from a browser form, through a Node.js API and Kafka, to a PostgreSQL status update.

## What runs where

- `frontend/`: React page at `http://localhost:5173`.
- `backend/`: Express API and a separate Kafka consumer worker.
- `infra/compose.yaml`: the recipe Podman uses to start Kafka and its browser UI together.
- PostgreSQL: your existing Windows installation, reached at `localhost:5432`.
- Kafbat UI: Kafka dashboard at `http://localhost:8080`.

Compose is a YAML recipe for starting related containers with their network settings. Here it lets the Kafka UI reach Kafka inside Podman while the Node.js app reaches Kafka at `localhost:9092`. You do not need Docker Desktop.

## 1. Start Kafka and its visual UI

Make sure the Podman machine is running in Podman Desktop. From PowerShell, in this repository folder, run:

```powershell
npm run infra:up
```

Open [http://localhost:8080](http://localhost:8080). Kafbat UI should show the local cluster and the `orders.created` topic. Kafka remains available to the Node.js app at `localhost:9092`.

To stop the containers while keeping Kafka's named data volume:

```powershell
npm run infra:down
```

## 2. Prepare PostgreSQL

In pgAdmin, create a database named `orderstream_lab`. Then copy the example environment file and change its database password:

```powershell
Copy-Item backend/.env.example backend/.env
```

Edit `backend/.env` and put your PostgreSQL username and password in `DATABASE_URL`.

## 3. Install and initialize the app

From the repository root in PowerShell:

```powershell
npm install
npm run db:init
npm run dev
```

Open [http://localhost:5173](http://localhost:5173), submit an order, then watch it move from `pending` to `processed`. The frontend, API, and worker run as three Node.js processes; the worker reads the Kafka event and updates PostgreSQL.

## Learning checkpoints

1. Inspect the broker, topic, and message in Kafbat UI.
2. Start the API and frontend in separate terminals with `npm run dev:api` and `npm run dev:web`.
3. Submit an order before starting the worker; it stays pending. Start `npm run dev:worker` in another terminal and watch it become processed.

This is a single-broker local learning setup, not a production deployment. Kafka's data is stored in a named Podman volume. `npm run infra:down` keeps that volume; deleting the volume removes Kafka's stored topics and messages.

## Generate a batch of orders

With Kafka, PostgreSQL, the API, and the worker running, use:

```powershell
npm run orders:bulk --workspace backend
```

The script sends 1,000 orders through the existing API, with up to 10 requests in flight. It watches each created order's PostgreSQL status for up to two minutes and writes a per-order CSV report under `backend/reports/`. Use `Topics > orders.created > Messages` in Kafbat UI to inspect the Kafka events. You can optionally pass a count and concurrency, for example `npm run orders:bulk --workspace backend -- 100 5`.

To observe Kafka holding events before a consumer processes them, stop only the worker before running the script. The orders should remain `pending`; start the worker again while the script is watching, and observe them move to `processed`.
