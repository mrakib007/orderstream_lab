import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import { pool } from '../db.js';
import { sportsProducer } from '../kafka.js';
import { createSportsOutboxModel } from '../models/sportsOutboxModel.js';

const outboxModel = createSportsOutboxModel(pool);
const workerId = hostname() + '-' + process.pid + '-' + randomUUID().slice(0, 8);
let stopping = false;

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  await sportsProducer.disconnect().catch(() => {});
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

try {
  await sportsProducer.connect();
  console.log('Sports outbox publisher ready.');
  while (!stopping) {
    const row = await outboxModel.claimNext(workerId);
    if (!row) {
      await delay(Math.max(100, config.sportsOutboxPollDelayMs));
      continue;
    }
    try {
      const metadata = await sportsProducer.send({
        topic: row.topic,
        acks: -1,
        messages: [{ key: row.messageKey, value: JSON.stringify(row.payload) }],
      });
      const acknowledgement = metadata[0];
      const stored = await outboxModel.markPublished(row.outboxId, workerId, {
        partition: acknowledgement.partition,
        offset: acknowledgement.baseOffset,
      });
      console.log(
        'outboxId=' + row.outboxId + ' eventId=' + row.eventId + ' topic=' + row.topic +
        ' key=' + row.messageKey + ' partition=' + acknowledgement.partition +
        ' offset=' + acknowledgement.baseOffset + ' traceStored=' + stored,
      );
    } catch (error) {
      try {
        await outboxModel.retryLater(row.outboxId, workerId, error.message);
      } catch (recordError) {
        console.error('Could not schedule outbox retry:', recordError.message);
      }
      console.error('Outbox publish retry scheduled: outboxId=' + row.outboxId + ' error=' + error.message);
    }
  }
} catch (error) {
  console.error('Sports outbox publisher failed:', error.message);
  process.exitCode = 1;
} finally {
  await shutdown();
}
