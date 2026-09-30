import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import { kafka, sportsProducer } from '../kafka.js';

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function launchSportsConsumer({ groupId, topics, handler, close = async () => {}, logger = console }) {
  const consumer = kafka.consumer({ groupId });
  let shuttingDown = false;

  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
    await sportsProducer.disconnect().catch(() => {});
    await close().catch(() => {});
  }

  process.once('SIGINT', () => void shutdown());
  process.once('SIGTERM', () => void shutdown());

  try {
    await sportsProducer.connect();
    await consumer.connect();
    for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true });
    logger.log('Sports consumer ready: group=' + groupId + ' topics=' + topics.join(', '));

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const source = {
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
        };
        const rawValue = message.value?.toString() ?? '';
        let event;
        let lastError;
        const attempts = Math.max(1, Math.floor(config.sportsConsumerMaxAttempts || 3));

        for (let attempt = 1; attempt <= attempts; attempt += 1) {
          try {
            event = JSON.parse(rawValue);
            await handler({ event, source, attempt });
            logger.log(
              'group=' + groupId + ' topic=' + topic + ' partition=' + partition +
              ' offset=' + message.offset + ' key=' + source.key + ' eventId=' + (event?.eventId ?? 'unknown'),
            );
            return;
          } catch (error) {
            lastError = error;
            if (attempt < attempts) {
              await delay(Math.max(0, config.sportsConsumerRetryDelayMs) * attempt);
            }
          }
        }

        const deadLetter = {
          failureId: event?.eventId ?? randomUUID(),
          failedAt: new Date().toISOString(),
          source: { ...source, consumerGroup: groupId },
          attempts,
          error: String(lastError?.message ?? lastError).slice(0, 1000),
          event: event ?? null,
          rawValue: event ? undefined : rawValue.slice(0, 10000),
        };
        await sportsProducer.send({
          topic: config.sportsDeadLetterTopic,
          acks: -1,
          messages: [{
            key: event?.matchId ?? source.key ?? 'unknown',
            value: JSON.stringify(deadLetter),
          }],
        });
        logger.error(
          'dead-lettered group=' + groupId + ' topic=' + topic + ' partition=' + partition +
          ' offset=' + message.offset + ' attempts=' + attempts + ' error=' + deadLetter.error,
        );
      },
    });
  } catch (error) {
    logger.error('Sports consumer failed: group=' + groupId + ' error=' + error.message);
    await shutdown();
    process.exitCode = 1;
  }
}
