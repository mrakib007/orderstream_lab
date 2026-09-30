import { config } from '../config.js';
import { pool } from '../db.js';
import { createSportsProjectionModel } from '../models/sportsProjectionModel.js';
import { closeRedisClient, createRedisClient } from '../redis.js';
import { createSportsProjectionService } from '../services/sportsProjectionService.js';
import { launchSportsConsumer } from './sportsConsumer.js';

const redisClient = createRedisClient();
try {
  await redisClient.connect();
} catch (error) {
  console.error('Sports score cache is unavailable; PostgreSQL remains the projection source:', error.message);
}

const projectionService = createSportsProjectionService({
  projectionModel: createSportsProjectionModel(pool),
  redisClient,
});

await launchSportsConsumer({
  groupId: config.sportsScoreGroupId,
  topics: [config.sportsMatchEventsTopic],
  handler: ({ event, source }) => projectionService.applyEvent(event, source),
  close: async () => {
    closeRedisClient(redisClient);
    await pool.end();
  },
});
