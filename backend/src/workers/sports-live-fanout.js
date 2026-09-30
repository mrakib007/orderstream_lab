import { config } from '../config.js';
import { closeRedisClient, createRedisClient } from '../redis.js';
import { createSportsFanoutService } from '../services/sportsFanoutService.js';
import { launchSportsConsumer } from './sportsConsumer.js';

const redisClient = createRedisClient();
await redisClient.connect();
const fanoutService = createSportsFanoutService(redisClient, config);

await launchSportsConsumer({
  groupId: config.sportsFanoutGroupId,
  topics: [config.sportsMatchEventsTopic, config.sportsAlertEventsTopic],
  handler: ({ event, source }) => fanoutService.publish({ event, ...source }),
  close: async () => closeRedisClient(redisClient),
});
