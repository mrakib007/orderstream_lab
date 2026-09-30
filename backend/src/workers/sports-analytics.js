import { config } from '../config.js';
import { pool } from '../db.js';
import { createSportsAnalyticsModel } from '../models/sportsAnalyticsModel.js';
import { createSportsAnalyticsService } from '../services/sportsAnalyticsService.js';
import { launchSportsConsumer } from './sportsConsumer.js';

const analyticsService = createSportsAnalyticsService(createSportsAnalyticsModel(pool));

await launchSportsConsumer({
  groupId: config.sportsAnalyticsGroupId,
  topics: [config.sportsMatchEventsTopic],
  handler: ({ event }) => analyticsService.recordEvent(event),
  close: () => pool.end(),
});
