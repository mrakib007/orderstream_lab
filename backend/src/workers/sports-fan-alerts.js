import { config } from '../config.js';
import { pool } from '../db.js';
import { createSportsAlertModel } from '../models/sportsAlertModel.js';
import { createSportsAlertService } from '../services/sportsAlertService.js';
import { launchSportsConsumer } from './sportsConsumer.js';

const alertService = createSportsAlertService({
  alertModel: createSportsAlertModel(pool),
  config,
});

await launchSportsConsumer({
  groupId: config.sportsAlertsGroupId,
  topics: [config.sportsMatchEventsTopic],
  handler: ({ event }) => alertService.processEvent(event),
  close: () => pool.end(),
});
