import 'dotenv/config';

export const config = {
  port: Number(process.env.PORT ?? 3000),
  databaseUrl:
    process.env.DATABASE_URL ??
    'postgres://postgres:change-me@localhost:5432/orderstream_lab',
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean),
  ordersTopic: process.env.ORDERS_TOPIC ?? 'order_events',
  notificationsTopic: process.env.NOTIFICATIONS_TOPIC ?? 'notification_events',
  orderGroupId: 'order_processing_group',
  notificationGroupId: 'notification_workers',
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  sportsMatchEventsTopic: process.env.SPORTS_MATCH_EVENTS_TOPIC ?? 'sports.match-events.v1',
  sportsAlertEventsTopic: process.env.SPORTS_ALERT_EVENTS_TOPIC ?? 'sports.alert-events.v1',
  sportsDeadLetterTopic: process.env.SPORTS_DEAD_LETTER_TOPIC ?? 'sports.match-events.dlq.v1',
  sportsScoreGroupId: process.env.SPORTS_SCORE_GROUP_ID ?? 'score_projection_group',
  sportsFanoutGroupId: process.env.SPORTS_FANOUT_GROUP_ID ?? 'live_fanout_group',
  sportsAlertsGroupId: process.env.SPORTS_ALERTS_GROUP_ID ?? 'fan_alerts_group',
  sportsAnalyticsGroupId: process.env.SPORTS_ANALYTICS_GROUP_ID ?? 'sports_analytics_group',
  sportsSimulationEventDelayMs: Number(process.env.SPORTS_SIMULATION_EVENT_DELAY_MS ?? 1000),
  sportsConsumerMaxAttempts: Number(process.env.SPORTS_CONSUMER_MAX_ATTEMPTS ?? 3),
  sportsConsumerRetryDelayMs: Number(process.env.SPORTS_CONSUMER_RETRY_DELAY_MS ?? 200),
  sportsOutboxPollDelayMs: Number(process.env.SPORTS_OUTBOX_POLL_DELAY_MS ?? 300),
  sportsSimulatorPollDelayMs: Number(process.env.SPORTS_SIMULATOR_POLL_DELAY_MS ?? 1000),
  sportsTelemetryCacheMs: Number(process.env.SPORTS_TELEMETRY_CACHE_MS ?? 5000),
};
