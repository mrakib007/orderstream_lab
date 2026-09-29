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
  ordersTopic: process.env.ORDERS_TOPIC ?? 'orders.created',
};
