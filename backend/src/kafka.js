import { Kafka } from 'kafkajs';
import { config } from './config.js';

export const kafka = new Kafka({
  clientId: 'orderstream-lab',
  brokers: config.kafkaBrokers,
});

export const producer = kafka.producer();
export const sportsProducer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
export const admin = kafka.admin();
