import { Kafka } from 'kafkajs';
import { config } from './config.js';

export const kafka = new Kafka({
  clientId: 'orderstream-lab',
  brokers: config.kafkaBrokers,
});

export const producer = kafka.producer();
