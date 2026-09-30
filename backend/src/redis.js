import { createClient } from 'redis';
import { config } from './config.js';

export function createRedisClient() {
  const client = createClient({ url: config.redisUrl });
  client.on('error', (error) => {
    console.error('Redis client error:', error.message);
  });
  return client;
}
