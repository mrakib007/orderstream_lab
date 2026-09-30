import { createClient } from 'redis';
import { config } from './config.js';

export function createRedisClient() {
  const client = createClient({ url: config.redisUrl });
  client.on('error', (error) => {
    console.error('Redis client error:', error.message);
  });
  return client;
}

export function closeRedisClient(client) {
  if (!client?.isOpen) return;
  try {
    client.destroy();
  } catch {
    // A connection failure may close the socket between the isOpen check and destroy().
  }
}
