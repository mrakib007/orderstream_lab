export function createSportsFanoutService(redisClient) {
  return {
    async publish({ event, topic, partition, offset, key }) {
      if (!event || typeof event !== 'object') throw new Error('Sports fan-out message must be a JSON object.');
      const message = JSON.stringify({
        event,
        source: { topic, partition, offset, key },
      });
      if (topic.endsWith('.match-events.v1') && event.matchId) {
        await redisClient.publish('sports:match:' + event.matchId, message);
      } else if (topic.endsWith('.alert-events.v1') && event.fanId) {
        await redisClient.publish('sports:fan:' + event.fanId, message);
      } else {
        throw new Error('Sports fan-out event is missing the channel key for its topic.');
      }
    },
  };
}
