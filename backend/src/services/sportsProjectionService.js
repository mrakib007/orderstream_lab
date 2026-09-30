const eventTypes = new Set(['match_started', 'goal', 'yellow_card', 'red_card', 'half_time', 'match_completed']);

export function createSportsProjectionService({ projectionModel, redisClient, logger = console }) {
  return {
    async applyEvent(event, source) {
      if (!event || typeof event !== 'object' || Array.isArray(event)) {
        throw new Error('Sports match event must be a JSON object.');
      }
      if (typeof event.eventId !== 'string' || !event.eventId ||
          typeof event.matchId !== 'string' || !event.matchId ||
          !Number.isInteger(event.sequence) || event.sequence < 1 ||
          !eventTypes.has(event.eventType) ||
          typeof event.description !== 'string' || !event.description ||
          !Number.isFinite(Date.parse(event.occurredAt))) {
        throw new Error('Sports match event is missing a valid ID, match, sequence, type, description, or timestamp.');
      }
      if (event.teamId !== undefined && event.teamId !== null && typeof event.teamId !== 'string') {
        throw new Error('Sports event team ID must be a string or null.');
      }

      const result = await projectionModel.applyEvent({ event, source });
      if (result.snapshot && redisClient?.isReady) {
        await redisClient.set('sports:match-state:' + event.matchId, JSON.stringify(result.snapshot))
          .catch((error) => logger.error('Could not refresh sports match cache:', error.message));
      }
      return result;
    },
  };
}
