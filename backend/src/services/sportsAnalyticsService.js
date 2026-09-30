export function createSportsAnalyticsService(analyticsModel) {
  return {
    async recordEvent(event) {
      if (!event || typeof event !== 'object' || !event.eventId || !event.matchId ||
          !event.eventType || !Number.isFinite(Date.parse(event.occurredAt))) {
        throw new Error('Sports analytics event is incomplete.');
      }
      const date = new Date(event.occurredAt);
      const bucketStart = new Date(Date.UTC(
        date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes(),
      ));
      return analyticsModel.recordEvent(event, bucketStart);
    },
  };
}
