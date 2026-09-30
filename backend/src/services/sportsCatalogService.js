import { HttpError } from '../errors/HttpError.js';
import { getKafkaOverview } from '../kafka-overview.js';

export function createSportsCatalogService(catalogModel, { admin, config, redisClient, logger = console } = {}) {
  let telemetrySnapshot;
  let telemetryExpiresAt = 0;
  let telemetryRequest;

  return {
    async listMatches() {
      const matches = await catalogModel.listMatches();
      if (!redisClient?.isReady) return matches;
      return Promise.all(matches.map(async (match) => {
        const cacheKey = 'sports:match-state:' + match.matchId;
        try {
          const cached = await redisClient.get(cacheKey);
          if (!cached) {
            await redisClient.set(cacheKey, JSON.stringify(match));
            return match;
          }
          const snapshot = JSON.parse(cached);
          if (Number(snapshot.lastEventSequence ?? 0) !== Number(match.lastEventSequence)) {
            await redisClient.set(cacheKey, JSON.stringify(match));
            return match;
          }
          return { ...match, ...snapshot };
        } catch (error) {
          logger.error('Could not read sports match cache:', error.message);
          return match;
        }
      }));
    },

    async listEvents(matchId) {
      const match = await catalogModel.findMatch(matchId);
      if (!match) throw new HttpError(404, { error: 'Match not found.' });
      return catalogModel.listEvents(matchId);
    },

    async queueSimulation(matchId) {
      if (!matchId || matchId.length > 120) {
        throw new HttpError(400, { error: 'Choose a valid match ID.' });
      }
      const result = await catalogModel.queueSimulation(matchId);
      if (result.kind === 'missing') throw new HttpError(404, { error: 'Match not found.' });
      if (result.kind === 'not-simulatable') {
        throw new HttpError(409, { error: 'Only a scheduled match or a failed simulation can be started.' });
      }
      if (result.kind === 'already-requested') {
        throw new HttpError(409, { error: 'This match already has a ' + result.status + ' simulation.' });
      }
      return { matchId, status: 'queued', resumed: result.resumed };
    },

    async listFanAlerts(fanIdInput) {
      const fanId = typeof fanIdInput === 'string' && fanIdInput.trim()
        ? fanIdInput.trim().slice(0, 120)
        : 'demo-fan';
      return catalogModel.listFanAlerts(fanId);
    },

    async getTelemetry() {
      if (telemetrySnapshot && Date.now() < telemetryExpiresAt) return telemetrySnapshot;
      if (telemetryRequest) return telemetryRequest;
      const topicNames = [
        config.sportsMatchEventsTopic,
        config.sportsAlertEventsTopic,
        config.sportsDeadLetterTopic,
      ];
      const groupIds = [
        config.sportsScoreGroupId,
        config.sportsFanoutGroupId,
        config.sportsAlertsGroupId,
        config.sportsAnalyticsGroupId,
      ];
      const groupTopics = [
        [config.sportsMatchEventsTopic],
        [config.sportsMatchEventsTopic, config.sportsAlertEventsTopic],
        [config.sportsMatchEventsTopic],
        [config.sportsMatchEventsTopic],
      ];
      telemetryRequest = (async () => {
        const [metrics, kafka] = await Promise.all([
          catalogModel.getEventMetrics(),
          getKafkaOverview(admin, topicNames, groupIds, groupTopics)
            .then((overview) => ({ ...overview, error: null }))
            .catch((error) => ({ topics: [], groups: [], error: error.message })),
        ]);
        telemetrySnapshot = {
          collectedAt: new Date().toISOString(),
          metrics,
          topics: kafka.topics,
          groups: kafka.groups,
          errors: { kafka: kafka.error },
        };
        telemetryExpiresAt = Date.now() + Math.max(0, config.sportsTelemetryCacheMs);
        return telemetrySnapshot;
      })();
      try {
        return await telemetryRequest;
      } finally {
        telemetryRequest = undefined;
      }
    },
  };
}
