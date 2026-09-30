import { HttpError } from '../errors/HttpError.js';
import { getKafkaOverview } from '../kafka-overview.js';

export function createSportsCatalogService(catalogModel, { admin, config } = {}) {
  return {
    listMatches() {
      return catalogModel.listMatches();
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
      const [metrics, kafka] = await Promise.all([
        catalogModel.getEventMetrics(),
        getKafkaOverview(admin, topicNames, groupIds, groupTopics)
          .then((overview) => ({ ...overview, error: null }))
          .catch((error) => ({ topics: [], groups: [], error: error.message })),
      ]);
      return {
        collectedAt: new Date().toISOString(),
        metrics,
        topics: kafka.topics,
        groups: kafka.groups,
        errors: { kafka: kafka.error },
      };
    },
  };
}
