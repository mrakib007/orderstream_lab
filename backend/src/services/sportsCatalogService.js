import { HttpError } from '../errors/HttpError.js';

export function createSportsCatalogService(catalogModel) {
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
  };
}
