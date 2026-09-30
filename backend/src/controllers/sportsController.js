import { HttpError } from '../errors/HttpError.js';

export function createSportsController(sportsService, logger = console) {
  async function respond(response, operation, failureMessage) {
    try {
      return await operation();
    } catch (error) {
      if (error instanceof HttpError) {
        response.status(error.statusCode).json(error.body);
        return undefined;
      }
      logger.error(failureMessage, error.message);
      response.status(500).json({ error: failureMessage });
      return undefined;
    }
  }

  return {
    listMatches: async (_request, response) => {
      await respond(response, async () => response.json(await sportsService.listMatches()), 'Could not load matches.');
    },

    listEvents: async (request, response) => {
      await respond(response, async () => response.json(await sportsService.listEvents(request.params.matchId)), 'Could not load match events.');
    },

    queueSimulation: async (request, response) => {
      await respond(response, async () => response.status(202).json(
        await sportsService.queueSimulation(request.params.matchId),
      ), 'Could not queue match simulation.');
    },

    listFanAlerts: async (request, response) => {
      await respond(response, async () => response.json(
        await sportsService.listFanAlerts(request.query.fanId),
      ), 'Could not load fan alerts.');
    },
  };
}
