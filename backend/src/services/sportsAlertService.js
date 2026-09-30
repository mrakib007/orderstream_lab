import { randomUUID } from 'node:crypto';

const alertableTypes = new Set(['goal', 'yellow_card', 'red_card', 'match_completed']);

export function createSportsAlertService({ alertModel, config }) {
  return {
    async processEvent(event) {
      if (!event || typeof event !== 'object' || !event.eventId || !event.matchId || !event.eventType) {
        throw new Error('Sports event is missing its ID, match ID, or event type.');
      }
      if (!alertableTypes.has(event.eventType)) return { created: 0 };
      if (event.eventType !== 'match_completed' && !event.teamId) {
        throw new Error('A goal or card alert requires a team ID.');
      }

      const alertType = event.eventType;
      const result = await alertModel.createAlerts({
        event,
        alertEventId: () => randomUUID(),
        alertType,
        topic: config.sportsAlertEventsTopic,
        message: ({ match, event: sourceEvent, eventTeam }) => {
          if (sourceEvent.eventType === 'match_completed') {
            return match.homeTeam + ' vs ' + match.awayTeam + ' match finished.';
          }
          const teamName = eventTeam ?? sourceEvent.teamId;
          if (sourceEvent.eventType === 'goal') {
            return teamName + ' scored' + (sourceEvent.playerName ? ' through ' + sourceEvent.playerName : '') +
              ' at minute ' + sourceEvent.matchMinute + ' in ' + match.matchId + '.';
          }
          return teamName + ' received a ' + (sourceEvent.eventType === 'yellow_card' ? 'yellow' : 'red') +
            ' card at minute ' + sourceEvent.matchMinute + ' in ' + match.matchId + '.';
        },
      });
      return result;
    },
  };
}
