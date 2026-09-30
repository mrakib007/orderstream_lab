export function createSportsAlertModel(pool) {
  return {
    async createAlerts({ event, alertEventId, alertType, message, topic }) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const matchResult = await client.query(
          'SELECT m.match_id AS "matchId", m.home_team_id AS "homeTeamId", m.away_team_id AS "awayTeamId", ' +
          'home.name AS "homeTeam", away.name AS "awayTeam", t.name AS "eventTeam" ' +
          'FROM sports_matches m ' +
          'JOIN sports_teams home ON home.team_id = m.home_team_id ' +
          'JOIN sports_teams away ON away.team_id = m.away_team_id ' +
          'LEFT JOIN sports_teams t ON t.team_id = $2 ' +
          'WHERE m.match_id = $1',
          [event.matchId, event.teamId ?? null],
        );
        const match = matchResult.rows[0];
        if (!match) {
          await client.query('COMMIT');
          return { created: 0 };
        }

        let eligibleTeamIds = [];
        if (event.eventType === 'goal' || event.eventType === 'yellow_card' || event.eventType === 'red_card') {
          if (![match.homeTeamId, match.awayTeamId].includes(event.teamId)) {
            throw new Error('Cannot alert for an event team outside its match.');
          }
          eligibleTeamIds = [event.teamId];
        } else if (event.eventType === 'match_completed') {
          eligibleTeamIds = [match.homeTeamId, match.awayTeamId];
        } else {
          await client.query('COMMIT');
          return { created: 0 };
        }

        const fanResult = await client.query(
          'SELECT DISTINCT fan_id AS "fanId" FROM sports_fan_subscriptions ' +
          'WHERE team_id = ANY($1::text[]) ORDER BY fan_id',
          [eligibleTeamIds],
        );
        let created = 0;
        for (const { fanId } of fanResult.rows) {
          const alertMessage = message({ match, event, eventTeam: match.eventTeam });
          const alertInsert = await client.query(
            'INSERT INTO sports_fan_alerts (alert_id, fan_id, event_id, match_id, alert_type, message) ' +
            'VALUES (gen_random_uuid(), $1, $2, $3, $4, $5) ' +
            'ON CONFLICT (fan_id, event_id, alert_type) DO NOTHING RETURNING alert_id',
            [fanId, event.eventId, event.matchId, alertType, alertMessage],
          );
          if (!alertInsert.rowCount) continue;
          const createdAt = new Date().toISOString();
          const alertEvent = {
            eventId: alertEventId(fanId),
            sourceEventId: event.eventId,
            fanId,
            matchId: event.matchId,
            alertType,
            message: alertMessage,
            createdAt,
          };
          await client.query(
            'INSERT INTO sports_outbox_events (event_id, topic, message_key, payload) VALUES ($1, $2, $3, $4::jsonb)',
            [alertEvent.eventId, topic, fanId, JSON.stringify(alertEvent)],
          );
          created += 1;
        }
        await client.query('COMMIT');
        return { created };
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  };
}
