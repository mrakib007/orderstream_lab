export function createSportsCatalogModel(pool) {
  return {
    async listMatches() {
      const result = await pool.query(
        'SELECT m.match_id AS "matchId", m.competition, m.kickoff_at AS "kickoffAt", ' +
        'm.status, m.home_score AS "homeScore", m.away_score AS "awayScore", ' +
        'm.last_event_sequence AS "lastEventSequence", ' +
        'm.reconciliation_required AS "reconciliationRequired", ' +
        'home.team_id AS "homeTeamId", home.name AS "homeTeam", home.short_name AS "homeShortName", ' +
        'away.team_id AS "awayTeamId", away.name AS "awayTeam", away.short_name AS "awayShortName", ' +
        'j.status AS "simulationStatus", j.next_event_sequence AS "nextEventSequence" ' +
        'FROM sports_matches m ' +
        'JOIN sports_teams home ON home.team_id = m.home_team_id ' +
        'JOIN sports_teams away ON away.team_id = m.away_team_id ' +
        'LEFT JOIN sports_simulation_jobs j ON j.match_id = m.match_id ' +
        'ORDER BY m.kickoff_at, m.match_id',
      );
      return result.rows;
    },

    async findMatch(matchId) {
      const result = await pool.query(
        'SELECT m.match_id AS "matchId", m.competition, m.kickoff_at AS "kickoffAt", ' +
        'm.status, m.home_score AS "homeScore", m.away_score AS "awayScore", ' +
        'm.last_event_sequence AS "lastEventSequence", ' +
        'm.reconciliation_required AS "reconciliationRequired", ' +
        'home.team_id AS "homeTeamId", home.name AS "homeTeam", home.short_name AS "homeShortName", ' +
        'away.team_id AS "awayTeamId", away.name AS "awayTeam", away.short_name AS "awayShortName", ' +
        'j.status AS "simulationStatus", j.next_event_sequence AS "nextEventSequence" ' +
        'FROM sports_matches m ' +
        'JOIN sports_teams home ON home.team_id = m.home_team_id ' +
        'JOIN sports_teams away ON away.team_id = m.away_team_id ' +
        'LEFT JOIN sports_simulation_jobs j ON j.match_id = m.match_id ' +
        'WHERE m.match_id = $1',
        [matchId],
      );
      return result.rows[0] ?? null;
    },

    async listEvents(matchId, limit = 100) {
      const result = await pool.query(
        'SELECT e.event_id AS "eventId", e.match_id AS "matchId", e.sequence, e.event_type AS "eventType", ' +
        'e.team_id AS "teamId", t.name AS "teamName", e.player_name AS "playerName", ' +
        'e.match_minute AS "matchMinute", e.description, e.payload, e.occurred_at AS "occurredAt", ' +
        'e.kafka_topic AS "kafkaTopic", e.kafka_partition AS "kafkaPartition", e.kafka_offset AS "kafkaOffset" ' +
        'FROM sports_match_events e LEFT JOIN sports_teams t ON t.team_id = e.team_id ' +
        'WHERE e.match_id = $1 ORDER BY e.sequence DESC LIMIT $2',
        [matchId, limit],
      );
      return result.rows.reverse();
    },

    async listFanAlerts(fanId, limit = 50) {
      const result = await pool.query(
        'SELECT alert_id AS "alertId", fan_id AS "fanId", event_id AS "eventId", ' +
        'match_id AS "matchId", alert_type AS "alertType", message, created_at AS "createdAt" ' +
        'FROM sports_fan_alerts WHERE fan_id = $1 ORDER BY created_at DESC, alert_id DESC LIMIT $2',
        [fanId, limit],
      );
      return result.rows;
    },

    async getEventMetrics() {
      const result = await pool.query(
        'SELECT COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL \'60 seconds\')::int AS "eventsLastMinute", ' +
        'COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL \'5 minutes\')::int AS "eventsLastFiveMinutes", ' +
        'AVG(EXTRACT(EPOCH FROM (created_at - occurred_at)) * 1000) ' +
        'AS "averageProjectionLatencyMs", MAX(created_at) AS "lastProjectedAt" ' +
        'FROM sports_match_events WHERE created_at >= NOW() - INTERVAL \'5 minutes\'',
      );
      const row = result.rows[0];
      return {
        ...row,
        eventsPerSecond: Number(row.eventsLastMinute ?? 0) / 60,
        averageProjectionLatencyMs: row.averageProjectionLatencyMs === null
          ? null
          : Number(row.averageProjectionLatencyMs),
      };
    },

    async queueSimulation(matchId) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const matchResult = await client.query(
          'SELECT status FROM sports_matches WHERE match_id = $1 FOR UPDATE',
          [matchId],
        );
        const match = matchResult.rows[0];
        if (!match) {
          await client.query('ROLLBACK');
          return { kind: 'missing' };
        }
        if (match.status === 'finished') {
          await client.query('ROLLBACK');
          return { kind: 'not-simulatable' };
        }

        const jobResult = await client.query(
          'SELECT status FROM sports_simulation_jobs WHERE match_id = $1 FOR UPDATE',
          [matchId],
        );
        const job = jobResult.rows[0];
        if (job?.status === 'queued' || job?.status === 'running' || job?.status === 'completed') {
          await client.query('ROLLBACK');
          return { kind: 'already-requested', status: job.status };
        }
        if (job?.status === 'failed') {
          await client.query(
            'UPDATE sports_simulation_jobs SET status = \'queued\', requested_at = NOW(), ' +
            'finished_at = NULL, last_error = NULL, lease_owner = NULL, lease_until = NULL ' +
            'WHERE match_id = $1',
            [matchId],
          );
          await client.query('COMMIT');
          return { kind: 'queued', resumed: true };
        }
        if (match.status !== 'scheduled') {
          await client.query('ROLLBACK');
          return { kind: 'not-simulatable' };
        }

        await client.query(
          'INSERT INTO sports_simulation_jobs (match_id, status) VALUES ($1, \'queued\')',
          [matchId],
        );
        await client.query('COMMIT');
        return { kind: 'queued', resumed: false };
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  };
}
