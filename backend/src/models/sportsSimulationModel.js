export function createSportsSimulationModel(pool) {
  return {
    async claimNextJob(workerId) {
      const result = await pool.query(
        'WITH candidate AS (' +
        '  SELECT j.match_id FROM sports_simulation_jobs j ' +
        '  WHERE j.status = \'queued\' OR (j.status = \'running\' AND j.lease_until <= NOW()) ' +
        '  ORDER BY j.requested_at, j.match_id FOR UPDATE SKIP LOCKED LIMIT 1' +
        ') ' +
        'UPDATE sports_simulation_jobs j SET status = \'running\', started_at = COALESCE(j.started_at, NOW()), ' +
        'lease_owner = $1, lease_until = NOW() + INTERVAL \'45 seconds\' ' +
        'FROM candidate c WHERE j.match_id = c.match_id ' +
        'RETURNING j.match_id AS "matchId", j.next_event_sequence AS "nextEventSequence"',
        [workerId],
      );
      return result.rows[0] ?? null;
    },

    async getMatchForSimulation(matchId) {
      const result = await pool.query(
        'SELECT m.match_id AS "matchId", m.home_team_id AS "homeTeamId", m.away_team_id AS "awayTeamId", ' +
        'home.name AS "homeTeam", away.name AS "awayTeam" ' +
        'FROM sports_matches m ' +
        'JOIN sports_teams home ON home.team_id = m.home_team_id ' +
        'JOIN sports_teams away ON away.team_id = m.away_team_id ' +
        'WHERE m.match_id = $1',
        [matchId],
      );
      return result.rows[0] ?? null;
    },

    async appendOutboxEvent({ workerId, matchId, topic, event }) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const jobResult = await client.query(
          'SELECT status, next_event_sequence, lease_owner, lease_until > NOW() AS lease_valid ' +
          'FROM sports_simulation_jobs WHERE match_id = $1 FOR UPDATE',
          [matchId],
        );
        const job = jobResult.rows[0];
        if (!job || job.status !== 'running' || job.lease_owner !== workerId || !job.lease_valid) {
          await client.query('ROLLBACK');
          return { kind: 'lease-lost' };
        }
        if (Number(job.next_event_sequence) !== event.sequence) {
          await client.query('ROLLBACK');
          return { kind: 'sequence-mismatch', nextEventSequence: job.next_event_sequence };
        }

        await client.query(
          'INSERT INTO sports_outbox_events (event_id, topic, message_key, payload) VALUES ($1, $2, $3, $4::jsonb)',
          [event.eventId, topic, matchId, JSON.stringify(event)],
        );
        const lastEvent = event.eventType === 'match_completed';
        await client.query(
          'UPDATE sports_simulation_jobs SET next_event_sequence = next_event_sequence + 1, ' +
          'lease_until = NOW() + INTERVAL \'45 seconds\', ' +
          'status = CASE WHEN $3 THEN \'completed\' ELSE status END, ' +
          'finished_at = CASE WHEN $3 THEN NOW() ELSE finished_at END, ' +
          'lease_owner = CASE WHEN $3 THEN NULL ELSE lease_owner END, ' +
          'lease_until = CASE WHEN $3 THEN NULL ELSE NOW() + INTERVAL \'45 seconds\' END ' +
          'WHERE match_id = $1 AND lease_owner = $2',
          [matchId, workerId, lastEvent],
        );
        await client.query('COMMIT');
        return { kind: lastEvent ? 'completed' : 'appended' };
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },

    async markJobFailed(matchId, workerId, errorMessage) {
      await pool.query(
        'UPDATE sports_simulation_jobs SET status = \'failed\', finished_at = NOW(), last_error = $3, ' +
        'lease_owner = NULL, lease_until = NULL WHERE match_id = $1 AND lease_owner = $2',
        [matchId, workerId, String(errorMessage).slice(0, 1000)],
      );
    },
  };
}
