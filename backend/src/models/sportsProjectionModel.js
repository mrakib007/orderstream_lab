function projectionError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

export function createSportsProjectionModel(pool) {
  return {
    async applyEvent({ event, source }) {
      const client = await pool.connect();
      let inTransaction = false;
      let snapshot;
      try {
        await client.query('BEGIN');
        inTransaction = true;
        const matchResult = await client.query(
          'SELECT match_id AS "matchId", home_team_id AS "homeTeamId", away_team_id AS "awayTeamId", ' +
          'status, home_score AS "homeScore", away_score AS "awayScore", ' +
          'last_event_sequence AS "lastEventSequence", reconciliation_required AS "reconciliationRequired" ' +
          'FROM sports_matches WHERE match_id = $1 FOR UPDATE',
          [event.matchId],
        );
        const match = matchResult.rows[0];
        if (!match) throw projectionError('UNKNOWN_MATCH', 'The event references an unknown match.');

        const duplicateResult = await client.query(
          'SELECT event_id AS "eventId", sequence FROM sports_match_events ' +
          'WHERE match_id = $1 AND (event_id = $2 OR sequence = $3)',
          [event.matchId, event.eventId, event.sequence],
        );
        if (duplicateResult.rows.length) {
          const duplicate = duplicateResult.rows[0];
          if (duplicate.eventId === event.eventId && Number(duplicate.sequence) === event.sequence) {
            await client.query('COMMIT');
            inTransaction = false;
            return { duplicate: true, snapshot: match };
          }
          await client.query(
            'UPDATE sports_matches SET reconciliation_required = TRUE, updated_at = NOW() WHERE match_id = $1',
            [event.matchId],
          );
          await client.query('COMMIT');
          inTransaction = false;
          throw projectionError('SEQUENCE_CONFLICT', 'An event ID or match sequence conflicts with a stored event.');
        }

        if (match.reconciliationRequired) {
          await client.query('COMMIT');
          inTransaction = false;
          throw projectionError('RECONCILIATION_REQUIRED', 'The match is paused until its event history is reconciled.');
        }
        if (event.sequence !== Number(match.lastEventSequence) + 1) {
          await client.query(
            'UPDATE sports_matches SET reconciliation_required = TRUE, updated_at = NOW() WHERE match_id = $1',
            [event.matchId],
          );
          await client.query('COMMIT');
          inTransaction = false;
          throw projectionError('SEQUENCE_GAP', 'Expected sequence ' + (Number(match.lastEventSequence) + 1) + ' but received ' + event.sequence + '.');
        }

        let nextStatus = match.status;
        let homeScore = Number(match.homeScore);
        let awayScore = Number(match.awayScore);
        let transitionError = null;
        const teamIsMatchParticipant = event.teamId === match.homeTeamId || event.teamId === match.awayTeamId;
        if (event.teamId && !teamIsMatchParticipant) {
          await client.query(
            'UPDATE sports_matches SET reconciliation_required = TRUE, updated_at = NOW() WHERE match_id = $1',
            [event.matchId],
          );
          await client.query('COMMIT');
          inTransaction = false;
          throw projectionError('INVALID_TEAM', 'The event team does not play in this match.');
        }

        switch (event.eventType) {
          case 'match_started':
            if (match.status !== 'scheduled') {
              transitionError = projectionError('INVALID_TRANSITION', 'A match can start only from scheduled status.');
              break;
            }
            nextStatus = 'live';
            break;
          case 'goal':
          case 'yellow_card':
          case 'red_card':
            if (!['live', 'half_time'].includes(match.status) || !event.teamId) {
              transitionError = projectionError('INVALID_TRANSITION', 'A goal or card requires a live match and a participating team.');
              break;
            }
            nextStatus = 'live';
            if (event.eventType === 'goal') {
              if (event.teamId === match.homeTeamId) homeScore += 1;
              else awayScore += 1;
            }
            break;
          case 'half_time':
            if (match.status !== 'live') {
              transitionError = projectionError('INVALID_TRANSITION', 'Half time can follow a live match only.');
              break;
            }
            nextStatus = 'half_time';
            break;
          case 'match_completed':
            if (!['live', 'half_time'].includes(match.status)) {
              transitionError = projectionError('INVALID_TRANSITION', 'A match can finish only after it has started.');
              break;
            }
            nextStatus = 'finished';
            break;
          default:
            transitionError = projectionError('UNKNOWN_EVENT_TYPE', 'The event type is not supported by the score projection.');
        }

        if (transitionError) {
          await client.query(
            'UPDATE sports_matches SET reconciliation_required = TRUE, updated_at = NOW() WHERE match_id = $1',
            [event.matchId],
          );
          await client.query('COMMIT');
          inTransaction = false;
          throw transitionError;
        }

        const inserted = await client.query(
          'INSERT INTO sports_match_events ' +
          '(event_id, match_id, sequence, event_type, team_id, player_name, match_minute, description, payload, occurred_at, kafka_topic, kafka_partition, kafka_offset) ' +
          'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, $13) ' +
          'ON CONFLICT DO NOTHING RETURNING event_id',
          [event.eventId, event.matchId, event.sequence, event.eventType, event.teamId ?? null,
            event.playerName ?? null, event.matchMinute ?? null, event.description, JSON.stringify(event),
            event.occurredAt, source.topic, source.partition, source.offset],
        );
        if (!inserted.rowCount) {
          await client.query(
            'UPDATE sports_matches SET reconciliation_required = TRUE, updated_at = NOW() WHERE match_id = $1',
            [event.matchId],
          );
          await client.query('COMMIT');
          inTransaction = false;
          throw projectionError('SEQUENCE_CONFLICT', 'The event could not be stored because its key already exists.');
        }

        const updatedResult = await client.query(
          'UPDATE sports_matches SET status = $2, home_score = $3, away_score = $4, ' +
          'last_event_sequence = $5, updated_at = NOW() WHERE match_id = $1 ' +
          'RETURNING match_id AS "matchId", status, home_score AS "homeScore", away_score AS "awayScore", ' +
          'last_event_sequence AS "lastEventSequence", reconciliation_required AS "reconciliationRequired"',
          [event.matchId, nextStatus, homeScore, awayScore, event.sequence],
        );
        snapshot = { ...match, ...updatedResult.rows[0] };
        await client.query('COMMIT');
        inTransaction = false;
      } catch (error) {
        if (inTransaction) await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
      return { duplicate: false, snapshot };
    },
  };
}
