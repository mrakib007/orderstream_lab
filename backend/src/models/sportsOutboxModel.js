export function createSportsOutboxModel(pool) {
  return {
    async claimNext(workerId) {
      const result = await pool.query(
        'WITH candidate AS (' +
        '  SELECT current.outbox_id FROM sports_outbox_events current ' +
        '  WHERE current.published_at IS NULL AND current.next_attempt_at <= NOW() ' +
        '    AND (current.locked_until IS NULL OR current.locked_until <= NOW()) ' +
        '    AND NOT EXISTS (' +
        '      SELECT 1 FROM sports_outbox_events earlier ' +
        '      WHERE earlier.topic = current.topic AND earlier.message_key = current.message_key ' +
        '        AND earlier.outbox_id < current.outbox_id AND earlier.published_at IS NULL' +
        '    ) ' +
        '  ORDER BY current.outbox_id FOR UPDATE SKIP LOCKED LIMIT 1' +
        ') ' +
        'UPDATE sports_outbox_events e SET locked_by = $1, locked_until = NOW() + INTERVAL \'45 seconds\', ' +
        'attempts = e.attempts + 1 FROM candidate c WHERE e.outbox_id = c.outbox_id ' +
        'RETURNING e.outbox_id AS "outboxId", e.event_id AS "eventId", e.topic, e.message_key AS "messageKey", ' +
        'e.payload, e.attempts',
        [workerId],
      );
      return result.rows[0] ?? null;
    },

    async markPublished(outboxId, workerId, trace) {
      const result = await pool.query(
        'UPDATE sports_outbox_events SET published_at = NOW(), kafka_partition = $3, kafka_offset = $4, ' +
        'locked_by = NULL, locked_until = NULL, last_error = NULL ' +
        'WHERE outbox_id = $1 AND locked_by = $2 AND published_at IS NULL',
        [outboxId, workerId, trace.partition, trace.offset],
      );
      return result.rowCount > 0;
    },

    async retryLater(outboxId, workerId, errorMessage) {
      await pool.query(
        'UPDATE sports_outbox_events SET next_attempt_at = NOW() + make_interval(secs => ' +
        'LEAST(60, power(2, LEAST(attempts, 6))::int)), last_error = $3, ' +
        'locked_by = NULL, locked_until = NULL ' +
        'WHERE outbox_id = $1 AND locked_by = $2 AND published_at IS NULL',
        [outboxId, workerId, String(errorMessage).slice(0, 1000)],
      );
    },
  };
}
