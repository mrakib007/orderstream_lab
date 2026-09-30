export function createSportsAnalyticsModel(pool) {
  return {
    async recordEvent(event, bucketStart) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const seen = await client.query(
          'INSERT INTO sports_analytics_processed_events (event_id) VALUES ($1) ' +
          'ON CONFLICT (event_id) DO NOTHING RETURNING event_id',
          [event.eventId],
        );
        if (seen.rowCount) {
          await client.query(
            'INSERT INTO sports_analytics_minute (match_id, bucket_start, event_type, event_count) ' +
            'VALUES ($1, $2, $3, 1) ' +
            'ON CONFLICT (match_id, bucket_start, event_type) ' +
            'DO UPDATE SET event_count = sports_analytics_minute.event_count + 1',
            [event.matchId, bucketStart, event.eventType],
          );
        }
        await client.query('COMMIT');
        return { duplicate: !seen.rowCount };
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  };
}
