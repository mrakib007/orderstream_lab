export function createOrderModel(pool) {
  return {
    async listRecent() {
      const result = await pool.query(
        'SELECT id, product_id, product, quantity, status, created_at, processed_at, ' +
        'kafka_topic, kafka_key, kafka_partition, kafka_offset ' +
        'FROM orders ORDER BY created_at DESC LIMIT 50',
      );
      return result.rows;
    },

    async findById(id) {
      const result = await pool.query(
        'SELECT id, product_id, product, quantity, status, created_at, processed_at, ' +
        'event_id, kafka_topic, kafka_key, kafka_partition, kafka_offset ' +
        'FROM orders WHERE id = $1',
        [id],
      );
      return result.rows[0] ?? null;
    },

    async insertPending({ id, productId, productName, quantity, eventId }) {
      await pool.query(
        "INSERT INTO orders (id, product_id, product, quantity, status, event_id) " +
        "VALUES ($1, $2, $3, $4, 'pending', $5)",
        [id, productId, productName, quantity, eventId],
      );
    },

    async markPublishFailed(id) {
      await pool.query("UPDATE orders SET status = 'publish_failed' WHERE id = $1", [id]);
    },

    async saveKafkaTrace(id, trace) {
      await pool.query(
        'UPDATE orders SET kafka_topic = $2, kafka_key = $3, kafka_partition = $4, kafka_offset = $5 WHERE id = $1',
        [id, trace.topic, trace.key, trace.partition, trace.offset],
      );
    },
  };
}
