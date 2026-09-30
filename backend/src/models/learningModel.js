export function createLearningModel(pool) {
  return {
    async getOrderTotals() {
      const result = await pool.query(
        'SELECT COUNT(*)::int AS total, ' +
        "COUNT(*) FILTER (WHERE status = 'pending')::int AS pending, " +
        "COUNT(*) FILTER (WHERE status = 'confirmed')::int AS confirmed, " +
        "COUNT(*) FILTER (WHERE status = 'out_of_stock')::int AS out_of_stock, " +
        "COUNT(*) FILTER (WHERE status = 'publish_failed')::int AS publish_failed " +
        'FROM orders',
      );
      return result.rows[0];
    },
  };
}
