export function createNotificationModel(pool) {
  return {
    async findForOrder(orderId) {
      const result = await pool.query(
        'SELECT order_id, event_id, status, message, created_at ' +
        'FROM notifications WHERE order_id = $1',
        [orderId],
      );
      return result.rows[0] ?? null;
    },
  };
}
