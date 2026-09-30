export function createProductModel(pool) {
  return {
    async listCatalog() {
      const result = await pool.query(
        'SELECT product_id, name, available_stock FROM products ORDER BY product_id',
      );
      return result.rows;
    },

    async findForOrder(productId) {
      const result = await pool.query(
        'SELECT product_id, name FROM products WHERE product_id = $1',
        [productId],
      );
      return result.rows[0] ?? null;
    },
  };
}
