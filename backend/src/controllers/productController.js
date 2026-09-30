export function createProductController(productModel, logger = console) {
  return {
    listProducts: async (_request, response) => {
      try {
        response.json(await productModel.listCatalog());
      } catch (error) {
        logger.error('Could not load products:', error.message);
        response.status(500).json({ error: 'Could not load product catalog.' });
      }
    },
  };
}
