import { HttpError } from '../errors/HttpError.js';

export function createOrderController(orderService, logger = console) {
  return {
    listOrders: async (_request, response) => {
      try {
        response.json(await orderService.listOrders());
      } catch (error) {
        logger.error('Could not load orders:', error.message);
        response.status(500).json({ error: 'Could not load orders.' });
      }
    },

    getOrder: async (request, response) => {
      try {
        const order = await orderService.getOrder(request.params.id);
        if (!order) {
          response.status(404).json({ error: 'Order not found.' });
          return;
        }
        response.json(order);
      } catch (error) {
        logger.error('Could not load order detail:', error.message);
        response.status(400).json({ error: 'Could not load this order.' });
      }
    },

    createOrder: async (request, response) => {
      try {
        const order = await orderService.createOrder(request.body);
        response.status(201).json(order);
      } catch (error) {
        if (error instanceof HttpError) {
          response.status(error.statusCode).json(error.body);
          return;
        }
        logger.error('Could not create order:', error.message);
        response.status(500).json({ error: 'Could not create order.' });
      }
    },
  };
}
