import { Router } from 'express';

export function createApiRoutes(controllers) {
  const router = Router();

  router.get('/health', controllers.health.getHealth);
  router.get('/products', controllers.products.listProducts);
  router.get('/orders', controllers.orders.listOrders);
  router.get('/orders/:id', controllers.orders.getOrder);
  router.post('/orders', controllers.orders.createOrder);
  router.get('/learning/overview', controllers.learning.getOverview);
  router.get('/sports/matches', controllers.sports.listMatches);
  router.get('/sports/matches/:matchId/events', controllers.sports.listEvents);
  router.post('/sports/matches/:matchId/simulate', controllers.sports.queueSimulation);
  router.get('/sports/fan-alerts', controllers.sports.listFanAlerts);

  return router;
}
