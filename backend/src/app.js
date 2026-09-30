import express from 'express';
import { createHealthController } from './controllers/healthController.js';
import { createLearningController } from './controllers/learningController.js';
import { createOrderController } from './controllers/orderController.js';
import { createProductController } from './controllers/productController.js';
import { createSportsController } from './controllers/sportsController.js';
import { createLearningModel } from './models/learningModel.js';
import { createNotificationModel } from './models/notificationModel.js';
import { createOrderModel } from './models/orderModel.js';
import { createProductModel } from './models/productModel.js';
import { createSportsCatalogModel } from './models/sportsCatalogModel.js';
import { createApiRoutes } from './routes/apiRoutes.js';
import { createHealthService } from './services/healthService.js';
import { createLearningService } from './services/learningService.js';
import { createOrderService } from './services/orderService.js';
import { createSportsCatalogService } from './services/sportsCatalogService.js';

export function createApp({ pool, admin, producer, config, logger = console }) {
  const productModel = createProductModel(pool);
  const orderModel = createOrderModel(pool);
  const notificationModel = createNotificationModel(pool);
  const learningModel = createLearningModel(pool);
  const sportsCatalogModel = createSportsCatalogModel(pool);

  const controllers = {
    health: createHealthController(createHealthService({ pool, admin })),
    products: createProductController(productModel, logger),
    orders: createOrderController(
      createOrderService({ orderModel, productModel, notificationModel, producer, config, logger }),
      logger,
    ),
    learning: createLearningController(
      createLearningService({ pool, admin, config, learningModel, productModel }),
    ),
    sports: createSportsController(
      createSportsCatalogService(sportsCatalogModel, { admin, config }),
      logger,
    ),
  };

  const app = express();
  app.use(express.json());
  app.use('/api', createApiRoutes(controllers));
  return app;
}
