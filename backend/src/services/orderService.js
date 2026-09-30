import { randomUUID } from 'node:crypto';
import { HttpError } from '../errors/HttpError.js';

export function createOrderService({ orderModel, productModel, notificationModel, producer, config, logger = console }) {
  return {
    listOrders() {
      return orderModel.listRecent();
    },

    async getOrder(id) {
      const order = await orderModel.findById(id);
      if (!order) return null;

      const notification = await notificationModel.findForOrder(id);
      return { ...order, notification };
    },

    async createOrder(input) {
      const productId = typeof input?.productId === 'string' ? input.productId.trim() : '';
      const quantity = Number(input?.quantity);

      if (!productId || !Number.isInteger(quantity) || quantity < 1 || quantity > 1000) {
        throw new HttpError(400, {
          error: 'Choose a catalog product and a whole-number quantity from 1 to 1000.',
        });
      }

      let product;
      try {
        product = await productModel.findForOrder(productId);
      } catch (error) {
        logger.error('Could not validate product:', error.message);
        throw new HttpError(503, { error: 'Could not read the product catalog.' });
      }

      if (!product) {
        throw new HttpError(400, { error: 'That product is not in the catalog.' });
      }

      const id = randomUUID();
      const eventId = randomUUID();
      const createdAt = new Date().toISOString();
      const event = {
        eventId,
        orderId: id,
        createdAt,
        productId: product.product_id,
        product: product.name,
        quantity,
      };

      try {
        await orderModel.insertPending({
          id,
          productId: product.product_id,
          productName: product.name,
          quantity,
          eventId,
        });
      } catch (error) {
        logger.error('Could not save order:', error.message);
        throw new HttpError(500, { error: 'Could not save the order in PostgreSQL.' });
      }

      let metadata;
      try {
        metadata = await producer.send({
          topic: config.ordersTopic,
          acks: -1,
          messages: [{ key: product.product_id, value: JSON.stringify(event) }],
        });
      } catch (error) {
        await orderModel.markPublishFailed(id).catch(() => {});
        logger.error('Could not publish order event:', error.message);
        throw new HttpError(502, {
          error: 'Order was saved, but Kafka could not confirm its event.',
          orderId: id,
        });
      }

      const acknowledgement = metadata[0];
      const trace = {
        topic: acknowledgement.topicName ?? config.ordersTopic,
        key: product.product_id,
        partition: acknowledgement.partition,
        offset: acknowledgement.baseOffset,
      };

      try {
        await orderModel.saveKafkaTrace(id, trace);
      } catch (error) {
        logger.error('Kafka accepted order ' + id + ', but its trace could not be saved:', error.message);
      }

      return {
        id,
        productId: product.product_id,
        productName: product.name,
        quantity,
        status: 'pending',
        createdAt,
        kafka: trace,
      };
    },
  };
}
