import { getKafkaOverview } from '../kafka-overview.js';

export function createLearningService({ pool, admin, config, learningModel, productModel }) {
  return {
    async getOverview() {
      const overview = {
        services: {
          api: { ok: true },
          database: { ok: false },
          kafka: { ok: false },
        },
        totals: { total: 0, pending: 0, confirmed: 0, out_of_stock: 0, publish_failed: 0 },
        products: [],
        topics: [],
        groups: [],
        errors: {},
      };

      try {
        await pool.query('SELECT 1');
        overview.services.database = { ok: true };
        const [totals, products] = await Promise.all([
          learningModel.getOrderTotals(),
          productModel.listCatalog(),
        ]);
        overview.totals = totals;
        overview.products = products;
      } catch (error) {
        overview.errors.database = error.message;
      }

      try {
        const kafkaOverview = await getKafkaOverview(
          admin,
          [config.ordersTopic, config.notificationsTopic],
          [config.orderGroupId, config.notificationGroupId],
        );
        overview.services.kafka = { ok: true };
        overview.topics = kafkaOverview.topics;
        overview.groups = kafkaOverview.groups;
      } catch (error) {
        overview.errors.kafka = error.message;
      }

      return overview;
    },
  };
}
