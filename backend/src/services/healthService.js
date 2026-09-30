export function createHealthService({ pool, admin }) {
  return {
    async getHealth() {
      const services = {
        api: { ok: true },
        database: { ok: false },
        kafka: { ok: false },
      };

      try {
        await pool.query('SELECT 1');
        services.database = { ok: true };
      } catch (error) {
        services.database = { ok: false, error: error.message };
      }

      try {
        await admin.describeCluster();
        services.kafka = { ok: true };
      } catch (error) {
        services.kafka = { ok: false, error: error.message };
      }

      return {
        ok: services.database.ok && services.kafka.ok,
        services,
      };
    },
  };
}
