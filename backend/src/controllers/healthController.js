export function createHealthController(healthService) {
  return {
    getHealth: async (_request, response) => {
      const health = await healthService.getHealth();
      response.status(health.ok ? 200 : 503).json(health);
    },
  };
}
