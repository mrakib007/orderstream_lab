export function createLearningController(learningService) {
  return {
    getOverview: async (_request, response) => {
      response.json(await learningService.getOverview());
    },
  };
}
