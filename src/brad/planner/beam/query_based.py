import asyncio
import logging

from brad.planner import BlueprintPlanner

logger = logging.getLogger(__name__)


class QueryBasedBeamPlanner(BlueprintPlanner):
    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(3)
            logger.debug("Planner is checking if a replan is needed...")
            if self._check_if_metrics_warrant_replanning():
                await self.run_replan()

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False

    async def run_replan(self) -> None:
        logger.info("Running a replan...")

        # 1. Fetch the next workload and make query execution predictions.
        next_workload = self._workload_provider.next_workload()
        self._analytics_latency_scorer.apply_predicted_latencies(next_workload)

        # 2. Compute query gains and reorder queries by their gain in descending
        # order.
        gains = next_workload.compute_latency_gains()
        query_indices = list(range(len(next_workload.analytical_queries())))
        query_indices.sort(key=lambda idx: gains[idx], reverse=True)

        # 3. Initialize the beam.

        # 4. Run beam search.

        # 5. Return best blueprint.
