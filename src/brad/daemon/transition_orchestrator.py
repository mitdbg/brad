import asyncio
from typing import Optional

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.diff.provisioning import ProvisioningDiff
from brad.blueprint.manager import BlueprintManager


class TransitionOrchestrator:
    def __init__(
        self,
        blueprint_mgr: BlueprintManager,
        next_blueprint: Blueprint,
    ) -> None:
        self._blueprint_mgr = blueprint_mgr
        self._next_blueprint = next_blueprint

    async def run_pre_transition(self) -> None:
        curr_blueprint = self._blueprint_mgr.get_blueprint()
        diff = BlueprintDiff.of(curr_blueprint, self._next_blueprint)
        if diff is None:
            # Nothing to do.
            return

        await self._blueprint_mgr.start_transition(self._next_blueprint)

        aurora_diff = diff.aurora_diff()
        redshift_diff = diff.redshift_diff()

        aurora_awaitable = self._run_aurora_pre_transition(aurora_diff)
        redshift_awaitable = self._run_redshift_pre_transition(redshift_diff)
        await asyncio.gather(aurora_awaitable, redshift_awaitable)

    async def run_post_transition(self) -> None:
        pass

    def _requires_aurora_failover(
        self, aurora_diff: Optional[ProvisioningDiff]
    ) -> bool:
        if aurora_diff is None:
            return False
        return aurora_diff.new_instance_type() is not None

    async def _run_aurora_pre_transition(
        self, diff: Optional[ProvisioningDiff]
    ) -> None:
        if diff is None:
            return

    async def _run_redshift_pre_transition(
        self, diff: Optional[ProvisioningDiff]
    ) -> None:
        if diff is None:
            return
