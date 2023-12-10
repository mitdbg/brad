from typing import Optional

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.system_event import SystemEvent
from brad.daemon.system_event_logger import SystemEventLogger


class BlueprintWatchdog:
    """
    Used to prevent selecting blueprints that would cause issues. If the
    watchdog fires, we must redo the experiment (this is meant as a backstop).
    """

    def __init__(self, event_logger: Optional[SystemEventLogger]) -> None:
        self._event_logger = event_logger

    def reject_blueprint(self, blueprint: Blueprint) -> bool:
        # Embedding table should not leave Aurora.
        try:
            embedding_locations = blueprint.get_table_locations("embeddings")
            if Engine.Aurora not in embedding_locations:
                if self._event_logger is not None:
                    self._event_logger.log(
                        SystemEvent.WatchdogFired,
                        f"embedding_left_aurora: {str(embedding_locations)}",
                    )
                return True
        except ValueError:
            # Indicates the table is not used in this schema - no problem.
            pass

        # All ok.
        return False
