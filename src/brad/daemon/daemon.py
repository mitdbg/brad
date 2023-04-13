import asyncio
import signal
import logging
import multiprocessing as mp

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.daemon.messages import ShutdownDaemon, NewBlueprint, ReceivedQuery
from brad.forecasting.forecaster import WorkloadForecaster
from brad.planner.neighborhood import NeighborhoodSearchPlanner
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


class BradDaemon:
    """
    Represents BRAD's background process.

    This code is written with the assumption that this daemon is spawned by the
    BRAD server. In the future, we may want the daemon to be launched
    independently and for it to communicate with the server via RPCs.
    """

    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        event_loop: asyncio.AbstractEventLoop,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        self._config = config
        self._schema_name = schema_name
        self._event_loop = event_loop
        self._input_queue = input_queue
        self._output_queue = output_queue

        self._planner = NeighborhoodSearchPlanner()
        self._forecaster = WorkloadForecaster()

    async def run_forever(self) -> None:
        """
        Starts running the daemon.
        """
        logger.info("The BRAD daemon is running.")
        self._planner.register_new_blueprint_callback(self._handle_new_blueprint)
        await asyncio.gather(self._read_server_messages(), self._planner.run_forever())

    async def _read_server_messages(self) -> None:
        """
        Waits for messages from the server and processes them.
        """
        while True:
            message = await self._event_loop.run_in_executor(
                None, self._input_queue.get
            )

            if isinstance(message, ShutdownDaemon):
                logger.debug("Daemon received shutdown message.")
                self._event_loop.create_task(self._shutdown())
                break

            elif isinstance(message, ReceivedQuery):
                # Might be a good idea to record this query string for offline
                # processing (it's a query trace).
                query_str = message.query_str
                logger.debug("Received query %s", query_str)
                self._forecaster.process(query_str)

            else:
                logger.debug("Received message %s", str(message))

    async def _handle_new_blueprint(self, blueprint: Blueprint) -> None:
        """
        Informs the server about a new blueprint.
        """
        await self._event_loop.run_in_executor(
            None, self._output_queue.put, NewBlueprint(blueprint)
        )

    async def _shutdown(self) -> None:
        logger.info("The BRAD daemon is shutting down...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._event_loop.stop()

    @staticmethod
    def launch_in_subprocess(
        config_path: str,
        schema_name: str,
        debug_mode: bool,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ) -> None:
        """
        Schedule this method to run in a child process to launch the BRAD
        daemon.
        """
        config = ConfigFile(config_path)
        set_up_logging(filename=config.daemon_log_path, debug_mode=debug_mode)

        event_loop = asyncio.new_event_loop()
        event_loop.set_debug(enabled=debug_mode)
        asyncio.set_event_loop(event_loop)

        # Signal handlers are inherited from the parent server process. We want
        # to ignore these signals since we receive a shutdown signal from the
        # server directly.
        for sig in [signal.SIGTERM, signal.SIGINT]:
            event_loop.add_signal_handler(sig, _noop)

        try:
            daemon = BradDaemon(
                config, schema_name, event_loop, input_queue, output_queue
            )
            event_loop.create_task(daemon.run_forever())
            logger.info("The BRAD daemon is starting...")
            event_loop.run_forever()
        finally:
            event_loop.close()
            logger.info("The BRAD daemon has shut down.")


def _noop():
    pass
