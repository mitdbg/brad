import asyncio
import logging
import signal
import multiprocessing as mp

from brad.config.file import ConfigFile
from brad.front_end.front_end import BradFrontEnd
from brad.front_end.vdbe.vdbe_front_end import BradVdbeFrontEnd
from brad.provisioning.directory import Directory
from brad.utils import set_up_logging
from brad.vdbe.models import VirtualInfrastructure

logger = logging.getLogger(__name__)


def start_front_end(
    fe_index: int,
    config: ConfigFile,
    schema_name: str,
    path_to_system_config: str,
    debug_mode: bool,
    directory: Directory,
    input_queue: mp.Queue,
    output_queue: mp.Queue,
) -> None:
    """
    Schedule this method to run in a child process to launch a BRAD front
    end server.
    """
    set_up_logging(filename=config.front_end_log_file(fe_index), debug_mode=debug_mode)

    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=debug_mode)
    asyncio.set_event_loop(event_loop)

    # Signal handlers are inherited from the parent server process. We want
    # to ignore these signals since we receive a shutdown signal from the
    # daemon directly.
    for sig in [signal.SIGTERM, signal.SIGINT]:
        event_loop.add_signal_handler(sig, _noop)
    # This is useful for debugging purposes.
    event_loop.add_signal_handler(signal.SIGUSR1, _drop_into_pdb)
    event_loop.set_exception_handler(_handle_exception)

    try:
        front_end = BradFrontEnd(
            fe_index,
            config,
            schema_name,
            path_to_system_config,
            debug_mode,
            directory,
            input_queue,
            output_queue,
        )
        event_loop.create_task(front_end.serve_forever())
        logger.info("BRAD front end %d is starting...", fe_index)
        event_loop.run_forever()
    finally:
        event_loop.close()
        logger.info("BRAD front end %d has shut down.", fe_index)


def start_vdbe_front_end(
    config: ConfigFile,
    schema_name: str,
    path_to_system_config: str,
    debug_mode: bool,
    directory: Directory,
    initial_infra: VirtualInfrastructure,
    input_queue: mp.Queue,
    output_queue: mp.Queue,
) -> None:
    """
    Schedule this method to run in a child process to launch a BRAD front
    end server.
    """
    set_up_logging(
        filename=config.front_end_log_file(BradVdbeFrontEnd.NUMERIC_IDENTIFIER),
        debug_mode=debug_mode,
    )

    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=debug_mode)
    asyncio.set_event_loop(event_loop)

    # Signal handlers are inherited from the parent server process. We want
    # to ignore these signals since we receive a shutdown signal from the
    # daemon directly.
    for sig in [signal.SIGTERM, signal.SIGINT]:
        event_loop.add_signal_handler(sig, _noop)
    # This is useful for debugging purposes.
    event_loop.add_signal_handler(signal.SIGUSR1, _drop_into_pdb)
    event_loop.set_exception_handler(_handle_exception)

    try:
        front_end = BradVdbeFrontEnd(
            config,
            schema_name,
            path_to_system_config,
            debug_mode,
            directory,
            initial_infra,
            input_queue,
            output_queue,
        )
        event_loop.create_task(front_end.serve_forever())
        logger.info("BRAD VDBE front end is starting...")
        event_loop.run_forever()
    finally:
        event_loop.close()
        logger.info("BRAD VDBE front end has shut down.")


def _handle_exception(event_loop, context):
    message = context.get("exception", context["message"])
    logging.error("Encountered uncaught exception: %s", message)
    logging.error("%s", context)
    if event_loop.is_closed():
        return


def _drop_into_pdb():
    import pdb

    # N.B. Leaving this in is intentional.
    pdb.set_trace()  # pylint: disable=forgotten-debug-statement


def _noop():
    pass
