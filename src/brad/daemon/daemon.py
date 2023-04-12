import logging
import multiprocessing as mp

from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


class BradDaemon:
    """
    Represents BRAD's background process.
    """

    def __init__(
        self,
        config: ConfigFile,
        schema_name: str,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        self._config = config
        self._schema_name = schema_name
        self._input_queue = input_queue
        self._output_queue = output_queue

    def run(self):
        pass

    @classmethod
    def launch_as_subprocess(
        cls,
        config_path: str,
        schema_name: str,
        input_queue: mp.Queue,
        output_queue: mp.Queue,
    ):
        config = ConfigFile(config_path)
        daemon = BradDaemon(config, schema_name, input_queue, output_queue)
        daemon.run()
