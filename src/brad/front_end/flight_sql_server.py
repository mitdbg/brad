import logging
import threading
from typing import Callable

# pylint: disable-next=import-error,no-name-in-module,unused-import
import brad.native.pybind_brad_server as brad_server

logger = logging.getLogger(__name__)


class BradFlightSqlServer:
    def __init__(self, host: str, port: int, callback: Callable) -> None:
        # pylint: disable-next=c-extension-no-member
        self._flight_sql_server = brad_server.BradFlightSqlServer()
        self._flight_sql_server.init(host, port, callback)
        self._thread = threading.Thread(name="BradFlightSqlServer", target=self._serve)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        logger.info("BRAD FlightSQL server stopping...")
        self._flight_sql_server.shutdown()
        self._thread.join()
        logger.info("BRAD FlightSQL server stopped.")

    def _serve(self) -> None:
        self._flight_sql_server.serve()
