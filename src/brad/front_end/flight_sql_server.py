import logging
import threading

# pylint: disable-next=import-error,no-name-in-module,unused-import
import brad.native.pybind_brad_server as brad_server

logger = logging.getLogger(__name__)

class BradFlightSqlServer:
    def __init__(self, host, port):
        self._flight_sql_server = brad_server.BradFlightSqlServer()
        self._flight_sql_server.init(host, port)
        self._thread = threading.Thread(name="BradFlightSqlServer",
                                        target=self._serve)

    def start(self):
        self._thread.start()

    def stop(self):
        logger.info("BRAD FlightSQL server stopping...")
        self._flight_sql_server.shutdown()
        self._thread.join()
        logger.info("BRAD FlightSQL server stopped.")

    def _serve(self):
        self._flight_sql_server.serve()
