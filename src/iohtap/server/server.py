import time

from iohtap.config.file import ConfigFile
from iohtap.cost_model.model import CostModel
from iohtap.server.db_connection_manager import DBConnectionManager


class IOHTAPServer:
    def __init__(self, config: ConfigFile):
        self._config = config
        self._cost_model = CostModel()
        self._dbs = DBConnectionManager(self._config)

    def handle_query(self, sql_query: str):
        # Predict which DBMS to use.
        run_times = self._cost_model.predict_run_time(sql_query)
        db_to_use, _ = run_times.min_time_ms()

        # Actually execute the query
        connection = self._dbs.get_connection(db_to_use)
        cursor = connection.cursor()

        start = time.time()
        cursor.execute(sql_query)
        end = time.time()

        # Extract the results.
        # NOTE: Send the results back to the client.
        for row in cursor:
            print(" | ".join(map(str, row)))
        print()
        print("Ran for {:.2f} seconds".format(end - start))
