import random
import logging

from database import Database

logger = logging.getLogger(__name__)


class TelemetryWorker:
    def __init__(self, worker_id: int, seed: int) -> None:
        self.worker_id = worker_id
        self.prng = random.Random(seed)

        # to generate queries
        self.max_dist = 1000
        self.max_close_cinemas = 80
        self.min_cap = 10
        self.max_cap = 1000

    def random_timerange(self):
        year = 2023
        month = self.prng.randint(1, 12)

        if month == 2:
            day = self.prng.randint(1, 28)
        elif month in [4, 6, 9, 11]:
            day = self.prng.randint(1, 30)
        else:
            day = self.prng.randint(1, 31)

        hour = self.prng.randint(0, 23)
        minute = self.prng.randint(0, 59)
        second = self.prng.randint(0, 59)
        millisecond = self.prng.randint(0, 999)

        return (
            f"{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millisecond:03}",
            f"{year:04}-{month:02}-{day:02} {hour+1:02}:{minute:02}:{second:02}.{millisecond:03}",
        )

    def random_gb_key(self):
        cols = ["ip", "event_id", "movie_id"]
        return self.prng.choice(cols)

    def query1(self, db: Database) -> bool:
        """
        COUNT with GROUP BY
        Note: Group by ip takes over 10x longer than group by movie_id or event_id
        """
        try:
            gb_key = self.random_gb_key()
            query = f"SELECT {gb_key}, COUNT(*) FROM telemetry GROUP BY {gb_key};"
            print(query)

            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False

    def query2(self, db: Database) -> bool:
        try:
            ts1, ts2 = self.random_timerange()
            query = f"""
                    SELECT COUNT(*)
                    FROM telemetry
                    WHERE timestamp > '{ts1}'
                    AND timestamp < '{ts2}';
                    """

            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False

    def query3(self, db: Database) -> bool:
        try:
            ts1, ts2 = self.random_timerange()
            gb_key = self.random_gb_key()

            query = f"""
                    SELECT {gb_key}, COUNT(*)
                    FROM telemetry
                    WHERE timestamp > '{ts1}'
                    AND timestamp < '{ts2}'
                    GROUP BY {gb_key};
                    """

            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False


if __name__ == "__main__":
    tw = TelemetryWorker(1, 20)
    print(tw.query1(None))
