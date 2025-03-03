import random
import logging

from workload_utils.database import Database

logger = logging.getLogger(__name__)


class GeospatialWorker:
    def __init__(self, worker_id: int, seed: int) -> None:
        self.worker_id = worker_id
        self.prng = random.Random(seed)

        # to generate queries
        self.max_dist = 1000
        self.max_close_cinemas = 80
        self.min_cap = 10
        self.max_cap = 1000

    def query1(self, db: Database) -> bool:
        """
        For each cinema, count how many homes are within proximity dist
        """

        try:
            dist = self.prng.uniform(0, self.max_dist)
            query = f"""
            SELECT c.id, COUNT(*)
            FROM theatres AS c
            JOIN homes AS h ON ST_Distance(ST_Point(h.location_x,h.location_y), ST_Point(c.location_x,c.location_y)) < {dist}
            GROUP BY c.id
            ORDER BY c.id;
            """
            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False

    def query2(self, db: Database) -> bool:
        """
        Select homes with more than cutoff, cinemas in proximity of dist.
        """

        try:
            dist = self.prng.uniform(0, self.max_dist)
            cutoff = self.prng.randint(0, self.max_close_cinemas)

            query = f"""
            WITH homecinema(h, c) AS (
                SELECT h.id, c.id
                FROM homes AS h
                JOIN theatres AS c ON ST_Distance(ST_Point(h.location_x,h.location_y), ST_Point(c.location_x,c.location_y)) < {dist}
            ),
            homecinemacount(h, c) AS (
                SELECT h, COUNT(*)
                FROM homecinema
                GROUP BY h
            )
            SELECT h, c
            FROM homecinemacount
            WHERE c > {cutoff}
            ORDER BY c;
            """
            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False

    def query3(self, db: Database) -> bool:
        """
        Distance between showing's cinema and where ticket was purchased
        """

        try:
            cap = self.prng.randint(self.min_cap, self.max_cap)

            query = f"""
            SELECT ST_Distance(ST_Point(t.location_x, t.location_y), ST_Point(o.location_x, o.location_y))
            FROM ticket_orders as o
            JOIN showings as s ON s.id = o.showing_id
            JOIN theatres as t ON t.id = s.theatre_id
            WHERE s.total_capacity < {cap};
            """
            db.execute_sync(query)
            return True

        except:  # pylint: disable=bare-except
            return False
