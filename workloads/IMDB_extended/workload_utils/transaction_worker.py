import random
import logging
from datetime import datetime, timedelta
import time
from typing import List, Tuple, Any

from brad.grpc_client import RowList, BradClientError
from workload_utils.database import Database
from workload_utils.dataset_config import (
    MIN_MOVIE_ID,
    THEATRES_PER_SF,
    MIN_THEATRE_ID,
    MIN_CAPACITY,
    MAX_CAPACITY,
    MAX_MOVIE_ID_ORIGINAL,
    MAX_MOVIE_ID_20GB,
    MAX_MOVIE_ID_100GB,
)

logger = logging.getLogger(__name__)


class TransactionWorker:
    def __init__(
        self,
        worker_id: int,
        seed: int,
        scale_factor: int,
        dataset_type: str = "original",
    ) -> None:
        self.worker_id = worker_id
        self.prng = random.Random(seed)

        self.min_movie_id = MIN_MOVIE_ID
        if dataset_type == "original":
            self.max_movie_id = MAX_MOVIE_ID_ORIGINAL
        elif dataset_type == "20gb":
            self.max_movie_id = MAX_MOVIE_ID_20GB
        elif dataset_type == "100gb":
            self.max_movie_id = MAX_MOVIE_ID_100GB
        else:
            raise RuntimeError(dataset_type)
        self.min_theatre_id = MIN_THEATRE_ID
        self.max_theatre_id = THEATRES_PER_SF * scale_factor
        self.offset_date = datetime(year=2023, month=7, day=18)

        self.showings_to_add = (1, 3)  # [min, max]
        self.showing_capacity = (MIN_CAPACITY, MAX_CAPACITY)  # [min, max]
        self.showings_to_consider = (1, 5)  # [min, max]
        self.ticket_quantity = (1, 2)  # [min, max]
        self.loc_max = 1e6
        self.showing_years = 2

    def edit_movie_note(self, db: Database) -> bool:
        """
        Represents editing the "misc info" for a specific movie.

        - Select a movie (title table), read its id
        - Read matching rows in
            - movie_info
            - aka_title
        - Edit note (append characters or remove them) in
            - movie_info
            - aka_title
        """

        # 1. Select a random movie id.
        movie_id = self.prng.randint(self.min_movie_id, self.max_movie_id)

        try:
            # Start the transaction.
            db.begin_sync()

            # 2. Select matching movie infos.
            infos = db.execute_sync(
                f"SELECT id, note FROM movie_info WHERE movie_id = {movie_id}"
            )

            # 3. Select matching aka_titles.
            titles = db.execute_sync(
                f"SELECT id, note FROM aka_title WHERE movie_id = {movie_id}"
            )

            # 3. Edit the notes and titles.
            movie_note_edits = self._make_note_edits(infos)
            title_note_edits = self._make_note_edits(titles)

            # 4. Write back the edits.
            for edit in movie_note_edits:
                db.execute_sync(
                    f"UPDATE movie_info SET note = '{edit[1]}' WHERE id = {edit[0]}"
                )

            for edit in title_note_edits:
                db.execute_sync(
                    f"UPDATE aka_title SET note = '{edit[1]}' WHERE id = {edit[0]}"
                )

            # 5. Commit changes.
            db.commit_sync()
            return True

        except:  # pylint: disable=bare-except
            logger.exception("Need to rollback.")
            db.rollback_sync()
            return False

    def add_new_showing(self, db: Database) -> bool:
        """
        Represents a theatre employee adding new showings.

        - Select theatre by id
        - Select movie by id
        - Insert into showing
        """
        # 1. Select a random theatre id.
        theatre_id = self.prng.randint(self.min_theatre_id, self.max_theatre_id)

        # 2. Select a random movie id.
        movie_id = self.prng.randint(self.min_movie_id, self.max_movie_id)

        showings_to_add = self.prng.randint(*self.showings_to_add)

        try:
            # Start the transaction.
            db.begin_sync()

            # 3. Verify that the movie actually exists.
            rows = db.execute_sync(f"SELECT id FROM title WHERE id = {movie_id}")
            if len(rows) == 0:
                # We chose an invalid movie. But we still consider this transaction
                # to be a "success" and return true.
                db.commit_sync()
                return True

            # 4. Insert the showing.
            for _ in range(showings_to_add):
                capacity = self.prng.randint(*self.showing_capacity)
                day_offset = self.prng.randint(1, 365 * self.showing_years)
                hour_offset = self.prng.randint(0, 23)
                date_time = self.offset_date + timedelta(
                    days=day_offset, hours=hour_offset
                )
                formatted_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
                db.execute_sync(
                    "INSERT INTO showings (theatre_id, movie_id, date_time, total_capacity, seats_left) "
                    f"VALUES ({theatre_id}, {movie_id}, '{formatted_date_time}', {capacity}, {capacity})"
                )

            db.commit_sync()
            return True

        except:  # pylint: disable=bare-except
            logger.exception("Need to rollback.")
            db.rollback_sync()
            return False

    def purchase_tickets(self, db: Database, select_using_name: bool) -> bool:
        """
        Represents a user buying tickets for a specific showing.

        - Select theatre (by name or id)
        - Select showing by theatre id and date
        - Insert into `ticket_order`
        - Update the `showing` entry
        """
        # 1. Select a random theatre number.
        theatre_num = self.prng.randint(self.min_theatre_id, self.max_theatre_id)

        try:
            # Start the transaction.
            db.begin_sync()
            if select_using_name:
                results = db.execute_sync(
                    f"SELECT id FROM theatres WHERE name = 'Theatre #{theatre_num}'"
                )
                if len(results) == 0:
                    # Occasionally, nothing is found.
                    db.commit_sync()
                    return True
                theatre_id = results[0][0]
            else:
                # By design, the theatre number is equal to the ID.
                theatre_id = theatre_num

            # 2. Look for a showing.
            num_to_consider = self.prng.randint(*self.showings_to_consider)
            showing_options = db.execute_sync(
                f"SELECT id, seats_left FROM showings WHERE theatre_id = {theatre_id} "
                f"AND seats_left > 0 ORDER BY date_time ASC LIMIT {num_to_consider}"
            )
            if len(showing_options) == 0:
                # No options. We still consider this as a "success" and return true.
                db.commit_sync()
                return True

            # 3. Choose a showing.
            choice = self.prng.randint(0, len(showing_options) - 1)
            showing = showing_options[choice]
            showing_id = showing[0]
            seats_left = showing[1]

            # 4. Insert the ticket order.
            quantity = min(self.prng.randint(*self.ticket_quantity), seats_left)
            # contact_name = "P{}".format(self.worker_id)
            # loc_x = self.prng.random() * self.loc_max
            # loc_y = self.prng.random() * self.loc_max
            # start_time = time.time()
            # db.execute_sync(
            #     "INSERT INTO ticket_orders (showing_id, quantity, contact_name, location_x, location_y) "
            #     f"VALUES ({showing_id}, {quantity}, '{contact_name}', {loc_x:.4f}, {loc_y:.4f})"
            # )
            # end_time = time.time()
            # print(f"Inserting ticket order took: {end_time-start_time}s")
            # 5. Update the showing's seats left.
            db.execute_sync(
                f"UPDATE showings SET seats_left = {seats_left - quantity} WHERE id = {showing_id}"
            )

            # 6. Commit changes.
            db.commit_sync()
            return True

        except BradClientError:
            db.rollback_sync()
            return False

        except:  # pylint: disable=bare-except
            logger.exception("Need to rollback.")
            db.rollback_sync()
            return False

    def _make_note_edits(self, rows: RowList) -> RowList:
        to_edit: List[Tuple[Any, ...]] = []
        for row in rows:
            if row[1] is not None and row[1].endswith(_EDIT_NOTE_SUFFIX):
                # Bump the number in the suffix.
                suffix_start_idx = row[1].rindex("$")
                edit_num_end_idx = row[1].rindex("[")
                orig = row[1][:suffix_start_idx]
                edit_num_str = row[1][suffix_start_idx + 1 : edit_num_end_idx]
                curr_edit_num = int(edit_num_str)
                if curr_edit_num > 100:
                    diff = self.prng.randint(-1, 1)
                else:
                    diff = 1
                updated = orig + _EDIT_NOTE_FORMAT.format(curr_edit_num + diff)
            else:
                if row[1] is None:
                    updated = _EDIT_NOTE_FORMAT.format(1)
                else:
                    updated = row[1] + _EDIT_NOTE_FORMAT.format(1)
            to_edit.append((row[0], self._quote_escape(updated)))
        return to_edit

    def _quote_escape(self, val: str) -> str:
        # N.B. It's better to rely on the DB connection library to do string
        # escaping. But we might be running against BRAD, which does not provide
        # these utilities baked in.
        return val.replace("'", "''")


_EDIT_NOTE_SUFFIX = "[BRAD]"
_EDIT_NOTE_FORMAT = "${}" + _EDIT_NOTE_SUFFIX
