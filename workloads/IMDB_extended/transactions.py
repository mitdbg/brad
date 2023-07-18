import random
from datetime import datetime, timedelta

from brad.grpc_client import RowList


class Context:
    def __init__(self, worker_id: int, seed: int) -> None:
        self.worker_id = worker_id
        self.prng = random.Random(seed)
        self.min_movie_id = 1
        self.max_movie_id = 3870547
        self.min_theatre_id = 1
        self.max_theatre_id = 100000
        self.offset_date = datetime(year=2023, month=7, day=18)


class Database:
    def execute_sync(self, query: str) -> RowList:
        raise NotImplementedError

    def commit_sync(self) -> None:
        raise NotImplementedError

    def rollback_sync(self) -> None:
        raise NotImplementedError


def edit_movie_note(db: Database, ctx: Context) -> bool:
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
    movie_id = ctx.prng.randint(ctx.min_movie_id, ctx.max_movie_id)

    try:
        # Start the transaction.
        db.execute_sync("BEGIN")

        # 2. Select matching movie infos.
        infos = db.execute_sync(
            f"SELECT id, note FROM movie_info WHERE movie_id = {movie_id}"
        )

        # 3. Select matching aka_titles.
        titles = db.execute_sync(
            f"SELECT id, note FROM aka_title WHERE movie_id = {movie_id}"
        )

        # 3. Edit the notes and titles.
        movie_note_edits = _make_note_edits(infos)
        title_note_edits = _make_note_edits(titles)

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

    except:
        db.rollback_sync()
        return False


def add_new_showing(db: Database, ctx: Context) -> bool:
    """
    Represents a theatre employee adding new showings.

    - Select theatre by id
    - Select movie by id
    - Insert into showing
    """
    # 1. Select a random theatre id.
    theatre_id = ctx.prng.randint(ctx.min_theatre_id, ctx.max_theatre_id)

    # 2. Select a random movie id.
    movie_id = ctx.prng.randint(ctx.min_movie_id, ctx.max_movie_id)

    showings_to_add = ctx.prng.randint(1, 3)

    try:
        # Start the transaction.
        db.execute_sync("BEGIN")

        # 3. Verify that the movie actually exists.
        rows = db.execute_sync(f"SELECT id FROM title WHERE id = {movie_id}")
        if len(rows) == 0:
            # We chose an invalid movie. But we still consider this transaction
            # to be a "success" and return true.
            db.commit_sync()
            return True

        # 4. Insert the showing.
        for _ in range(showings_to_add):
            capacity = ctx.prng.randint(200, 400)
            day_offset = ctx.prng.randint(1, 365 * 2)
            hour_offset = ctx.prng.randint(0, 23)
            date_time = ctx.offset_date + timedelta(days=day_offset, hours=hour_offset)
            formatted_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
            db.execute_sync(
                "INSERT INTO showings (theatre_id, movie_id, date_time, total_capacity, seats_left) "
                f"VALUES ({theatre_id}, {movie_id}, {formatted_date_time}, {capacity}, {capacity})"
            )

        db.commit_sync()
        return True

    except:
        db.rollback_sync()
        return False


def purchase_tickets(db: Database, ctx: Context) -> bool:
    """
    Represents a user buying tickets for a specific showing.

    - Select theatre by id
    - Select showing by theatre id and date
    - Insert into `ticket_order`
    - Update the `showing` entry
    """

    # 1. Select a random theatre id.
    theatre_id = ctx.prng.randint(ctx.min_theatre_id, ctx.max_theatre_id)

    try:
        # Start the transaction.
        db.execute_sync("BEGIN")

        # 2. Look for a showing.
        num_to_consider = ctx.prng.randint(1, 5)
        showing_options = db.execute_sync(
            f"SELECT id, seats_left FROM showings WHERE theatre_id = {theatre_id} "
            f"AND seats_left > 0 ORDER BY date_time ASC LIMIT {num_to_consider}"
        )
        if len(showing_options) == 0:
            # No options. We still consider this as a "success" and return true.
            db.execute_sync("COMMIT")
            return True

        # 3. Choose a showing.
        choice = ctx.prng.randint(0, len(showing_options) - 1)
        showing = showing_options[choice]
        showing_id = showing[0]
        seats_left = showing[1]

        # 4. Insert the ticket order.
        quantity = min(ctx.prng.randint(1, 2), seats_left)
        contact_name = "P{}".format(ctx.worker_id)
        loc_x = ctx.prng.random() * 1000
        loc_y = ctx.prng.random() * 1000
        db.execute_sync(
            "INSERT INTO ticket_orders (showing_id, quantity, contact_name, location_x, location_y) "
            f"VALUES ({showing_id}, {quantity}, '{contact_name}', {loc_x:.4f}, {loc_y:.4f})"
        )

        # 5. Update the showing's seats left.
        db.execute_sync(
            f"UPDATE showings SET seats_left = {seats_left - quantity} WHERE id = {showing_id}"
        )

        # 6. Commit changes.
        db.commit_sync()
        return True

    except:
        db.rollback_sync()
        return False


def _make_note_edits(rows: RowList) -> RowList:
    to_edit = []
    for row in rows:
        if row[1].endswith(_EDIT_NOTE_SUFFIX):
            # Bump the number in the suffix.
            suffix_start_idx = row[1].rindex("$")
            edit_num_end_idx = row[1].rindex("[")
            orig = row[1][:suffix_start_idx]
            edit_num_str = row[1][suffix_start_idx + 1 : edit_num_end_idx]
            curr_edit_num = int(edit_num_str)
            to_edit.append((row[0], orig + _EDIT_NOTE_FORMAT.format(curr_edit_num + 1)))
            pass
        else:
            to_edit.append((row[0], row[1] + _EDIT_NOTE_FORMAT.format(1)))
    return to_edit


_EDIT_NOTE_SUFFIX = "[BRAD]"
_EDIT_NOTE_FORMAT = "${}" + _EDIT_NOTE_SUFFIX
