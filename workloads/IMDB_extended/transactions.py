import random

from brad.grpc_client import RowList


class Context:
    def __init__(self, seed: int) -> None:
        self._prng = random.Random(seed)
        self._min_movie_id = 1
        self._max_movie_id = 3870547


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
    movie_id = ctx._prng.randint(ctx._min_movie_id, ctx._max_movie_id)

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
