import csv
import datetime

from brad.blueprint import Blueprint
from brad.planner.scoring.score import Score


class BlueprintPlanningLogger:
    """
    Used for debugging purposes.
    """

    def __init__(self) -> None:
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file_name = f"brad_planning_{curr_time}.csv"
        self._out_file = open(out_file_name, "w", encoding="UTF-8")
        self._first_log = True

    def __del__(self) -> None:
        self._out_file.close()

    def log_blueprint_and_score(self, bp: Blueprint, score: Score) -> None:
        writer = csv.writer(self._out_file)

        bp_dict = bp.as_dict()
        perf_dict = score.perf_metrics()
        score_dict = score.debug_components()

        bp_keys = bp_dict.keys()
        perf_keys = perf_dict.keys()
        score_keys = score_dict.keys()

        if self._first_log:
            all_keys = (
                list(bp_dict.keys()) + list(perf_dict.keys()) + list(score_dict.keys())
            )
            writer.writerow(all_keys)

        values = []
        for k in bp_keys:
            values.append(bp_dict[k])
        for k in perf_keys:
            values.append(perf_dict[k])
        for k in score_keys:
            values.append(score_dict[k])

        writer.writerow(values)
        self._out_file.flush()

        self._first_log = False
