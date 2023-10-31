import math
import pickle
import numpy as np
from typing import Optional, List, Mapping
import numpy.typing as npt


def gen_num_client_dist(
    time_interval: int = 60,
    max_num_client: int = 10,
    hourly_distribution: Optional[List[float]] = None,
    seed: int = 0,
    sigma: float = 0.0,
    save_path: Optional[str] = None,
) -> Mapping[int, int]:
    """generate the number of client for different time of the day
    time_interval: the granularity we want to divide a whole day by (unit minute,
                   will be good to make it a number divisible by 60)
    max_num_client: maximum number of client
    hourly_distribution: a list of 24 integer for the average number of clients within an hour
    sigma: the random noise added to the generation process (with random seed)
    """
    np.random.seed(seed)
    if hourly_distribution is None:
        hourly_distribution = [
            0.1,
            0.05,
            0.05,
            0.05,
            0.05,
            0.05,
            0.05,
            0.1,
            0.2,
            0.4,
            0.6,
            0.8,
            0.8,
            0.9,
            1.0,
            1.0,
            0.9,
            0.7,
            0.4,
            0.3,
            0.2,
            0.2,
            0.1,
            0.1,
        ]
    assert (
        len(hourly_distribution) == 24
    ), "invalid length for hourly_distribution, must be 24"

    num_client_by_time = dict()
    for h in range(24):
        avg_num_client_h = hourly_distribution[h] * max_num_client
        for m in range(0, 60, time_interval):
            num_client_m = np.random.normal(avg_num_client_h, sigma)
            num_client_m = min(math.ceil(num_client_m), max_num_client)
            time_in_s = (h * 60 + m) * 60
            num_client_by_time[time_in_s] = num_client_m

    if save_path is not None:
        with open(save_path, "wb") as f:
            pickle.dump(num_client_by_time, f)
    return num_client_by_time


def gen_gap_time_dist(
    time_interval: int = 60,
    avg_gap_in_s: int = 4,
    hourly_distribution: Optional[List[float]] = None,
    seed: int = 0,
    sigma: float = 0.5,
    save_path: Optional[str] = None,
) -> npt.NDArray:
    """generate the execution time gap for different time of the day
    time_interval: the granularity we want to divide a whole day by (unit minute,
                   will be good to make it a number divisible by 60)
    avg_wait_time_in_s: average wait gap in second
    hourly_distribution: a list of 24 integer for the average number of clients within an hour
    sigma: the random noise added to the generation process (with random seed)
    """
    np.random.seed(seed)
    if hourly_distribution is None:
        hourly_distribution = [
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            0.7,
            0.6,
            0.5,
            0.5,
            0.5,
            0.3,
            0.3,
            0.3,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
            1.0,
            1.0,
        ]
    assert (
        len(hourly_distribution) == 24
    ), "invalid length for hourly_distribution, must be 24"

    gap_time_dist = []
    for h in range(24):
        avg_gap_time_h = hourly_distribution[h] * avg_gap_in_s
        sigma_gap_time_h = hourly_distribution[h] * sigma
        for m in range(0, 60, time_interval):
            gap_time_m = np.random.normal(avg_gap_time_h, sigma_gap_time_h)
            gap_time_dist.append(np.abs(gap_time_m))
    gap_time_dist = np.asarray(gap_time_dist)
    if save_path is not None:
        np.save(save_path, gap_time_dist)
    return gap_time_dist
