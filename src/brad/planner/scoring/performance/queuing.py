import numpy as np


def predict_mm1_wait_time(
    mean_service_time_s: float, utilization: float, quantile: float, eps: float = 1e-3
) -> float:
    """
    Predicts the wait time assuming a M/M/1 system.

      `utilization` should be in [0, 1]
      `quantile` should be in [0, 1] and represents the wait time quantile
        (e.g., 0.9 represents p90 wait time).

    See Equation 4.14: https://www.win.tue.nl/~iadan/queueing.pdf
    """
    # W = -1/mu * 1/(1-rho) * log(1/rho (1 - quantile))
    eps = 1e-3
    util = max(eps, utilization)  # To prevent numeric errors.
    util = min(1.0 - eps, util)
    lf = np.log(1.0 / util * (1.0 - quantile))
    wait_time = mean_service_time_s * (-1.0 / (1.0 - util)) * lf
    return wait_time


def predict_mm1_expected_wait_time(
    mean_service_time_s: float,
    utilization: float,
    alpha: float = 1.0,
    eps: float = 1e-3,
) -> float:
    # W = 1/mu * util / (1 - util)
    denom = max(eps, 1.0 - utilization)  # Want to avoid division by 0.
    wait_sf = utilization / denom
    return mean_service_time_s * wait_sf * alpha
