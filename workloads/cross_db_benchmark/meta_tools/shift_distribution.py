import numpy as np
import matplotlib.pyplot as plt


def shift_frequency_to_match_hist(runtime: np.array, target_hist: (np.array, np.array), balanced: bool) -> np.array:
    """
    change the frequency of execution for a list of queries to match the distribution of a target histogram
    runtime: a numpy array of shape (n, 3) corresponding to the athena-aurora-redshift runtime of n queries
    target_hist: a tuple of (bin_count, bin_boundary) for target histogram
    balanced: whether to balance query from the best engine.
    """
    n_queries = runtime.shape[0]
    target_count, bins = target_hist
    target_freq = target_count / np.sum(target_count)

    # approximate brad runtime as the average of two lower runtime
    est_brad_rt = (np.sum(runtime, axis=1) - np.max(runtime, axis=1)) / 2
    count, _, _ = plt.hist(est_brad_rt, bins=bins)
    freq = count / len(est_brad_rt)
    return_freq = np.zeros(n_queries)
    frac_best = np.zeros(3)

    for i, f in enumerate(freq):
        if f == 0:
            continue
        else:
            idx = np.where((est_brad_rt > bins[i]) & (est_brad_rt <= bins[i + 1]))[0]
            assert np.isclose(len(idx), f * n_queries)
            if len(idx) == 1:
                return_freq[idx[0]] = target_freq[i]
                best_engine = np.argmin(runtime[idx[0], :])
                if balanced:
                    frac_best[best_engine] += target_freq[i]

    for i, f in enumerate(freq):
        if f == 0:
            continue
        else:
            idx = np.where((est_brad_rt > bins[i]) & (est_brad_rt <= bins[i + 1]))[0]
            assert np.isclose(len(idx), f * n_queries)
            if len(idx) > 1:
                if balanced:
                    # assign with inverse probability to the frac_best
                    best_engines = np.argmin(runtime[idx, :], axis=1)
                    has_best_engines = np.isin(np.arange(3), best_engines)
                    # if all queries in a particular bin have unique best engine, we tune down its frequency
                    if np.sum(has_best_engines) == 1:
                        best_engine = np.where(has_best_engines)[0][0]
                        return_freq[idx] = target_freq[i] * 0.7 / len(idx)
                        frac_best[best_engine] += target_freq[i] * 0.7
                    else:
                        inverse_frac_best = 1 / np.maximum(frac_best, 1e-5)
                        inverse_frac_best[~has_best_engines] = 0
                        inverse_frac_best[has_best_engines] = inverse_frac_best[has_best_engines] / np.sum(
                            inverse_frac_best[has_best_engines])
                        for e in range(3):
                            if has_best_engines[e]:
                                e_best_idx = idx[np.where(best_engines == e)[0]]
                                return_freq[e_best_idx] = target_freq[i] * inverse_frac_best[e] / len(e_best_idx)
                                frac_best[e] += target_freq[i] * inverse_frac_best[e]
                else:
                    return_freq[idx] = target_freq[i] / len(idx)

    # the distribution is highly skewed so we assign the unmatched frequency to the engine with least frequency
    unassigned_freq = 1 - np.sum(return_freq)
    le = np.argmin(frac_best)
    best_engines = np.argmin(runtime, axis=1)
    return_freq[np.where(best_engines == le)[0]] += unassigned_freq / np.sum(best_engines == le)
    return return_freq

