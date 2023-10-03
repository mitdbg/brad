import pathlib
import numpy as np
import numpy.typing as npt

from typing import List, Tuple
from brad.config.engine import Engine


EI = {
    Engine.Athena: 0,
    Engine.Aurora: 1,
    Engine.Redshift: 2,
}


class QueryDataset:
    @classmethod
    def load(cls, data_dir: str) -> "QueryDataset":
        data_dir_path = pathlib.Path(data_dir)
        with open(data_dir_path / "queries.sql", encoding="UTF-8") as f:
            queries = [line.strip() for line in f]

        run_times = np.load(data_dir_path / "run_time_s-athena-aurora-redshift.npy")
        data_accessed = np.load(data_dir_path / "data_accessed-athena-aurora.npy")

        return cls(queries, run_times, data_accessed)

    def __init__(
        self, queries: List[str], run_times: npt.NDArray, data_accessed: npt.NDArray
    ) -> None:
        self.queries = queries
        self.run_times = run_times
        self.data_accessed = data_accessed

    def serialize(self, output_path: pathlib.Path) -> None:
        with open(output_path / "queries.sql", "w", encoding="UTF-8") as file:
            for q in self.queries:
                print(q, file=file)

        np.save(output_path / "run_time_s-athena-aurora-redshift.npy", self.run_times)
        np.save(output_path / "data_accessed-athena-aurora.npy", self.data_accessed)

    def compute_dataset_dist(self, thres: float = 1.5) -> None:
        data = self.run_times.copy()
        data[np.isnan(data)] = 10000.0
        total, _ = data.shape
        aurora_best = (
            (data[:, EI[Engine.Aurora]] * thres < data[:, EI[Engine.Redshift]])
            & (data[:, EI[Engine.Aurora]] * thres < data[:, EI[Engine.Athena]])
        ).sum()
        athena_best = (
            (data[:, EI[Engine.Athena]] * thres < data[:, EI[Engine.Redshift]])
            & (data[:, EI[Engine.Athena]] * thres < data[:, EI[Engine.Aurora]])
        ).sum()
        redshift_best = (
            (data[:, EI[Engine.Redshift]] * thres < data[:, EI[Engine.Aurora]])
            & (data[:, EI[Engine.Redshift]] * thres < data[:, EI[Engine.Athena]])
        ).sum()
        too_close = total - aurora_best - athena_best - redshift_best

        # Run times summary
        def summarize_run_times(engine):
            data = self.run_times.copy()
            rel = data[:, EI[engine]]
            valid = rel[~np.isnan(rel)]
            print(f"{engine} run times:")
            print("Min:", np.min(valid))
            print("Max:", np.max(valid))
            print("Mean:", np.mean(valid))
            print("Std:", np.std(valid))

        print("Total:", total)
        print("Aurora best:", aurora_best)
        print("Athena best:", athena_best)
        print("Redshift best:", redshift_best)
        print("Too close:", too_close)

        print()
        print("Aurora best frac: {:.4f}".format(aurora_best / total))
        print("Athena best frac: {:.4f}".format(athena_best / total))
        print("Redshift best frac: {:.4f}".format(redshift_best / total))
        print("Too close frac: {:.4f}".format(too_close / total))

        print()
        summarize_run_times(Engine.Athena)
        print()
        summarize_run_times(Engine.Aurora)
        print()
        summarize_run_times(Engine.Redshift)

    def compute_cost(self, query_index: int, engine: Engine) -> float:
        data_stats = self.run_times[query_index]
        if engine == Engine.Athena:
            cost_per_mb = 0.000005
            min_mb = 10
            bytes_accessed = data_stats[EI[Engine.Athena]]
            mb_accessed = bytes_accessed / 1000 / 1000
            return (
                mb_accessed * cost_per_mb
                if mb_accessed > min_mb
                else cost_per_mb * min_mb
            )
        elif engine == Engine.Aurora:
            blocks_accessed = data_stats[EI[Engine.Aurora]]
            cost_per_million = 0.20
            mil_blocks_accessed = blocks_accessed / 1_000_000
            return mil_blocks_accessed * cost_per_million
        else:
            assert False

    def get_best_indices(
        self, engine: Engine, thres: float = 1.5
    ) -> Tuple[npt.NDArray, npt.NDArray]:
        all_engines = [Engine.Redshift, Engine.Aurora, Engine.Athena]
        all_engines.remove(engine)
        data = self.run_times.copy()
        timeout_placeholder = 1000000.0
        data[np.isnan(data)] = timeout_placeholder
        mask1 = data[:, EI[engine]] * thres < data[:, EI[all_engines[0]]]
        mask2 = data[:, EI[engine]] * thres < data[:, EI[all_engines[1]]]
        mask3 = np.any(data < timeout_placeholder, axis=1)
        # Exclude queries where all engines timeout.
        comb = mask1 & mask2 & mask3
        return comb, np.where(comb)[0]

    def get_sql(self, query_index: int) -> str:
        return self.queries[query_index]

    def get_run_times(self, query_index: int) -> npt.NDArray:
        return self.run_times[query_index]

    def get_sql_multi(self, indices: npt.NDArray) -> List[str]:
        to_return = []
        for idx, q in enumerate(self.queries):
            if idx in indices:
                to_return.append(q)
        return to_return

    def split(
        self, test_indices: npt.NDArray, overall_to_exclude: npt.NDArray
    ) -> Tuple["QueryDataset", "QueryDataset"]:
        matching_queries = []
        nonmatching_queries = []

        matching_run_times = []
        nonmatching_run_times = []

        matching_data = []
        nonmatching_data = []

        # Sometimes we want to exclude queries that are not in the test set.
        to_exclude = np.unique(np.concatenate([test_indices, overall_to_exclude]))

        for idx in test_indices:
            matching_queries.append(self.queries[idx])
            matching_run_times.append(self.run_times[idx])
            matching_data.append(self.data_accessed[idx])

        for idx in range(len(self.queries)):
            if idx in to_exclude:
                continue
            nonmatching_queries.append(self.queries[idx])
            nonmatching_run_times.append(self.run_times[idx])
            nonmatching_data.append(self.data_accessed[idx])

        return (
            QueryDataset(
                matching_queries,
                np.stack(matching_run_times, axis=0),
                np.stack(matching_data, axis=0),
            ),
            QueryDataset(
                nonmatching_queries,
                np.stack(nonmatching_run_times, axis=0),
                np.stack(nonmatching_data, axis=0),
            ),
        )

    def valid_mask(self) -> npt.NDArray:
        # Returns a mask of queries that are valid. Validity conditions:
        # - At least one engine did not time out
        # - Data stats are available for both Athena and Aurora
        data = self.run_times.copy()
        timeout_placeholder = 100000000.0
        data[np.isnan(data)] = timeout_placeholder
        rt_valid = np.any(data < timeout_placeholder, axis=1)
        data_valid = np.all(self.data_accessed > 0, axis=1)
        return rt_valid & data_valid


def select_queries_from_mask(
    mask: npt.NDArray, num_to_select: int, seed: int = 42
) -> npt.NDArray:
    np.random.seed(seed)
    indices = np.where(mask)[0]
    np.random.shuffle(indices)
    return indices[:num_to_select]


if __name__ == "__main__":
    # This script is used for selecting test queries.
    # Modify the paths below as needed.
    reg_20g_full = QueryDataset.load("IMDB_20GB/regular_rebalanced_5k")
    reg_100g = QueryDataset.load("IMDB_100GB/regular_rebalanced_2k/")

    reg_100g_valid = reg_100g.valid_mask()
    athena_best_100g_mask, athena_best_100g = reg_100g.get_best_indices(Engine.Athena)
    aurora_best_100g_mask, aurora_best_100g = reg_100g.get_best_indices(Engine.Aurora)
    redshift_best_100g_mask, redshift_best_100g = reg_100g.get_best_indices(
        Engine.Redshift
    )
    close_100g_mask = (
        (~athena_best_100g_mask)
        & (~aurora_best_100g_mask)
        & (~redshift_best_100g_mask)
        & reg_100g_valid
    )

    reg_20g_valid = reg_20g_full.valid_mask()
    athena_best_20g_mask, athena_best_20g = reg_20g_full.get_best_indices(Engine.Athena)
    aurora_best_20g_mask, aurora_best_20g = reg_20g_full.get_best_indices(Engine.Aurora)
    redshift_best_20g_mask, redshift_best_20g = reg_20g_full.get_best_indices(
        Engine.Redshift
    )
    close_20g_mask = (
        (~athena_best_20g_mask)
        & (~aurora_best_20g_mask)
        & (~redshift_best_20g_mask)
        & reg_20g_valid
    )

    athena_test_queries_100 = select_queries_from_mask(
        (athena_best_100g_mask & reg_100g_valid), num_to_select=25, seed=42
    )
    aurora_test_queries_100 = select_queries_from_mask(
        (aurora_best_100g_mask & reg_100g_valid), num_to_select=25, seed=42 ^ 1
    )
    redshift_test_queries_100 = select_queries_from_mask(
        (redshift_best_100g_mask & reg_100g_valid), num_to_select=25, seed=42 ^ 2
    )
    close_test_queries_100 = select_queries_from_mask(
        (close_100g_mask & reg_100g_valid), num_to_select=25, seed=42 ^ 3
    )

    athena_test_queries_20 = select_queries_from_mask(
        (athena_best_20g_mask[:2000] & reg_20g_valid[:2000]),
        num_to_select=25,
        seed=42 ^ 4,
    )
    aurora_test_queries_20 = select_queries_from_mask(
        (aurora_best_20g_mask[:2000] & reg_20g_valid[:2000]),
        num_to_select=25,
        seed=42 ^ 5,
    )
    redshift_test_queries_20 = select_queries_from_mask(
        (redshift_best_20g_mask[:2000] & reg_20g_valid[:2000]),
        num_to_select=25,
        seed=42 ^ 6,
    )
    close_test_queries_20 = select_queries_from_mask(
        (close_20g_mask[:2000] & reg_20g_valid[:2000]), num_to_select=25, seed=42 ^ 7
    )

    test_100g = np.concatenate(
        [
            athena_test_queries_100,
            aurora_test_queries_100,
            redshift_test_queries_100,
            close_test_queries_100,
        ]
    )
    test_20g = np.concatenate(
        [
            athena_test_queries_20,
            aurora_test_queries_20,
            redshift_test_queries_20,
            close_test_queries_20,
        ]
    )

    reg_100g_test, reg_100g_train = reg_100g.split(
        test_100g, np.concatenate([test_100g, test_20g])
    )
    reg_20g_test, reg_20g_train = reg_20g_full.split(
        test_20g, np.concatenate([test_100g, test_20g])
    )

    # Serialize the split datasets. Modify as needed
    # reg_100g_train.serialize(pathlib.Path("IMDB_100GB/regular_train"))
    # reg_100g_test.serialize(pathlib.Path("IMDB_100GB/regular_test"))
    # reg_20g_train.serialize(pathlib.Path("IMDB_20GB/regular_train"))
    # reg_20g_test.serialize(pathlib.Path("IMDB_20GB/regular_test"))
