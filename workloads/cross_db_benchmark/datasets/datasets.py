from dataclasses import dataclass


@dataclass()
class SourceDataset:
    name: str
    osf: bool = True


@dataclass()
class Database:
    db_name: str
    _source_dataset: str = None
    _data_folder: str = None
    max_no_joins: int = 4
    min_no_joins: int = 1
    max_no_predicates: int = 4
    min_no_predicates: int = 1
    scale: int = 1
    duplicate_tables: list = None
    duplicate_no: int = 1
    contain_unicode: bool = False
    full_outer_join: bool = False

    @property
    def source_dataset(self) -> str:
        if self._source_dataset is None:
            return self.db_name
        return self._source_dataset

    @property
    def data_folder(self) -> str:
        if self._data_folder is None:
            return self.db_name
        return self._data_folder


# datasets that can be downloaded from osf and should be unzipped
source_dataset_list = [
    # original datasets
    SourceDataset("airline"),
    SourceDataset("imdb"),
    SourceDataset("ssb"),
    SourceDataset("tpc_h"),
    SourceDataset("walmart"),
    SourceDataset("financial"),
    SourceDataset("basketball"),
    SourceDataset("accidents"),
    SourceDataset("movielens"),
    SourceDataset("baseball"),
    SourceDataset("hepatitis"),
    SourceDataset("tournament"),
    SourceDataset("genome"),
    SourceDataset("credit"),
    SourceDataset("employee"),
    SourceDataset("carcinogenesis"),
    SourceDataset("consumer"),
    SourceDataset("geneea"),
    SourceDataset("seznam"),
    SourceDataset("stats"),
    SourceDataset("fhnk"),
]

# Each dataset is generating workload with diverse pattern (e.g. different no of joins/predicates)
# This is to make sure the overall transqo training data is diverse
database_list = [
    # unscaled
    Database(
        "airline",
        max_no_joins=6,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=0,
    ),
    Database(
        "imdb",
        _data_folder="imdb_new",
        max_no_joins=10,
        min_no_joins=4,
        max_no_predicates=6,
        min_no_predicates=2,
    ),
    Database(
        "ssb",
        _data_folder="ssb",
        max_no_joins=4,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=1,
    ),
    Database(
        "tpc_h",
        max_no_joins=6,
        min_no_joins=2,
        max_no_predicates=6,
        min_no_predicates=2,
    ),
    Database("walmart", max_no_joins=2),
    # scaled batch 1
    Database(
        "financial",
        _data_folder="scaled_financial",
        scale=6,
        max_no_joins=6,
        min_no_joins=2,
        max_no_predicates=3,
        min_no_predicates=0,
    ),
    Database(
        "basketball",
        _data_folder="scaled_basketball",
        scale=200,
        max_no_joins=7,
        min_no_joins=2,
        max_no_predicates=8,
        min_no_predicates=2,
    ),
    Database(
        "accidents",
        _data_folder="scaled_accidents",
        scale=2,
        contain_unicode=True,
        duplicate_tables=["nesreca", "oseba"],
        max_no_joins=4,
        min_no_joins=1,
        max_no_predicates=6,
        min_no_predicates=1,
    ),
    Database(
        "movielens",
        _data_folder="scaled_movielens",
        scale=8,
        max_no_joins=5,
        min_no_joins=2,
        max_no_predicates=6,
        min_no_predicates=2,
    ),
    Database(
        "baseball",
        _data_folder="scaled_baseball",
        scale=10,
        max_no_joins=10,
        min_no_joins=2,
        max_no_predicates=8,
        min_no_predicates=2,
    ),
    # scaled batch 2
    Database("hepatitis", _data_folder="scaled_hepatitis", scale=2000),
    Database(
        "tournament",
        _data_folder="scaled_tournament",
        scale=80,
        max_no_joins=7,
        min_no_joins=2,
        max_no_predicates=8,
        min_no_predicates=3,
    ),
    Database(
        "credit",
        _data_folder="scaled_credit",
        scale=6,
        max_no_joins=6,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=1,
    ),
    Database(
        "employee",
        _data_folder="scaled_employee",
        scale=8,
        max_no_joins=4,
        min_no_joins=2,
        max_no_predicates=2,
        min_no_predicates=0,
    ),
    Database("consumer", _data_folder="scaled_consumer", scale=6),
    Database(
        "geneea",
        _data_folder="scaled_geneea",
        scale=40,
        contain_unicode=True,
        max_no_joins=8,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=2,
    ),
    Database(
        "genome",
        _data_folder="scaled_genome",
        scale=8,
        max_no_joins=4,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=0,
    ),
    Database(
        "carcinogenesis",
        _data_folder="scaled_carcinogenesis",
        scale=674,
        max_no_joins=4,
        min_no_joins=2,
        max_no_predicates=4,
        min_no_predicates=0,
    ),
    Database("seznam", _data_folder="scaled_seznam", scale=2),
    Database(
        "stats",
        max_no_joins=6,
        min_no_joins=2,
        max_no_predicates=6,
        min_no_predicates=1,
    ),
    Database("fhnk", _data_folder="scaled_fhnk", scale=2),
]

database_dict = {db.db_name: db for db in database_list}

ext_database_list = database_list + [Database("imdb_full", _data_folder="imdb")]

selected_db_names = [
    "airline",
    "baseball",
    "carcinogenesis",
    "credit",
    "employee",
    "financial",
    "geneea",
    "imdb",
    "movielens",
    "ssb",
    "tournament",
]
