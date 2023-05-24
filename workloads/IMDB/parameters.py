import re
from typing import Any, Optional


class _Param:
    """Stores data for a single parameter."""

    def __init__(self, name: str, default_value: Any, description: str):
        self._name = name
        self._value = default_value
        self._description = description

    def Get(self) -> Any:
        return self._value

    def Set(self, value: Any) -> None:
        self._value = value


class Params:
    """Stores data for a set of parameters.

    Provides attribute-based API, e.g. "params.foo = 5".
    Uses internal {'name': _Param} dict for storing parameter data.
    """

    def __init__(self):
        self.__dict__["_immutable"] = False
        self._params = {}  # name => _Param

    def __setattr__(self, name: str, value: Any) -> None:
        if self._immutable:
            raise TypeError("This Params instance is immutable.")
        if name == "_params" or name == "_immutable":
            self.__dict__[name] = value
        else:
            try:
                self._params[name].Set(value)
            except KeyError:
                raise AttributeError(self._KeyErrorString(name))

    def __getattr__(self, name: str) -> Optional[Any]:
        if name == "_params" or name == "_immutable":
            return self.__dict__[name]
        try:
            return self._params[name].Get()
        except KeyError:
            # cPickle expects __getattr__ to raise AttributeError, not KeyError.
            raise AttributeError(self._KeyErrorString(name))

    def __dir__(self) -> list:
        return sorted(self._params.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._params

    def __len__(self) -> int:
        return len(self._params)

    # Note: This gets called by _Param.__eq__() on nested Params objects.
    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Params) and self._params == other._params
        )  # pylint: disable=protected-access

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __str__(self):
        return self._ToString(0)

    def _ToString(self, nested_depth: int) -> str:
        # Note: We use iteritems() below so as to sort by name.
        sorted_param_strs = [
            v.ToString(nested_depth + 1) for (_, v) in sorted(self._params.items())
        ]
        nested_indent = "  " * nested_depth
        return "{\n%s\n%s}" % ("\n".join(sorted_param_strs), nested_indent)

    def __deepcopy__(self, unused_memo: Any):
        return self.Copy()

    def Define(self, name: str, default_value: Any, description: str) -> None:
        """Defines a parameter.

        Args:
          name: The parameter name. Must only contain lowercase letters, numbers,
              and underscores. Must start with lowercase letter.
          default_value: Default value for this parameter. May be None.
          description: String description of this parameter.

        Raises:
          AttributeError: If parameter 'name' is already defined.
        """
        if self._immutable:
            raise TypeError("This Params instance is immutable.")
        assert (
            name is not None
            and isinstance(name, str)
            and (re.match("^[a-z][a-z0-9_]*$", name) is not None)
        )
        if name in self._params:
            raise AttributeError("Parameter %s is already defined" % name)
        self._params[name] = _Param(name, default_value, description)

    def Freeze(self) -> None:
        """Marks this Params as immutable."""
        self._immutable = True


class _RegistryHelper(object):
    # Copyright 2022 The Balsa Authors.
    """Helper class."""
    # Global dictionary mapping subclass name to registered params.
    _PARAMS = {}

    @classmethod
    def Register(cls, real_cls):
        k = real_cls.__name__
        assert k not in cls._PARAMS, "{} already registered!".format(k)
        cls._PARAMS[k] = real_cls
        return real_cls


Register = _RegistryHelper.Register


def Get(name: str) -> Params:
    if name not in _RegistryHelper._PARAMS:
        raise LookupError(
            "{} not found in registered params: {}".format(
                name, list(sorted(_RegistryHelper._PARAMS.keys()))
            )
        )
    p = _RegistryHelper._PARAMS[name]().Params()
    return p


def GetAll():
    return dict(_RegistryHelper._PARAMS)


class WorkloadParams(object):
    @classmethod
    def Params(cls) -> Params:
        p = Params()
        p.Define(
            "num_days",
            1,
            "number of days for simulation, the simulation pattern for these days will be the same."
            "if you would like different pattern for each day, run simulation multiple times.",
        )
        p.Define("schema_file", None, "location to the DB schema file.")
        p.Define("simulate_oltp", True, "simulate OLTP queries?")
        p.Define("simulate_olap", True, "simulate OLAP queries?")

        # Parameters for transactional workload
        p.Define("txn_query_dir", None, "directory for transaction queries.")
        p.Define(
            "total_num_txn_users",
            None,
            "total number of users who writes transaction queries.",
        )
        p.Define(
            "num_txn_queries_per_user",
            None,
            "number of transaction queries each user write per day.",
        )
        p.Define(
            "num_txn_queries_dist",
            None,
            "a list of length 24, representing number of txn queries for each hour.",
        )
        p.Define(
            "num_tuples_insert_per_user",
            None,
            "the average number of tuples to insert for each txn query.",
        )
        p.Define(
            "match_new_pk",
            False,
            "do we want the newly-inserted tuple's foreign key to match the old primary key"
            "or the newly-inserted primary key?",
        )
        p.Define(
            "table_insert_freq",
            None,
            "a dictionary indicating the insertion frequency for each table.",
        )
        p.Define(
            "txn_generation_ids_offset_file",
            None,
            "a log file for generating txn queries",
        )

        # Parameters for analytical workload
        p.Define("analytic_query_dir", None, "directory for transaction queries.")
        p.Define(
            "reporting_query_rt_interval",
            None,
            "a dictionary indicating how to select the reporting queries.",
        )
        p.Define(
            "reporting_time_window",
            None,
            "during which time of a day to run reporting queries.",
        )
        p.Define(
            "total_num_analytic_users",
            None,
            "total number of users who writes analytical queries.",
        )
        p.Define(
            "num_analytic_queries_per_user",
            None,
            "number of analytic queries each user write per day.",
        )
        p.Define(
            "num_analytic_queries_dist",
            None,
            "a list of length 24, representing number of analytic queries "
            "for each hour.",
        )
        p.Define(
            "analytic_query_rt_interval",
            None,
            "the runtime interval of ad-hoc analytical queries",
        )
        p.Define("aurora_timeout", False, "include timeout queries on Aurora?")
        p.Define("redshift_timeout", False, "include timeout queries on Redshift?")

        p.Define("force", False, "remove the existing query folder if exist")

        # Parameters for executing the workload
        p.Define("test_run", False, "do a quick test run?")
        return p


@Register
class Default(WorkloadParams):
    def Params(self) -> Params:
        p = super().Params()
        p.num_days = 1
        p.schema_file = "workloads/IMDB/schema.sql"
        p.simulate_olap = True
        p.simulate_oltp = True

        p.txn_query_dir = "workloads/IMDB/OLTP_queries"
        p.analytic_query_dir = "workloads/IMDB/OLAP_queries"
        p.total_num_txn_users = 10
        p.num_txn_queries_per_user = 1000
        p.num_txn_queries_dist = [
            0.003,
            0.002,
            0.001,
            0.001,
            0.002,
            0.004,
            0.005,
            0.01,
            0.05,
            0.1,
            0.2,
            0.2,
            0.1,
            0.2,
            0.2,
            0.2,
            0.2,
            0.1,
            0.05,
            0.01,
            0.01,
            0.003,
            0.003,
            0.003,
            0.003,
        ]
        p.num_tuples_insert_per_user = 500
        p.match_new_pk = False
        p.table_insert_freq = {
            "title": 0.1,
            "char_name": 0.1,
            "name": 0.1,
            "aka_name": 0.05,
            "company_type": 0,
            "keyword": 0.1,
            "movie_keyword": 0.1,
            "movie_companies": 0.1,
            "kind_type": 0,
            "person_info": 0.1,
            "cast_info": 1,
            "movie_info_idx": 0.05,
            "company_name": 0.01,
            "movie_info": 0.5,
            "info_type": 0,
            "aka_title": 0.01,
            "comp_cast_type": 0,
            "complete_cast": 0.01,
            "link_type": 0,
            "movie_link": 0.01,
            "role_type": 0,
        }
        p.txn_generation_ids_offset_file = "workloads/IMDB/OLTP_queries/log.json"
        p.reporting_query_rt_interval = {0.5: 0, 1: 50, 10: 100, 50: 20, 200: 10}
        p.reporting_time_window = ("00:00:00", "06:00:00")
        p.total_num_analytic_users = 10
        p.num_analytic_queries_per_user = 100
        p.num_analytic_queries_dist = [
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.05,
            0.1,
            0.2,
            0.2,
            0.1,
            0.2,
            0.2,
            0.2,
            0.2,
            0.1,
            0.05,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
            0.01,
        ]
        p.analytic_query_rt_interval = (1, 100)
        p.aurora_timeout = False
        p.redshift_timeout = False
        p.force = False

        # Workload execution parameters
        p.test_run = True
        return p
