import pytest

from brad.planner.workload import Workload
from brad.config.file import ConfigFile


def test_read_from_s3():
    config = ConfigFile("./config/config.yml")

    workload = Workload.from_s3_logs(config, 3)

    print(workload.analytical_queries())
    print(workload.transactional_queries())