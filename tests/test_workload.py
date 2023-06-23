from brad.planner.workload.utils import workload_from_s3_logs
from brad.config.file import ConfigFile


def test_read_from_s3():
    config = ConfigFile("./config/config.yml")

    workload = workload_from_s3_logs(config, 3)

    print(workload.analytical_queries())
    print(workload.transactional_queries())
