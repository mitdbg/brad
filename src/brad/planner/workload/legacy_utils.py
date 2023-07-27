import boto3
import pathlib
import re
from datetime import timedelta

from .workload import Workload
from .query import Query
from brad.config.file import ConfigFile


def workload_from_extracted_logs(file_path: str, period=timedelta(hours=1)) -> Workload:
    """
    Constructs a workload from extracted query logs. This method does not
    set the dataset size. Useful for testing purposes.
    """
    path = pathlib.Path(file_path)

    txn_queries = []
    analytical_queries = []

    with open(path / "oltp.sql", encoding="UTF-8") as txns:
        for txn in txns:
            if txn.startswith("COMMIT"):
                continue
            txn_queries.append(Query(txn))

    with open(path / "olap.sql", encoding="UTF-8") as analytics:
        for q in analytics:
            analytical_queries.append(Query(q))

    return Workload(period, analytical_queries, txn_queries, {})


def workload_from_s3_logs(
    config: ConfigFile, epochs: int, period=timedelta(hours=1)
) -> Workload:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_access_key_secret,
    )

    # List all objects in the directory
    response = s3.list_objects_v2(
        Bucket=config.s3_logs_bucket, Prefix=config.s3_logs_path
    )

    # Get the last `epochs` epochs by sorting the objects based on their key names
    # TODO: think about scenarios where files haven't been uploaded yet etc. - are the last k on S3 the most recent k?
    sorted_files = sorted(
        response["Contents"], key=lambda obj: obj["Key"], reverse=True
    )[: epochs * 2]

    txn_queries = []
    analytical_queries = []
    sampling_prob = 1

    # Retrieve the contents of each file
    for file_obj in sorted_files:
        file_key = file_obj["Key"]

        response = s3.get_object(Bucket=config.s3_logs_bucket, Key=file_key)
        content = response["Body"].read().decode("utf-8")

        if "analytical" in file_key:
            for line in content.strip().split("\n"):
                q = re.findall(r"Query: (.+?) Engine:", line)[0]
                analytical_queries.append(Query(q))
        elif "transactional" in file_key:
            prob = re.findall(r"_p(\d+)\.log$", file_key)[0]
            if (float(prob) / 100.0) < sampling_prob:
                sampling_prob = prob
            for line in content.strip().split("\n"):
                print(line)
                q = re.findall(r"Query: (.+) Engine:", line)[0]
                txn_queries.append(Query(q))

    # N.B. Sampling probability is currently unused, but is still needed to
    # reweigh the transactions.

    return Workload(period, analytical_queries, txn_queries, {})
