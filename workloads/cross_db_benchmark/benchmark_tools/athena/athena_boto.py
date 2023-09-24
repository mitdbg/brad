import boto3
import time
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class AthenaBotoClient:
    def __init__(
        self, schema_name: str, s3_output_path: str, query_timeout_s: float = 200.0
    ) -> None:
        # By default this will use the AWS secrets that you have configured
        # locally when setting up the AWS CLI.
        self._client = boto3.client("athena")
        self._schema_name = schema_name
        self._s3_output_path = s3_output_path
        self._query_timeout_s = query_timeout_s

    def run_query(self, query: str) -> Dict[str, Any]:
        response = self._client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self._schema_name},
            ResultConfiguration={"OutputLocation": self._s3_output_path},
        )

        # Get query execution ID
        query_execution_id = response["QueryExecutionId"]

        # Get query execution details
        query_execution = self._client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        # Set start time for timeout
        start_time = time.time()

        # Wait for query execution to finish or timeout
        while True:
            status = query_execution["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > self._query_timeout_s:
                # Cancel the query execution if timeout exceeded
                self._client.stop_query_execution(QueryExecutionId=query_execution_id)
                logger.warning("Timeout exceeded. Query cancelled.")
                status = "TIMEOUT"
                break

            # Wait for 1 second before checking the status again
            time.sleep(1)

            # Get updated query execution details
            query_execution = self._client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

        # Get query execution statistics if the query finished successfully
        exec_info = query_execution["QueryExecution"]

        if status == "SUCCEEDED":
            runtime_stats = self._collect_runtime_statistics(
                query_execution_id, max_attempts=5
            )
            return {
                "status": status,
                "exec_info": exec_info,
                "runtime_stats": runtime_stats,
            }
        else:
            return {
                "status": status,
                "exec_info": exec_info,
                "runtime_stats": {},
            }

    def _collect_runtime_statistics(
        self, query_exec_id: str, max_attempts: int
    ) -> Dict[str, Any]:
        attempts = 0

        while True:
            response = self._client.get_query_runtime_statistics(
                QueryExecutionId=query_exec_id
            )
            runtime_stats = response["QueryRuntimeStatistics"]
            attempts += 1

            if "Rows" in runtime_stats:
                return runtime_stats

            if attempts >= max_attempts:
                break

            # Wait before checking the status again. Sometimes the `Rows` metadata
            # is delayed from being propagated.
            time.sleep(0.5)

        # Default value.
        return {}
