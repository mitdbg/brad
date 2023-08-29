import enum


class FrontEndMetric(enum.Enum):
    # The number of transactions that have "ended" per second. We use "end" to
    # mean commit or abort. This metric is meant to capture the transactional
    # load imposed by BRAD's clients.
    TxnEndPerSecond = "txn_end_per_s"

    # The following two metrics are used to compute average query latencies. Sum
    # each across all front ends and then divide to get the average.
    QueryLatencySumSecond = "query_latency_sum_s"
    NumQueries = "num_queries"

    # The highest recorded query latency.
    QueryLatencyMaxSecond = "query_latency_max_s"
