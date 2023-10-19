import enum


class FrontEndMetric(enum.Enum):
    # The number of transactions that have "ended" per second. We use "end" to
    # mean commit or abort. This metric is meant to capture the transactional
    # load imposed by BRAD's clients.
    TxnEndPerSecond = "txn_end_per_s"

    QueryLatencySecondP50 = "query_latency_s_p50"
    QueryLatencySecondP95 = "query_latency_s_p95"

    TxnLatencySecondP50 = "txn_latency_s_p50"
    TxnLatencySecondP95 = "txn_latency_s_p95"
