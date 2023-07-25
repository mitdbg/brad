import enum


class FrontEndMetric(enum.Enum):
    # The number of transactions that have "ended" per second. We use "end" to
    # mean commit or abort. This metric is meant to capture the transactional
    # load imposed by BRAD's clients.
    TxnEndPerSecond = "txn_end_per_s"
