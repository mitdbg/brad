from .filter import Filter


class AuroraTransactions(Filter):
    """
    Ensures that any tables accessed transactionally are present on Aurora.
    """
