from datetime import datetime, timedelta


def period_start(timestamp: datetime, period_length: timedelta) -> datetime:
    """
    Returns the aligned period start timestamp associated with a given
    `timestamp`.
    """

    return (
        timestamp
        - (timestamp - datetime.min.replace(tzinfo=timestamp.tzinfo)) % period_length
    )
