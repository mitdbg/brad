import enum


class RedshiftAvailabilityStatus(enum.Enum):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift/client/describe_clusters.html
    Available = "Available"
    Unavailable = "Unavailable"
    Maintenance = "Maintenance"
    Modifying = "Modifying"
    Failed = "Failed"
    Paused = "Paused"

    @staticmethod
    def from_str(candidate: str) -> "RedshiftAvailabilityStatus":
        for status in RedshiftAvailabilityStatus:
            if status.value == candidate:
                return status
        raise ValueError(f"Unrecognized RedshiftAvailabilityStatus: {candidate}")
