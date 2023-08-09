import enum


class RdsStatus(enum.Enum):
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html#Overview.DBInstance.Status
    Available = "available"
    BackingUp = "backing-up"
    ConfiguringEnhancedMonitoring = "configuring-enhanced-monitoring"
    ConfiguringIamDatabaseAuth = "configuring-iam-database-auth"
    ConfiguringLogExports = "configuring-log-exports"
    ConvertingToVpc = "converting-to-vpc"
    Creating = "creating"
    DeletePrecheck = "delete-precheck"
    Deleting = "deleting"
    Failed = "failed"
    InaccessibleEncryptionCredentials = "inaccessible-encryption-credentials"
    InaccessibleEncryptionCredentialsRecoverable = (
        "inaccessible-encryption-credentials-recoverable"
    )
    IncompatibleNetwork = "incompatible-network"
    IncompatibleOptionGroup = "incompatible-option-group"
    IncompatibleParameters = "incompatible-parameters"
    IncompatibleRestore = "incompatible-restore"
    InsufficientCapacity = "insufficient-capacity"
    Maintenance = "maintenance"
    Modifying = "modifying"
    MovingToVpc = "moving-to-vpc"
    Rebooting = "rebooting"
    ResettingMasterCredentials = "resetting-master-credentials"
    Renaming = "renaming"
    RestoreError = "restore-error"
    Starting = "starting"
    Stopped = "stopped"
    Stopping = "stopping"
    StorageFull = "storage-full"
    StorageOptimization = "storage-optimization"
    Upgrading = "upgrading"

    @staticmethod
    def from_str(candidate: str) -> "RdsStatus":
        for status in RdsStatus:
            if status.value == candidate:
                return status
        raise ValueError(f"Unrecognized RDS status: {candidate}")
