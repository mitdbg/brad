# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from brad.cost_model.encoder.specific_models.postgres_zero_shot import (
    PostgresZeroShotModel,
)
from brad.cost_model.encoder.specific_models.aurora_zero_shot import AuroraZeroShotModel
from brad.cost_model.encoder.specific_models.redshift_zero_shot import (
    RedshiftZeroShotModel,
)
from brad.cost_model.encoder.specific_models.athena_zero_shot import AthenaZeroShotModel


# dictionary with tailored model for each database system (we learn one model per system that generalizes across
#   databases (i.e., datasets) but on the same database system)
zero_shot_models = {
    DatabaseSystem.POSTGRES: PostgresZeroShotModel,
    DatabaseSystem.AURORA: AuroraZeroShotModel,
    DatabaseSystem.REDSHIFT: RedshiftZeroShotModel,
    DatabaseSystem.ATHENA: AthenaZeroShotModel,
}
