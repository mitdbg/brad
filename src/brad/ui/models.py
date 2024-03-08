from typing import List, Dict
from pydantic import BaseModel, AwareDatetime


class TimestampedMetrics(BaseModel):
    timestamps: List[AwareDatetime]
    values: List[float]


class MetricsData(BaseModel):
    named_metrics: Dict[str, TimestampedMetrics]
