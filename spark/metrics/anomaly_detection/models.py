"""
Shared data models for the anomaly detection system.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────

class AnomalyLevel(str, Enum):
    """The detection layer that found the anomaly."""
    VOLUME       = "volume"
    DISTRIBUTION = "distribution"
    SCHEMA       = "schema"
    PERFORMANCE  = "performance"
    TEMPORAL     = "temporal"
    ML           = "ml"


class Severity(str, Enum):
    """Business-impact severity of an anomaly."""
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


class TrendDirection(str, Enum):
    DEGRADING  = "degrading"
    IMPROVING  = "improving"
    STABLE     = "stable"


# ─────────────────────────────────────────────
# Core anomaly object
# ─────────────────────────────────────────────

class Anomaly(BaseModel):
    """A single detected anomaly event."""

    id: str = Field(description="Unique identifier, e.g. pipeline+level+timestamp")
    pipeline_name: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)

    level: AnomalyLevel
    severity: Severity = Severity.MEDIUM

    # Human-readable description
    description: str
    # Raw metric name that triggered this (e.g. 'row_count', 'duration_seconds')
    metric_name: str
    # Observed vs expected values
    observed_value: Optional[float] = None
    expected_value: Optional[float] = None
    deviation_sigma: Optional[float] = None   # Standard deviations from mean

    # Additional context for LLM (Semaine 10)
    context: Dict[str, Any] = Field(default_factory=dict)

    # Scoring details (set by SeverityScorer)
    severity_score: float = 0.0          # 0–100
    is_recurring: bool = False
    occurrence_count: int = 1

    class Config:
        use_enum_values = True


class DetectionResult(BaseModel):
    """Aggregated result of a full detection run for one pipeline."""

    pipeline_name: str
    run_at: datetime = Field(default_factory=datetime.utcnow)
    anomalies: List[Anomaly] = Field(default_factory=list)

    # Convenience props
    @property
    def has_anomalies(self) -> bool:
        return len(self.anomalies) > 0

    @property
    def worst_severity(self) -> Optional[Severity]:
        if not self.anomalies:
            return None
        order = [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW]
        for s in order:
            if any(a.severity == s for a in self.anomalies):
                return s
        return None

    def summary(self) -> Dict[str, Any]:
        counts = {s.value: 0 for s in Severity}
        for a in self.anomalies:
            counts[a.severity] += 1
        return {
            "pipeline": self.pipeline_name,
            "total_anomalies": len(self.anomalies),
            "worst_severity": self.worst_severity,
            "by_severity": counts,
            "by_level": {
                lvl.value: sum(1 for a in self.anomalies if a.level == lvl)
                for lvl in AnomalyLevel
            },
        }


# ─────────────────────────────────────────────
# Metric snapshot (input to detectors)
# ─────────────────────────────────────────────

class PipelineMetrics(BaseModel):
    """A point-in-time snapshot of pipeline metrics."""

    pipeline_name: str
    measured_at: datetime = Field(default_factory=datetime.utcnow)

    # Volume
    row_count: Optional[int] = None

    # Performance
    duration_seconds: Optional[float] = None
    success: Optional[bool] = None
    task_failures: Optional[int] = None

    # Distribution (per-column stats as nested dict)
    # e.g. {"amount": {"mean": 45.2, "std": 12.1, "null_rate": 0.02, ...}}
    column_stats: Dict[str, Dict[str, float]] = Field(default_factory=dict)

    # Schema (column -> dtype string)
    schema_snapshot: Dict[str, str] = Field(default_factory=dict)

    # Arbitrary extra metrics
    extra: Dict[str, Any] = Field(default_factory=dict)
