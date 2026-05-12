"""
Anomaly detection package.

Public surface:
    from anomaly_detection import AnomalyDetector
    from anomaly_detection.models import PipelineMetrics, DetectionResult, Anomaly
    from anomaly_detection.detectors import VolumeDetector, DistributionDetector, SchemaDetector, PerformanceDetector
    from anomaly_detection.temporal import SeasonalityDetector, TrendDetector, CorrelationDetector
    from anomaly_detection.ml import MLBaselineDetector
    from anomaly_detection.scoring import SeverityScorer
"""

from .anomaly_detector import AnomalyDetector
from .models import Anomaly, AnomalyLevel, DetectionResult, PipelineMetrics, Severity

__all__ = [
    "AnomalyDetector",
    "Anomaly",
    "AnomalyLevel",
    "DetectionResult",
    "PipelineMetrics",
    "Severity",
]
