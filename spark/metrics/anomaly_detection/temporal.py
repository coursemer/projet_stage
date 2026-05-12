"""Re-exports temporal detectors under the 'temporal' namespace."""
from .temporal_detector import CorrelationDetector, SeasonalityDetector, TrendDetector

__all__ = ["SeasonalityDetector", "TrendDetector", "CorrelationDetector"]
