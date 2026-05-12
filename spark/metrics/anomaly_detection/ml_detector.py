"""
ML Baseline Anomaly Detector
------------------------------
Uses scikit-learn's Isolation Forest as an unsupervised anomaly detector.

It acts as a "second opinion" layer:
  - Trains on historical feature vectors (volume + performance + distribution stats).
  - Scores the current snapshot.
  - If Isolation Forest agrees with a rule-based anomaly, severity is escalated.
  - Standalone ML anomalies are emitted at MEDIUM severity.

Feature vector (per snapshot):
  - row_count
  - duration_seconds
  - task_failures
  - Per configured column: mean, std, null_rate
"""
from __future__ import annotations

import uuid
from typing import Dict, List, Optional

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity


class MLBaselineDetector:
    """
    Isolation Forest–based anomaly detector.

    Parameters
    ----------
    contamination : float
        Expected fraction of anomalies in training data (default 0.05).
    n_estimators : int
        Number of trees in the forest.
    min_train_samples : int
        Minimum number of historical snapshots to train.
    tracked_columns : list[str]
        Columns whose stats are included in the feature vector.
    """

    def __init__(
        self,
        contamination: float = 0.05,
        n_estimators: int = 100,
        min_train_samples: int = 30,
        tracked_columns: Optional[List[str]] = None,
    ) -> None:
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.min_train_samples = min_train_samples
        self.tracked_columns = tracked_columns or []

        self._model: Optional[IsolationForest] = None
        self._scaler: Optional[StandardScaler] = None
        self._feature_names: List[str] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fit(self, history: List[PipelineMetrics]) -> bool:
        """
        Train the Isolation Forest on historical snapshots.

        Returns True if training succeeded, False if not enough data.
        """
        if len(history) < self.min_train_samples:
            return False

        X, feature_names = self._build_feature_matrix(history)
        if X.shape[0] < self.min_train_samples:
            return False

        self._feature_names = feature_names
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)

        self._model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(X_scaled)
        return True

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        """
        Fit on history and score current.  Returns anomalies if flagged.
        """
        anomalies: List[Anomaly] = []

        if not self.fit(history):
            return anomalies   # Not enough history

        # Build feature vector for current snapshot
        x, _ = self._build_feature_matrix([current])
        if x.shape[0] == 0:
            return anomalies

        x_scaled = self._scaler.transform(x)

        # Isolation Forest: -1 = anomaly, +1 = normal
        prediction = self._model.predict(x_scaled)[0]
        score = self._model.score_samples(x_scaled)[0]   # more negative = more anomalous

        if prediction == -1:
            severity = self._score_to_severity(score)
            anomalies.append(Anomaly(
                id=f"{current.pipeline_name}-ml-{uuid.uuid4().hex[:8]}",
                pipeline_name=current.pipeline_name,
                level=AnomalyLevel.ML,
                severity=severity,
                description=(
                    f"Isolation Forest flagged this snapshot as anomalous "
                    f"(anomaly score: {score:.4f}). "
                    f"Features: {', '.join(self._feature_names)}."
                ),
                metric_name="isolation_forest_score",
                observed_value=score,
                context={
                    "detector": "MLBaselineDetector",
                    "model": "IsolationForest",
                    "anomaly_score": score,
                    "feature_names": self._feature_names,
                    "feature_values": x[0].tolist(),
                },
            ))

        return anomalies

    def get_anomaly_score(self, current: PipelineMetrics, history: List[PipelineMetrics]) -> Optional[float]:
        """Return raw anomaly score for use by SeverityScorer."""
        if not self.fit(history):
            return None
        x, _ = self._build_feature_matrix([current])
        if x.shape[0] == 0:
            return None
        x_scaled = self._scaler.transform(x)
        return float(self._model.score_samples(x_scaled)[0])

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def _build_feature_matrix(
        self, snapshots: List[PipelineMetrics]
    ) -> tuple[np.ndarray, List[str]]:
        """Convert a list of PipelineMetrics into a 2D numpy feature matrix."""
        feature_names: List[str] = []
        rows = []

        # Determine feature names from first non-null snapshot
        sample = next((s for s in snapshots if s.row_count is not None), None)

        base_features = ["row_count", "duration_seconds", "task_failures"]
        col_stat_features = [
            f"{col}.{stat}"
            for col in self.tracked_columns
            for stat in ("mean", "std", "null_rate")
        ]
        all_features = base_features + col_stat_features

        for snap in snapshots:
            row = []
            for feat in all_features:
                if "." in feat:
                    col, stat = feat.split(".", 1)
                    val = snap.column_stats.get(col, {}).get(stat, np.nan)
                else:
                    raw = getattr(snap, feat, None)
                    val = float(raw) if raw is not None else np.nan
                row.append(val)
            rows.append(row)

        if not rows:
            return np.empty((0, len(all_features))), all_features

        X = np.array(rows, dtype=float)

        # Impute NaN with column medians (computed from non-nan values)
        for col_idx in range(X.shape[1]):
            col = X[:, col_idx]
            nan_mask = np.isnan(col)
            if nan_mask.all():
                X[:, col_idx] = 0.0
            elif nan_mask.any():
                median = np.nanmedian(col)
                X[nan_mask, col_idx] = median

        return X, all_features

    # ------------------------------------------------------------------

    def _score_to_severity(self, score: float) -> Severity:
        """
        Isolation Forest score_samples: typical range [-0.5, 0].
        More negative = more anomalous.
        """
        if score < -0.35:
            return Severity.CRITICAL
        if score < -0.25:
            return Severity.HIGH
        if score < -0.15:
            return Severity.MEDIUM
        return Severity.LOW
