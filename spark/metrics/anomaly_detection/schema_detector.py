"""
Schema Anomaly Detector
-----------------------
Compares the current schema snapshot to the expected (reference) schema.

Detects:
  - Added columns   (unexpected new columns)
  - Dropped columns (missing expected columns)
  - Type changes    (column type changed)
"""
from __future__ import annotations

import uuid
from typing import Dict, List, Optional

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity


class SchemaDetector:
    """
    Detect schema-level anomalies by comparing current vs. reference schema.

    Parameters
    ----------
    reference_schema : dict[str, str]
        Expected mapping of column_name → dtype string.
        If None, the detector uses the first historical snapshot as reference.
    critical_columns : list[str]
        Columns whose removal or type change is always CRITICAL.
    """

    def __init__(
        self,
        reference_schema: Optional[Dict[str, str]] = None,
        critical_columns: Optional[List[str]] = None,
    ) -> None:
        self._reference_schema = reference_schema
        self.critical_columns = set(critical_columns or [])

    # ------------------------------------------------------------------

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        if not current.schema_snapshot:
            return anomalies

        reference = self._get_reference(history)
        if reference is None:
            return anomalies  # No reference yet – first run

        current_schema = current.schema_snapshot

        # ── Dropped columns ───────────────────────────────────────────
        for col, dtype in reference.items():
            if col not in current_schema:
                severity = (
                    Severity.CRITICAL if col in self.critical_columns else Severity.HIGH
                )
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=f"Column '{col}' (type: {dtype}) was DROPPED.",
                        metric_name="schema.dropped_column",
                        severity=severity,
                        context={"column": col, "expected_dtype": dtype},
                    )
                )

        # ── Added columns ─────────────────────────────────────────────
        for col, dtype in current_schema.items():
            if col not in reference:
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=f"New unexpected column '{col}' (type: {dtype}) detected.",
                        metric_name="schema.added_column",
                        severity=Severity.LOW,
                        context={"column": col, "detected_dtype": dtype},
                    )
                )

        # ── Type changes ───────────────────────────────────────────────
        for col in set(reference) & set(current_schema):
            ref_dtype = reference[col]
            cur_dtype = current_schema[col]
            if ref_dtype != cur_dtype:
                severity = (
                    Severity.CRITICAL if col in self.critical_columns else Severity.HIGH
                )
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=(
                            f"Column '{col}' type changed: "
                            f"'{ref_dtype}' → '{cur_dtype}'."
                        ),
                        metric_name="schema.type_change",
                        severity=severity,
                        context={
                            "column": col,
                            "old_dtype": ref_dtype,
                            "new_dtype": cur_dtype,
                        },
                    )
                )

        return anomalies

    # ------------------------------------------------------------------

    def _get_reference(
        self, history: List[PipelineMetrics]
    ) -> Optional[Dict[str, str]]:
        """Return the reference schema: explicit config or first history entry."""
        if self._reference_schema:
            return self._reference_schema
        for snap in history:
            if snap.schema_snapshot:
                return snap.schema_snapshot
        return None

    def _make_anomaly(
        self,
        pipeline: str,
        description: str,
        metric_name: str,
        severity: Severity,
        context: dict,
    ) -> Anomaly:
        return Anomaly(
            id=f"{pipeline}-schema-{uuid.uuid4().hex[:8]}",
            pipeline_name=pipeline,
            level=AnomalyLevel.SCHEMA,
            severity=severity,
            description=description,
            metric_name=metric_name,
            context={"detector": "SchemaDetector", **context},
        )
