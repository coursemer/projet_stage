"""
LineageParser — Semaine 12 : lineage automatique depuis dbt manifest + Spark jobs.

Sources parsées :
  1. dbt manifest.json  — modèles, sources et leurs dépendances (depends_on)
  2. Spark jobs Python  — regex sur .read.parquet() et .write.parquet()

Usage :
    from spark.catalog import LineageParser

    parser = LineageParser()

    dbt_graph   = parser.from_dbt("dbt/target/manifest.json")
    spark_graph = parser.from_spark_jobs("spark/jobs/")
    full_graph  = dbt_graph.merge(spark_graph)

    print(full_graph.upstream_of("int_sales_daily"))
    # → ["stg_sales"]
    print(full_graph.descendants("stg_sales"))
    # → ["int_sales_daily", "sales_summary"]
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import List

from .models import LineageEdge, LineageGraph, LineageNode


class LineageParser:
    """Parse les sources de lineage et retourne des LineageGraph."""

    # ── dbt ───────────────────────────────────────────────────────────────────

    def from_dbt(self, manifest_path: str) -> LineageGraph:
        """
        Parse un manifest.json dbt et construit le graphe de lineage.

        Inclut :
          - les modèles (resource_type=model)
          - les sources (resource_type=source)
          - les dépendances (depends_on.nodes)
        """
        with open(manifest_path) as f:
            manifest = json.load(f)

        nodes: List[LineageNode] = []
        edges: List[LineageEdge] = []
        seen_nodes: set = set()

        # Sources
        for uid, src in manifest.get("sources", {}).items():
            name = src.get("name", uid)
            if name not in seen_nodes:
                nodes.append(LineageNode(
                    name=name,
                    type="source",
                    source_path=src.get("original_file_path", ""),
                    extra={"unique_id": uid, "schema": src.get("schema", "")},
                ))
                seen_nodes.add(name)

        # Modèles + tests
        for uid, node in manifest.get("nodes", {}).items():
            rtype = node.get("resource_type", "")
            if rtype not in ("model", "test"):
                continue
            name = node.get("name", uid)
            if name not in seen_nodes:
                nodes.append(LineageNode(
                    name=name,
                    type=rtype,
                    source_path=node.get("original_file_path", ""),
                    extra={
                        "unique_id": uid,
                        "description": node.get("description", ""),
                        "columns": list(node.get("columns", {}).keys()),
                    },
                ))
                seen_nodes.add(name)

            # Arêtes : depends_on.nodes
            for dep_uid in node.get("depends_on", {}).get("nodes", []):
                dep_name = _uid_to_name(dep_uid, manifest)
                if dep_name and dep_name != name:
                    edges.append(LineageEdge(
                        upstream=dep_name,
                        downstream=name,
                        relation="depends_on",
                    ))

        return LineageGraph(nodes=nodes, edges=edges)

    # ── Spark jobs ────────────────────────────────────────────────────────────

    def from_spark_jobs(self, jobs_dir: str) -> LineageGraph:
        """
        Parse les fichiers Python dans jobs_dir et extrait les lectures/écritures.

        Patterns reconnus :
          spark.read.parquet("path/to/dir")     → nœud source
          .write.parquet("path/to/dir")          → nœud destination
          spark.read.csv("path/to/file.csv")    → nœud source
        """
        nodes: List[LineageNode] = []
        edges: List[LineageEdge] = []
        seen_nodes: set = set()

        for py_file in sorted(Path(jobs_dir).glob("*.py")):
            job_name = py_file.stem
            source   = py_file.read_text(errors="ignore")

            reads  = _extract_paths(source, r'\.read\.\w+\(["\']([^"\']+)["\']')
            writes = _extract_paths(source, r'\.write\.\w+\(["\']([^"\']+)["\']')

            # Ajouter le job comme nœud
            if job_name not in seen_nodes:
                nodes.append(LineageNode(
                    name=job_name,
                    type="job",
                    source_path=str(py_file),
                ))
                seen_nodes.add(job_name)

            for path in reads:
                src_name = _path_to_name(path)
                if src_name not in seen_nodes:
                    nodes.append(LineageNode(name=src_name, type="dataset", source_path=path))
                    seen_nodes.add(src_name)
                edges.append(LineageEdge(upstream=src_name, downstream=job_name, relation="reads"))

            for path in writes:
                dst_name = _path_to_name(path)
                if dst_name not in seen_nodes:
                    nodes.append(LineageNode(name=dst_name, type="dataset", source_path=path))
                    seen_nodes.add(dst_name)
                edges.append(LineageEdge(upstream=job_name, downstream=dst_name, relation="writes"))

        return LineageGraph(nodes=nodes, edges=edges)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid_to_name(uid: str, manifest: dict) -> str:
    """Résout un unique_id dbt → nom court."""
    # Cherche dans nodes
    node = manifest.get("nodes", {}).get(uid)
    if node:
        return node.get("name", uid)
    # Cherche dans sources
    src = manifest.get("sources", {}).get(uid)
    if src:
        return src.get("name", uid)
    # Fallback: dernière partie du uid (ex. "model.pkg.stg_sales" → "stg_sales")
    return uid.split(".")[-1]


def _extract_paths(source: str, pattern: str) -> List[str]:
    """Extrait les chemins de fichiers depuis un fichier source Python."""
    return re.findall(pattern, source)


def _path_to_name(path: str) -> str:
    """Convertit un chemin en nom de nœud court."""
    # Prend le dernier segment non vide et retire l'extension
    parts = [p for p in path.replace("\\", "/").split("/") if p]
    if not parts:
        return path
    name = parts[-1]
    # Retire extension
    name = re.sub(r'\.\w+$', '', name)
    # args variables comme {args.dest} → "dest"
    name = re.sub(r'^\{args\.(\w+)\}$', r'\1', name)
    return name or path
