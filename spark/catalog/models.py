"""
Modèles de données — Semaine 12 : Data Catalog enrichi.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional


EntryType = Literal["pipeline", "model", "dataset", "source"]


@dataclass
class CatalogEntry:
    """
    Entrée du catalog décrivant un asset de données.

    Attributs
    ---------
    name          : identifiant unique (ex. "clean_sales", "stg_sales")
    type          : pipeline | model | dataset | source
    description   : description humaine
    owner         : équipe ou personne responsable
    tags          : liste de labels libres
    quality_score : 0.0–1.0 calculé par le Data Trust Agent
    source_path   : chemin de la source (fichier dbt, job Spark…)
    extra         : métadonnées libres
    """
    name: str
    type: EntryType = "pipeline"
    description: str = ""
    owner: str = ""
    tags: List[str] = field(default_factory=list)
    quality_score: Optional[float] = None    # 0.0–1.0
    source_path: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_text(self) -> str:
        """Représentation textuelle pour l'indexation sémantique."""
        parts = [self.name, self.type, self.description]
        if self.tags:
            parts.append(" ".join(self.tags))
        if self.owner:
            parts.append(self.owner)
        return " ".join(p for p in parts if p)


@dataclass
class Incident:
    """Incident de qualité lié à une entrée du catalog."""
    entry_name: str
    severity: str                     # LOW | MEDIUM | HIGH | CRITICAL
    description: str
    anomaly_type: str = ""            # volume | performance | ml | …
    resolved: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    id: Optional[int] = None


@dataclass
class LineageNode:
    """Nœud dans le graphe de lineage."""
    name: str
    type: str                         # model | source | job | dataset
    source_path: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    """Lien directionnel upstream → downstream."""
    upstream: str
    downstream: str
    relation: str = "depends_on"      # depends_on | reads | writes


@dataclass
class LineageGraph:
    """Graphe de lineage complet (nœuds + arêtes)."""
    nodes: List[LineageNode] = field(default_factory=list)
    edges: List[LineageEdge] = field(default_factory=list)

    def upstream_of(self, name: str) -> List[str]:
        """Retourne les noms des nœuds en amont direct de `name`."""
        return [e.upstream for e in self.edges if e.downstream == name]

    def downstream_of(self, name: str) -> List[str]:
        """Retourne les noms des nœuds en aval direct de `name`."""
        return [e.downstream for e in self.edges if e.upstream == name]

    def ancestors(self, name: str) -> List[str]:
        """Remonte récursivement tous les ancêtres."""
        visited, queue = set(), [name]
        while queue:
            cur = queue.pop()
            for up in self.upstream_of(cur):
                if up not in visited:
                    visited.add(up)
                    queue.append(up)
        return list(visited)

    def descendants(self, name: str) -> List[str]:
        """Descend récursivement tous les descendants."""
        visited, queue = set(), [name]
        while queue:
            cur = queue.pop()
            for down in self.downstream_of(cur):
                if down not in visited:
                    visited.add(down)
                    queue.append(down)
        return list(visited)

    def node_names(self) -> List[str]:
        return [n.name for n in self.nodes]

    def merge(self, other: "LineageGraph") -> "LineageGraph":
        """Fusionne deux graphes (dbt + Spark) sans doublons."""
        existing_names = {n.name for n in self.nodes}
        existing_edges = {(e.upstream, e.downstream) for e in self.edges}
        new_nodes = self.nodes + [n for n in other.nodes if n.name not in existing_names]
        new_edges = self.edges + [
            e for e in other.edges if (e.upstream, e.downstream) not in existing_edges
        ]
        return LineageGraph(nodes=new_nodes, edges=new_edges)


@dataclass
class SearchResult:
    """Résultat d'une recherche sémantique."""
    entry: CatalogEntry
    score: float          # 0.0–1.0 (plus haut = plus pertinent)
    backend: str          # "tfidf" | "embeddings"
