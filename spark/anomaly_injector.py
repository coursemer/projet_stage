"""
Framework d'injection d'anomalies contrôlées
Injecte jusqu'à 10 types d'anomalies dans un DataFrame pandas et retourne
un rapport JSON détaillant ce qui a été injecté.

10 types d'anomalies :
  1.  nulls                  — valeurs nulles (données manquantes en amont)
  2.  duplicates             — lignes dupliquées (double ingestion)
  3.  out_of_range           — valeurs numériques hors bornes métier
  4.  type_mismatch          — valeur non castable dans une colonne numérique
  5.  format_violation       — identifiant au mauvais format (ORD-123 → ord123)
  6.  future_date            — date dans le futur
  7.  negative_values        — valeur négative sur colonne strictement positive
  8.  referential_break      — clé étrangère inexistante dans la table de référence
  9.  whitespace_corruption  — espaces parasites dans une colonne catégorielle
  10. revenue_inconsistency  — revenue incohérent avec quantity * unit_price

Usage programmatique :
    from spark.anomaly_injector import AnomalyInjector
    injector = AnomalyInjector(seed=42, injection_rate=0.05)
    dirty_df, report = injector.inject(df, anomaly_types=["nulls", "duplicates"])

Usage CLI :
    python spark/anomaly_injector.py \\
        --input  spark/data/raw/sales.csv \\
        --output spark/data/raw/sales_dirty.csv \\
        --report spark/data/raw/injection_report.json \\
        --rate   0.05 --seed 42 \\
        --types  nulls,duplicates,out_of_range
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


# ── Catalogue des anomalies ───────────────────────────────────────────────────

ANOMALY_CATALOG = {
    "nulls": {
        "description": "Injection de valeurs nulles (NaN) — simule des données manquantes en amont",
        "default_columns": ["customer_id", "product_id", "sale_date"],
        "use_case": "Tester la robustesse du pipeline face aux valeurs manquantes sur les clés métier",
    },
    "duplicates": {
        "description": "Duplication de lignes entières — simule une double ingestion depuis la source",
        "default_columns": None,
        "use_case": "Vérifier que l'étape de déduplication (clean_sales) fonctionne correctement",
    },
    "out_of_range": {
        "description": "Valeurs numériques hors bornes métier (quantity=9999, unit_price=0.0001)",
        "default_columns": ["quantity", "unit_price"],
        "use_case": "Valider les filtres de sanity check sur les bornes numériques",
    },
    "type_mismatch": {
        "description": "Valeur non castable dans une colonne numérique (ex. 'N/A', '--')",
        "default_columns": ["quantity", "unit_price"],
        "use_case": "Tester la résistance au schema enforcement de Spark/Pandera",
    },
    "format_violation": {
        "description": "Identifiant au mauvais format (ORD-1234567 → ord1234567 ou ORD_1234567)",
        "default_columns": ["order_id", "customer_id"],
        "use_case": "Valider les checks de format regex dans Pandera (^ORD-\\d+$)",
    },
    "future_date": {
        "description": "Date dans le futur (now + 1 à 365 jours) — simule une erreur d'horodatage",
        "default_columns": ["sale_date"],
        "use_case": "Détecter les anomalies temporelles dans les pipelines orientés date",
    },
    "negative_values": {
        "description": "Valeur négative sur une colonne strictement positive (quantity, revenue)",
        "default_columns": ["quantity", "revenue"],
        "use_case": "Tester les contraintes Check.greater_than(0) de Pandera",
    },
    "referential_break": {
        "description": "Clé étrangère inexistante dans la table de référence (customer_id fantôme)",
        "default_columns": ["customer_id", "product_id"],
        "use_case": "Simuler des violations d'intégrité référentielle inter-tables",
    },
    "whitespace_corruption": {
        "description": "Espaces parasites (leading/trailing/double) dans une colonne catégorielle",
        "default_columns": ["channel", "region", "currency"],
        "use_case": "Détecter les faux négatifs sur les checks Check.isin() après strip() manqué",
    },
    "revenue_inconsistency": {
        "description": "Revenue incohérent avec quantity × unit_price (facteur aléatoire appliqué)",
        "default_columns": ["revenue"],
        "use_case": "Tester les règles de cohérence croisée entre colonnes calculées",
    },
}

ALL_TYPES = list(ANOMALY_CATALOG.keys())


# ── Classe principale ─────────────────────────────────────────────────────────

class AnomalyInjector:
    """
    Injecte des anomalies contrôlées dans un DataFrame pandas.

    Paramètres
    ----------
    seed : int
        Graine pour la reproductibilité exacte.
    injection_rate : float
        Proportion de lignes affectées par chaque anomalie (défaut : 0.05 = 5 %).
    """

    def __init__(self, seed: int = 42, injection_rate: float = 0.05):
        self.seed           = seed
        self.injection_rate = injection_rate
        self._rng           = np.random.default_rng(seed)

    # ── API publique ──────────────────────────────────────────────────────────

    def inject(
        self,
        df: pd.DataFrame,
        anomaly_types: Optional[list] = None,
        target_columns: Optional[dict] = None,
        valid_ids: Optional[dict] = None,
        tag_rows: bool = False,
    ) -> tuple:
        """
        Injecte les anomalies demandées dans df.

        Paramètres
        ----------
        df            : DataFrame source (non modifié en place).
        anomaly_types : liste de clés d'anomalies (None = toutes).
        target_columns: dict {type: [col1, col2]} pour surcharger les colonnes par défaut.
        valid_ids     : dict {col: set_of_valid_ids} pour referential_break.
        tag_rows      : si True, ajoute les colonnes ``_row_id`` (int) et
                        ``_injected_types`` (str séparé par '|') au CSV de sortie.
                        Ces colonnes permettent au controlled_failure_pipeline de
                        mesurer précisément le taux de détection par type d'anomalie.

        Retourne
        --------
        (dirty_df, report) où report est un dict JSON-sérialisable.
        """
        types = anomaly_types or ALL_TYPES
        dirty = df.copy()

        # Numérotation stable des lignes (survit aux duplicats ajoutés plus loin)
        dirty["_row_id"] = range(len(dirty))
        # Dictionnaire {row_id: set_of_types} pour le tagging
        row_tags: dict[int, set] = {}

        log: list[dict] = []

        for atype in types:
            if atype not in ANOMALY_CATALOG:
                raise ValueError(f"Type d'anomalie inconnu : '{atype}'. "
                                 f"Valeurs valides : {ALL_TYPES}")

            cols = (target_columns or {}).get(atype) or ANOMALY_CATALOG[atype]["default_columns"]
            n_inject = max(1, int(len(dirty) * self.injection_rate))
            indices  = self._rng.choice(len(dirty), size=n_inject, replace=False).tolist()

            if atype == "nulls":
                dirty, entries = self._inject_nulls(dirty, cols, indices)
            elif atype == "duplicates":
                dirty, entries = self._inject_duplicates(dirty, indices)
            elif atype == "out_of_range":
                dirty, entries = self._inject_out_of_range(dirty, cols, indices)
            elif atype == "type_mismatch":
                dirty, entries = self._inject_type_mismatch(dirty, cols, indices)
            elif atype == "format_violation":
                dirty, entries = self._inject_format_violation(dirty, cols, indices)
            elif atype == "future_date":
                dirty, entries = self._inject_future_date(dirty, cols, indices)
            elif atype == "negative_values":
                dirty, entries = self._inject_negative_values(dirty, cols, indices)
            elif atype == "referential_break":
                dirty, entries = self._inject_referential_break(dirty, cols, indices, valid_ids)
            elif atype == "whitespace_corruption":
                dirty, entries = self._inject_whitespace_corruption(dirty, cols, indices)
            elif atype == "revenue_inconsistency":
                dirty, entries = self._inject_revenue_inconsistency(dirty, indices)

            log.extend(entries)

            # Accumuler les tags par row_id
            for entry in entries:
                for idx in entry["row_indices"]:
                    row_tags.setdefault(idx, set()).add(atype)

        # ── Colonnes de tagging optionnelles ─────────────────────────────────
        if tag_rows:
            if "_row_id" not in dirty.columns:
                dirty["_row_id"] = range(len(dirty))
            dirty["_injected_types"] = dirty["_row_id"].map(
                lambda rid: "|".join(sorted(row_tags.get(rid, set()))) or ""
            )
        else:
            # Supprimer _row_id si on ne le veut pas dans la sortie
            dirty = dirty.drop(columns=["_row_id"], errors="ignore")

        report = {
            "seed":              self.seed,
            "injection_rate":    self.injection_rate,
            "total_rows_input":  len(df),
            "total_rows_output": len(dirty),
            "generated_at":      datetime.utcnow().isoformat() + "Z",
            "anomalies_injected": log,
            "total_anomalous_rows": len({idx for e in log for idx in e["row_indices"]}),
            "tag_rows":           tag_rows,
        }
        return dirty, report

    # ── Méthodes privées — un type par méthode ────────────────────────────────

    def _pick_cols(self, df: pd.DataFrame, cols: list | None) -> list:
        """Retourne les colonnes présentes dans df parmi la liste demandée."""
        if not cols:
            return []
        return [c for c in cols if c in df.columns]

    def _inject_nulls(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        for col in cols:
            df = df.copy()
            df.loc[indices, col] = np.nan
            entries.append({
                "type": "nulls", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["nulls"]["description"],
            })
        return df, entries

    def _inject_duplicates(self, df, indices):
        dups = df.iloc[indices].copy()
        df   = pd.concat([df, dups], ignore_index=True)
        return df, [{
            "type": "duplicates", "column": None,
            "row_indices": indices, "count": len(indices),
            "description": ANOMALY_CATALOG["duplicates"]["description"],
        }]

    def _inject_out_of_range(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        extremes = {"quantity": 9_999, "unit_price": 0.0001, "revenue": -999_999}
        for col in cols:
            df = df.copy()
            extreme = extremes.get(col, 99_999)
            df.loc[indices, col] = extreme
            entries.append({
                "type": "out_of_range", "column": col,
                "row_indices": indices, "count": len(indices),
                "injected_value": extreme,
                "description": ANOMALY_CATALOG["out_of_range"]["description"],
            })
        return df, entries

    def _inject_type_mismatch(self, df, cols, indices):
        cols    = self._pick_cols(df, cols)
        garbage = ["N/A", "--", "null", "n/a", "???"]
        entries = []
        for col in cols:
            df    = df.copy()
            df[col] = df[col].astype(object)
            vals  = self._rng.choice(garbage, len(indices))
            for i, v in zip(indices, vals):
                df.at[i, col] = v
            entries.append({
                "type": "type_mismatch", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["type_mismatch"]["description"],
            })
        return df, entries

    def _inject_format_violation(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        for col in cols:
            df = df.copy()
            for i in indices:
                val = str(df.at[i, col])
                # ORD-1234567 → ord1234567  ou  CUST-12345 → CUST_12345
                corrupted = val.lower().replace("-", "") if self._rng.random() > 0.5 \
                    else val.replace("-", "_")
                df.at[i, col] = corrupted
            entries.append({
                "type": "format_violation", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["format_violation"]["description"],
            })
        return df, entries

    def _inject_future_date(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        today = datetime.utcnow().date()
        for col in cols:
            df = df.copy()
            for i in indices:
                delta = int(self._rng.integers(1, 366))
                df.at[i, col] = (today + timedelta(days=delta)).strftime("%Y-%m-%d")
            entries.append({
                "type": "future_date", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["future_date"]["description"],
            })
        return df, entries

    def _inject_negative_values(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        for col in cols:
            df = df.copy()
            df.loc[indices, col] = pd.to_numeric(df.loc[indices, col], errors="coerce").abs() * -1
            entries.append({
                "type": "negative_values", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["negative_values"]["description"],
            })
        return df, entries

    def _inject_referential_break(self, df, cols, indices, valid_ids):
        cols = self._pick_cols(df, cols)
        entries = []
        for col in cols:
            df = df.copy()
            # Génère des IDs fantômes qui ne sont PAS dans le jeu de référence
            prefix = col.replace("_id", "").upper()
            ghost_ids = [f"{prefix}-GHOST{i:04d}" for i in range(len(indices))]
            for i, gid in zip(indices, ghost_ids):
                df.at[i, col] = gid
            entries.append({
                "type": "referential_break", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["referential_break"]["description"],
            })
        return df, entries

    def _inject_whitespace_corruption(self, df, cols, indices):
        cols = self._pick_cols(df, cols)
        entries = []
        patterns = [
            lambda v: f"  {v}",      # leading spaces
            lambda v: f"{v}  ",      # trailing spaces
            lambda v: f" {v} ",      # both sides
            lambda v: v.replace(" ", "  ") if " " in v else f"{v} ",  # double space
        ]
        for col in cols:
            df = df.copy()
            for i in indices:
                fn = patterns[int(self._rng.integers(0, len(patterns)))]
                df.at[i, col] = fn(str(df.at[i, col]))
            entries.append({
                "type": "whitespace_corruption", "column": col,
                "row_indices": indices, "count": len(indices),
                "description": ANOMALY_CATALOG["whitespace_corruption"]["description"],
            })
        return df, entries

    def _inject_revenue_inconsistency(self, df, indices):
        if "revenue" not in df.columns:
            return df, []
        df = df.copy()
        factors = self._rng.uniform(0.1, 0.5, len(indices))   # revenu multiplié par 0.1–0.5
        for i, f in zip(indices, factors):
            try:
                df.at[i, "revenue"] = round(float(df.at[i, "revenue"]) * f, 2)
            except (ValueError, TypeError):
                pass
        return df, [{
            "type": "revenue_inconsistency", "column": "revenue",
            "row_indices": indices, "count": len(indices),
            "description": ANOMALY_CATALOG["revenue_inconsistency"]["description"],
        }]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Injecte des anomalies dans un CSV")
    parser.add_argument("--input",  required=True,  help="CSV source propre")
    parser.add_argument("--output", required=True,  help="CSV de sortie avec anomalies")
    parser.add_argument("--report", required=True,  help="Rapport JSON d'injection")
    parser.add_argument("--rate",   type=float, default=0.05, help="Taux d'injection (défaut: 0.05)")
    parser.add_argument("--seed",   type=int,   default=42,   help="Graine aléatoire (défaut: 42)")
    parser.add_argument("--types",  default=",".join(ALL_TYPES),
                        help=f"Types à injecter, séparés par virgules. "
                             f"Défaut: tous. Disponibles: {', '.join(ALL_TYPES)}")
    parser.add_argument("--tag-rows", action="store_true", default=False,
                        help="Ajoute les colonnes _row_id et _injected_types au CSV de sortie "
                             "pour une détection précise dans controlled_failure_pipeline.")
    args = parser.parse_args()

    types = [t.strip() for t in args.types.split(",") if t.strip()]

    print(f"[anomaly_injector] Lecture de {args.input}")
    df = pd.read_csv(args.input)
    print(f"  {len(df):,} lignes lues")

    injector = AnomalyInjector(seed=args.seed, injection_rate=args.rate)
    dirty_df, report = injector.inject(df, anomaly_types=types,
                                       tag_rows=args.tag_rows)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    dirty_df.to_csv(args.output, index=False)
    print(f"  {len(dirty_df):,} lignes écrites → {args.output}")

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  Rapport JSON      → {args.report}")
    print(f"  Lignes anormales  : {report['total_anomalous_rows']:,} "
          f"({report['total_anomalous_rows']/report['total_rows_input']*100:.1f}%)")
    print("[anomaly_injector] Terminé.")


if __name__ == "__main__":
    main()
