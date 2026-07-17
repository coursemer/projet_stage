"""
generate_dbt_tests.py — déduction automatique de tests dbt via Codestral.

Comble les lacunes qualité documentées en Phase 2 et jamais résolues depuis :
  duplicates · future_date · referential_break  (docs/rapport_followup_phase2.docx)

L'agent déduit directement (aucune autorisation demandée) et écrit ses
propositions dans dbt/tests_generated/ — hors des test-paths dbt (donc rien
ne s'exécute automatiquement). L'ingénieur revoit le diff et copie ce qu'il
approuve vers dbt/tests/ ou dbt/models/*.yml.

Usage :
    python generate_dbt_tests.py
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")

from spark.metrics.validation.codestral_test_generator import CodestralTestGenerator

MODELS = {
    "stg_sales":       "dbt/models/staging/stg_sales.sql",
    "stg_events":      "dbt/models/staging/stg_events.sql",
    "int_sales_daily": "dbt/models/intermediate/int_sales_daily.sql",
    "sales_summary":   "dbt/models/marts/sales_summary.sql",
}


def main() -> None:
    gen = CodestralTestGenerator()
    if not gen.configured:
        print("  ⚠️  MISTRAL_API_KEY non configurée — génération impossible.")
        return

    print("  🤖  Déduction de tests dbt via Codestral (duplicates · future_date · referential_break)\n")
    results = gen.generate_for_models(MODELS)

    for name, result in results.items():
        if result.get("error"):
            print(f"  ❌  {name} : {result['error']}")
            continue
        n_yaml = 1 if result.get("schema_yml") else 0
        n_sql  = len(result.get("singular_tests", []))
        print(f"  ✔  {name:<20} {n_yaml} bloc(s) schema.yml, {n_sql} test(s) singulier(s)")
        if result.get("invalid_refs"):
            print(f"       ⚠️  référence(s) invalide(s) détectée(s) : {result['invalid_refs']} — voir avertissement dans le fichier")
        for s in result.get("skipped", []):
            print(f"       ⏸  {s.get('gap'):<20} — {s.get('reason')}")

    print(f"\n  ✔  Propositions écrites dans dbt/tests_generated/ (statut : à valider par l'ingénieur)")


if __name__ == "__main__":
    main()
