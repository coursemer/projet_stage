"""
CodestralTestGenerator — génération automatique de tests dbt (YAML + SQL)
via Codestral, pour combler les lacunes documentées en Phase 2
(`docs/rapport_followup_phase2.docx`) et jamais comblées depuis :

    duplicates        — filtres actuels laissent passer ~94% des doublons
    future_date        — aucun check temporel actif (0.6% détecté)
    referential_break  — aucune validation FK dans les pipelines (5.4% détecté)

Fonctionnement :
  1. Lit le SQL compilé du modèle dbt + le schema.yml existant
     (`dbt/models/sources.yml`) pour connaître les colonnes disponibles.
  2. Demande à Codestral de proposer, pour CE modèle précis :
       - des tests YAML (unique, not_null, dbt_utils.unique_combination_of_columns,
         relationships, accepted_values...)
       - un test singulier SQL pour les dates futures (`future_date`)
     en ciblant explicitement les 3 lacunes ci-dessus, sans en inventer
     d'autres non justifiées par le schéma.
  3. Écrit les propositions dans `dbt/tests_generated/` — un dossier HORS
     de `test-paths` (dbt_project.yml : test-paths = [tests]), donc rien
     ne s'exécute automatiquement. L'ingénieur revoit le diff et copie
     manuellement ce qu'il approuve vers dbt/tests/ ou dbt/models/*.yml —
     même logique d'approbation que RuleStore, appliquée à du code plutôt
     qu'à des lignes en base.

Usage :
    from spark.metrics.validation.codestral_test_generator import CodestralTestGenerator

    gen = CodestralTestGenerator()
    result = gen.generate_for_model("stg_sales", model_sql_path="dbt/models/staging/stg_sales.sql")
    # → écrit dbt/tests_generated/stg_sales.yml (+ .sql singuliers éventuels)
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

_SYSTEM_PROMPT = (
    "Tu es un ingénieur analytics dbt senior. On te donne le SQL d'un modèle dbt "
    "et les tests déjà définis dans son schema.yml. Trois lacunes de qualité sont "
    "documentées et jamais comblées sur ce projet :\n"
    "  1. duplicates — les filtres actuels laissent passer la plupart des doublons\n"
    "  2. future_date — aucune vérification qu'une date de vente n'est pas dans le futur\n"
    "  3. referential_break — aucune validation d'intégrité référentielle (clé étrangère)\n\n"
    "Pour le modèle donné, propose UNIQUEMENT les tests dbt pertinents pour CES colonnes "
    "précises (n'invente aucune colonne, ne propose pas un test relationships s'il n'y a "
    "pas de table de référence identifiable dans le schema.yml fourni).\n\n"
    "RÈGLES STRICTES de syntaxe dbt (n'utilise QUE ces tests, dans ce format exact) :\n"
    "  - not_null : test simple, sans paramètre\n"
    "  - unique : test simple, sans paramètre (une seule colonne, valeurs uniques)\n"
    "  - accepted_values: {values: [liste de valeurs DISCRÈTES exactes, ex. ['ONLINE','STORE']]} "
    "— JAMAIS pour une plage numérique ou une date, JAMAIS de min_value/max_value dedans\n"
    "  - relationships: {to: ref('autre_modele'), field: colonne_cible} — seulement si "
    "cette table existe réellement dans le schema.yml fourni\n"
    "  - dbt_utils.accepted_range: {min_value: X, max_value: Y} — pour une plage numérique "
    "(revenue, quantity, prix > 0, etc.)\n"
    "  - dbt_utils.unique_combination_of_columns: {combination_of_columns: [colA, colB]} "
    "— pour des doublons sur plusieurs colonnes combinées\n"
    "Pour une contrainte de date (ex. pas de date future), n'utilise PAS de test YAML "
    "générique — préfère un test singulier SQL (voir singular_tests).\n\n"
    "Réponds UNIQUEMENT en JSON valide :\n"
    '{"schema_yml": "<bloc YAML models: ... à coller dans un schema.yml dbt, ou vide>", '
    '"singular_tests": [{"filename": "test_xxx.sql", "sql": "<SELECT qui retourne les '
    'lignes en échec — convention dbt : 0 ligne = test passé>", "targets_gap": '
    '"duplicates|future_date|referential_break"}], '
    '"skipped": [{"gap": "...", "reason": "..."}]}\n'
    "Le SQL des tests singuliers doit être un simple SELECT (pas de DDL), compatible DuckDB."
)


class CodestralTestGenerator:
    """Propose des tests dbt via Codestral pour les lacunes qualité connues."""

    def __init__(
        self,
        model: str = "codestral-latest",
        api_key: Optional[str] = None,
        output_dir: str = "dbt/tests_generated",
    ) -> None:
        self.model = os.environ.get("METRICS_LLM_CODESTRAL_MODEL", model)
        self.api_key = api_key or os.environ.get("MISTRAL_API_KEY", "")
        self.output_dir = output_dir
        self._client = None

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    # ── API publique ──────────────────────────────────────────────────────────

    def generate_for_model(
        self,
        model_name: str,
        model_sql_path: str,
        sources_yml_path: str = "dbt/models/sources.yml",
        known_models: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Génère et écrit les propositions de tests pour `model_name`. Retourne le JSON parsé."""
        if not self.configured:
            return {"schema_yml": "", "singular_tests": [], "skipped": [], "error": "MISTRAL_API_KEY absente"}

        model_sql = self._read(model_sql_path)
        existing_schema = self._read(sources_yml_path) or "(schema.yml introuvable)"
        prompt = self._build_prompt(model_name, model_sql, existing_schema)

        try:
            raw = self._codestral_call(prompt)
            result = self._parse(raw)
        except Exception as exc:
            return {"schema_yml": "", "singular_tests": [], "skipped": [], "error": str(exc)}

        invalid_refs = self._validate_refs(result.get("schema_yml", ""), model_name, known_models)
        result["invalid_refs"] = invalid_refs

        self._write(model_name, result, invalid_refs)
        return result

    def generate_for_models(self, models: Dict[str, str], sources_yml_path: str = "dbt/models/sources.yml") -> Dict[str, Dict[str, Any]]:
        """`models` = {model_name: chemin_sql}. Retourne un résultat par modèle."""
        known_models = list(models.keys())
        return {
            name: self.generate_for_model(name, sql_path, sources_yml_path, known_models=known_models)
            for name, sql_path in models.items()
        }

    # ── Validation anti-hallucination ────────────────────────────────────────

    def _validate_refs(
        self, schema_yml: str, model_name: str, known_models: Optional[List[str]]
    ) -> List[str]:
        """Détecte les `ref('xxx')` pointant vers un modèle qui n'existe pas dans ce projet.

        Codestral peut halluciner des tables de référence (ex. relationships vers
        un modèle inexistant) malgré l'instruction contraire — on ne fait jamais
        confiance aveuglément, on signale plutôt que de laisser passer.
        """
        if not schema_yml or known_models is None:
            return []
        allowed = set(known_models) | {model_name}
        refs = set(re.findall(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", schema_yml))
        return sorted(refs - allowed)

    # ── Contexte ──────────────────────────────────────────────────────────────

    def _read(self, path: str) -> str:
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""

    def _build_prompt(self, model_name: str, model_sql: str, existing_schema: str) -> str:
        return (
            f"Modèle : {model_name}\n\n"
            f"=== SQL du modèle ===\n{model_sql[:3000]}\n\n"
            f"=== schema.yml complet du projet (sources + tests existants) ===\n"
            f"{existing_schema[:4000]}\n"
        )

    # ── Codestral (via SDK Mistral, endpoint chat completions) ───────────────

    def _get_client(self):
        if self._client is None:
            try:
                from mistralai import Mistral
            except ImportError:
                from mistralai.client import Mistral  # mistralai>=2.x
            self._client = Mistral(api_key=self.api_key)
        return self._client

    def _codestral_call(self, prompt: str) -> str:
        client = self._get_client()
        response = client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content.strip()

    @staticmethod
    def _parse(raw: str) -> Dict[str, Any]:
        data = json.loads(raw)
        return {
            "schema_yml": data.get("schema_yml", "") or "",
            "singular_tests": data.get("singular_tests", []) or [],
            "skipped": data.get("skipped", []) or [],
        }

    # ── Écriture (staging — hors test-paths dbt) ─────────────────────────────

    def _write(self, model_name: str, result: Dict[str, Any], invalid_refs: Optional[List[str]] = None) -> None:
        os.makedirs(self.output_dir, exist_ok=True)

        if result.get("schema_yml"):
            path = os.path.join(self.output_dir, f"{model_name}.yml")
            with open(path, "w", encoding="utf-8") as f:
                f.write(
                    f"# Proposé par Codestral — À REVOIR avant de copier dans "
                    f"dbt/models/*.yml (ne s'exécute pas automatiquement)\n"
                )
                if invalid_refs:
                    f.write(
                        f"# ⚠️  ATTENTION — référence(s) vers un/des modèle(s) INEXISTANT(S) "
                        f"dans ce projet : {', '.join(invalid_refs)}. Ce test ne fonctionnera "
                        f"pas tel quel (dbt échouera à la compilation) — corriger ou retirer "
                        f"avant de l'activer.\n"
                    )
                f.write(result["schema_yml"])

        for test in result.get("singular_tests", []):
            filename = test.get("filename") or "test_generated.sql"
            if model_name not in filename:
                filename = f"{model_name}__{filename}"
            path = os.path.join(self.output_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                f.write(
                    f"-- Proposé par Codestral (cible : {test.get('targets_gap', '?')}) — "
                    f"À REVOIR avant de copier dans dbt/tests/ (ne s'exécute pas automatiquement)\n"
                )
                f.write(test.get("sql", ""))
