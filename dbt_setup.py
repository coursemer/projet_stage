#!/usr/bin/env python3
"""
dbt Core + Data Catalog Setup
Initialize and configure dbt project for data transformation
"""

import os
import json
import subprocess
from pathlib import Path


def setup_dbt_project():
    """Initialize dbt project structure."""
    dbt_dir = Path("dbt")
    dbt_dir.mkdir(exist_ok=True)
    
    # Create dbt_project.yml
    dbt_project = {
        "name": "projet_stage_dbt",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "default",
        "model-paths": ["models"],
        "analysis-paths": ["analysis"],
        "test-paths": ["tests"],
        "data-paths": ["data"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "projet_stage_dbt": {
                "staging": {
                    "materialized": "view"
                },
                "marts": {
                    "materialized": "table"
                }
            }
        }
    }
    
    with open(dbt_dir / "dbt_project.yml", "w") as f:
        import yaml
        yaml.dump(dbt_project, f, default_flow_style=False)
    
    # Create profiles.yml (minimal local config)
    profiles_dir = Path.home() / ".dbt"
    profiles_dir.mkdir(exist_ok=True)
    
    profiles = {
        "default": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": "localhost",
                    "user": "airflow",
                    "password": "airflow",
                    "port": 5432,
                    "dbname": "airflow_db",
                    "schema": "dbt_schema",
                    "threads": 4,
                    "keepalives_idle": 0
                }
            }
        }
    }
    
    with open(profiles_dir / "profiles.yml", "w") as f:
        import yaml
        yaml.dump(profiles, f, default_flow_style=False)
    
    # Create model directories
    models_dir = dbt_dir / "models"
    models_dir.mkdir(exist_ok=True)
    (models_dir / "staging").mkdir(exist_ok=True)
    (models_dir / "marts").mkdir(exist_ok=True)
    
    # Create a sample staging model
    staging_model = """-- Staging model: Raw data cleanup
{{ config(
    materialized='view',
    schema='dbt_staging'
) }}

SELECT
    {{ dbt_utils.surrogate_key(['id']) }} as surrogate_key,
    *
FROM {{ source('raw', 'events') }}
WHERE _etl_loaded_at IS NOT NULL
"""
    
    with open(models_dir / "staging" / "stg_events.sql", "w") as f:
        f.write(staging_model)
    
    # Create sources.yml for data catalog
    sources_file = models_dir / "sources.yml"
    sources = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "description": "Raw data sources",
                "tables": [
                    {
                        "name": "events",
                        "description": "Raw events from application",
                        "columns": [
                            {"name": "id", "description": "Event ID"},
                            {"name": "timestamp", "description": "Event timestamp"},
                            {"name": "user_id", "description": "User ID"},
                            {"name": "event_type", "description": "Type of event"}
                        ]
                    }
                ]
            }
        ],
        "models": [
            {
                "name": "stg_events",
                "description": "Staging model for events with data cleaning"
            }
        ]
    }
    
    with open(sources_file, "w") as f:
        import yaml
        yaml.dump(sources, f, default_flow_style=False)
    
    print("✅ dbt project structure créée")
    print(f"   - Répertoire: {dbt_dir}")
    print(f"   - Config profiles: {profiles_dir / 'profiles.yml'}")


if __name__ == "__main__":
    setup_dbt_project()
    print("\n✅ dbt Core + Data Catalog initialisé avec succès!")
