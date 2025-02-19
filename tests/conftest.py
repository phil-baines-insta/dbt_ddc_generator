from typing import Dict

import pytest


@pytest.fixture
def sample_dbt_directory(tmp_path) -> str:
    """Create a temporary dbt project structure."""
    # Create basic dbt project structure
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    # Create a sample model file
    model_content = """
    {{
        config(
            unique_key='id'
        )
    }}

    SELECT * FROM some_table
    """
    (models_dir / "fact_test.sql").write_text(model_content)

    return str(tmp_path)


@pytest.fixture
def sample_pipeline_yml() -> Dict:
    """Sample pipeline.yml content."""
    return {
        "owner": "test.user",
        "profile": "finance_data_mart",
        "models": [{"name": "fact_test"}, {"name": "dim_test"}],
    }


@pytest.fixture
def sample_profiles_yml(tmp_path) -> str:
    """Create a sample profiles.yml file."""
    profiles_content = {
        "instacart": {
            "outputs": {
                "finance_data_mart_prod": {
                    "type": "snowflake",
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "warehouse": "TEST_WH",
                    "role": "TEST_ROLE",
                    "threads": 4,
                }
            }
        }
    }

    import yaml

    profiles_file = tmp_path / "profiles.yml"
    with open(profiles_file, "w") as f:
        yaml.dump(profiles_content, f)

    return str(tmp_path)


@pytest.fixture
def sample_templates(tmp_path) -> str:
    """Create sample template files."""
    template_dir = tmp_path / "templates"
    template_dir.mkdir()

    # Create duplicates template
    duplicates_content = """formatVersion: 1
name: {{ name }}
description: {{ description }}
system: data-eng-finance-checks
every: 24h
mode: list
source: finance-snowflake
fail_on: non-zero
timeout: 20m
priority: P3
query: |
  select
    {{ column_name }},
    count(1)
  from {{ table_fqdn }}
  group by {{ column_name }}
  having count(1) > 1
annotations:
  data_sets:
    - {{ table_fqdn }}
compliance:
  sox: {}
"""
    (template_dir / "duplicates.yml").write_text(duplicates_content)

    # Create completeness template
    completeness_content = """formatVersion: 1
name: {{ name }}
description: {{ description }}
system: data-eng-finance-checks
every: 24h
mode: list
source: finance-snowflake
fail_on: non-zero
timeout: 20m
priority: P3
query: |
  with src as (
    select
      count(1) as source_count
    from {{ '{{ source_table }}' }}
    where {{ '{{ source_date_column }}' }} >= current_timestamp - interval '7 days'
  ),

  target as (
    select
      count(1) as target_count
    from {{ table_fqdn }}
    where {{ target_date_column }} >= current_timestamp - interval '7 days'
  )

  select
    src.source_count,
    target.target_count,
    (src.source_count - target.target_count) / src.source_count * 100 as percent_diff
  from src
  cross join target
annotations:
  data_sets:
    - {{ table_fqdn }}
compliance:
  sox: {}
"""
    (template_dir / "completeness.yml").write_text(completeness_content)

    # Create freshness template
    freshness_content = """formatVersion: 1
name: {{ name }}
description: {{ description }}
system: data-eng-finance-checks
every: 24h
mode: list
source: finance-snowflake
fail_on: zero
timeout: 20m
priority: P3
query: |
  select
    true
  from {{ table_fqdn }}
  where {{ column_name }} > current_timestamp - interval '{{ freshness_interval }}'
  limit 1
annotations:
  data_sets:
    - {{ table_fqdn }}
compliance:
  sox: {}
"""
    (template_dir / "freshness.yml").write_text(freshness_content)

    return str(template_dir)
