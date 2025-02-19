import pytest

from dbt_ddc_generator.core.utils.dbt_profiles import DbtProfiles


@pytest.fixture
def sample_profiles_yml(tmp_path):
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


def test_get_database_schema(
    monkeypatch, sample_dbt_directory, sample_profiles_yml, sample_pipeline_yml
):
    """Test getting database and schema from profile."""
    monkeypatch.setenv("dbt_profiles_directory", sample_profiles_yml)

    # Create scheduling directory with pipeline.yml
    import os

    scheduling_dir = os.path.join(sample_dbt_directory, "scheduling")
    os.makedirs(scheduling_dir)

    import yaml

    with open(os.path.join(scheduling_dir, "pipeline.yml"), "w") as f:
        yaml.dump(sample_pipeline_yml, f)

    profiles = DbtProfiles(sample_dbt_directory)
    db_schema = profiles.get_database_schema("fact_test", "prod")

    assert db_schema is not None
    database, schema = db_schema
    assert database == "TEST_DB"
    assert schema == "TEST_SCHEMA"
