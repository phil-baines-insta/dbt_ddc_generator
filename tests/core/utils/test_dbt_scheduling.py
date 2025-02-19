import os

from dbt_ddc_generator.core.utils.dbt_scheduling import DbtScheduling


def test_find_pipeline_config(sample_dbt_directory, sample_pipeline_yml, tmp_path):
    """Test finding pipeline config for a model."""
    # Create scheduling directory with pipeline.yml
    scheduling_dir = tmp_path / "scheduling"
    scheduling_dir.mkdir()
    pipeline_file = scheduling_dir / "pipeline.yml"

    import yaml

    with open(pipeline_file, "w") as f:
        yaml.dump(sample_pipeline_yml, f)

    scheduling = DbtScheduling(str(tmp_path))
    config = scheduling.find_pipeline_config("fact_test")

    assert config is not None
    assert config["deploy_profile"] == "finance_data_mart"


def test_pipeline_config_not_found(sample_dbt_directory):
    """Test when pipeline config not found for model."""
    # Create scheduling directory
    scheduling_dir = os.path.join(sample_dbt_directory, "scheduling")
    os.makedirs(scheduling_dir)

    scheduling = DbtScheduling(sample_dbt_directory)
    assert scheduling.find_pipeline_config("nonexistent_model") is None
