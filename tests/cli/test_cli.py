import os

import yaml
from click.testing import CliRunner

from dbt_ddc_generator.cli.cli import generate, version


def test_version_command():
    """Test version command output."""
    runner = CliRunner()
    result = runner.invoke(version)
    assert result.exit_code == 0
    assert "dbt-ddc-generator version" in result.output


def test_generate_command(
    monkeypatch, sample_dbt_directory, sample_profiles_yml, sample_pipeline_yml
):
    """Test generate command with sample model."""
    runner = CliRunner()

    # Mock environment variables
    monkeypatch.setenv("instacart_dbt_directory", sample_dbt_directory)
    monkeypatch.setenv("dbt_profiles_directory", sample_profiles_yml)

    # Create scheduling directory with pipeline.yml
    scheduling_dir = os.path.join(sample_dbt_directory, "scheduling")
    os.makedirs(scheduling_dir)

    with open(os.path.join(scheduling_dir, "pipeline.yml"), "w") as f:
        yaml.dump(sample_pipeline_yml, f)

    result = runner.invoke(generate, ["fact_test", "--env", "prod"])
    assert result.exit_code == 0
