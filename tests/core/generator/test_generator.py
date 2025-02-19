import os

import yaml

from dbt_ddc_generator.core.generator.generator import Generator


def test_generate_checks(
    monkeypatch, sample_dbt_directory, sample_profiles_yml, sample_pipeline_yml
):
    """Test generating all check types for a model."""
    # Mock environment variables
    monkeypatch.setenv("instacart_dbt_directory", sample_dbt_directory)
    monkeypatch.setenv("dbt_profiles_directory", sample_profiles_yml)

    # Create scheduling directory with pipeline.yml
    scheduling_dir = os.path.join(sample_dbt_directory, "scheduling")
    os.makedirs(scheduling_dir)

    with open(os.path.join(scheduling_dir, "pipeline.yml"), "w") as f:
        yaml.dump(sample_pipeline_yml, f)

    generator = Generator()
    checks = generator.generate("fact_test", "prod")

    assert len(checks) == 3  # Should generate all three check types
    check_types = {check["type"] for check in checks}
    assert check_types == {"duplicates", "completeness", "freshness"}
