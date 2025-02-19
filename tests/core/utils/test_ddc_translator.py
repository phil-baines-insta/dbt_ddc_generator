from dbt_ddc_generator.core.utils.ddc_translator import DDCTranslator


def test_generate_duplicates_check(sample_dbt_directory, sample_templates):
    """Test generating duplicates check yaml."""
    translator = DDCTranslator(sample_dbt_directory)
    translator.template_dir = sample_templates  # Override template directory
    config = {
        "name": "test check",
        "description": "test description",
        "table": "fact_test",
        "column_name": "id",
        "table_fqdn": "db.schema.fact_test",
    }

    yaml_content = translator.generate_duplicates_check(config)
    assert "name: test check" in yaml_content
    assert "select\n    id,\n    count(1)" in yaml_content  # Check rendered query
    assert "from db.schema.fact_test" in yaml_content


def test_generate_completeness_check(sample_dbt_directory, sample_templates):
    """Test generating completeness check yaml."""
    translator = DDCTranslator(sample_dbt_directory)
    translator.template_dir = sample_templates  # Override template directory
    config = {
        "name": "test check",
        "description": "test description",
        "table": "fact_test",
        "column_name": "id",
        "table_fqdn": "db.schema.fact_test",
    }

    yaml_content = translator.generate_completeness_check(config)
    assert "name: test check" in yaml_content
    assert "from db.schema.fact_test" in yaml_content


def test_generate_freshness_check(sample_dbt_directory, sample_templates):
    """Test generating freshness check yaml."""
    translator = DDCTranslator(sample_dbt_directory)
    translator.template_dir = sample_templates  # Override template directory
    config = {
        "name": "test check",
        "description": "test description",
        "table": "fact_test",
        "column_name": "created_at",
        "table_fqdn": "db.schema.fact_test",
        "freshness_interval": "24h",
    }

    yaml_content = translator.generate_freshness_check(config)
    assert "name: test check" in yaml_content
    assert "interval '24h'" in yaml_content
