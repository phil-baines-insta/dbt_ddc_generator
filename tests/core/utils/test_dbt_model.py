import pytest

from dbt_ddc_generator.core.utils.dbt_model import DbtModel


def test_get_unique_key(sample_dbt_directory):
    """Test extracting unique_key from model config."""
    model = DbtModel(sample_dbt_directory, "fact_test")
    assert model.get_unique_key() == "id"


def test_model_not_found(sample_dbt_directory):
    """Test error when model file not found."""
    with pytest.raises(ValueError, match="Model file not found"):
        DbtModel(sample_dbt_directory, "nonexistent_model")
