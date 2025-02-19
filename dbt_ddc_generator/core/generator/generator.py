import logging
import os
from typing import Optional
from dotenv import load_dotenv

from dbt_ddc_generator.core.utils.dbt_model import DbtModel
from dbt_ddc_generator.core.utils.ddc_translator import DDCTranslator

logger = logging.getLogger(__name__)

class Generator:
    """Main class for generating DDC files."""

    def __init__(self) -> None:
        """Initialize Generator with required components."""
        try:
            load_dotenv()

            dbt_directory = os.getenv("instacart_dbt_directory")
            if not dbt_directory:
                raise ValueError("DBT directory not found in environment variables")

            self.dbt_directory = dbt_directory
            if not os.path.exists(self.dbt_directory):
                raise ValueError(f"DBT directory does not exist: {self.dbt_directory}")

            self.translator = DDCTranslator(self.dbt_directory)

        except Exception as e:
            logger.error(f"Failed to initialize Generator: {e}")
            raise

    def generate(self, model_name: str, env: str = "local") -> None:
        """Generate documentation and data contracts for a specific dbt model."""
        try:
            # Get model info
            model = DbtModel(self.dbt_directory, model_name)

            # Common configuration
            base_config = {
                "table": model_name,
                "table_fqdn": f"database.schema.{model_name}",  # This would need to be configured properly
                "schedule_interval": "0 * * * *",  # Hourly by default
            }

            # Generate checks
            self._generate_checks(model_name, base_config, model)

        except Exception as e:
            logger.error(f"Error generating DDC: {e}")
            raise

    def _generate_checks(self, model_name: str, base_config: dict, model: DbtModel) -> None:
        """Generate and output DDC checks."""
        try:
            # Generate duplicates check
            logger.info(f"Generating duplicates check for {model_name}")
            duplicates_config = {
                **base_config,
                "name": f"{model_name} duplicate check",
                "description": f"Check for duplicates in {model_name}",
                "column_name": model.get_unique_key() or "id",
            }
            print(self.translator.generate_duplicates_check(duplicates_config))
            print("\n---\n")

            # Generate completeness check
            logger.info(f"Generating completeness check for {model_name}")
            completeness_config = {
                **base_config,
                "name": f"{model_name} completeness check",
                "description": f"Check completeness of {model_name}",
                "column_name": model.get_unique_key() or "id",
            }
            print(self.translator.generate_completeness_check(completeness_config))
            print("\n---\n")

            # Generate freshness check
            logger.info(f"Generating freshness check for {model_name}")
            freshness_config = {
                **base_config,
                "name": f"{model_name} freshness check",
                "description": f"Check freshness of {model_name}",
                "column_name": "created_at",  # Default to created_at
                "freshness_interval": "24h",
            }
            print(self.translator.generate_freshness_check(freshness_config))

        except Exception as e:
            logger.error(f"Error generating checks: {e}")
            raise
