import logging
import os

from dotenv import load_dotenv

from dbt_ddc_generator.core.utils.dbt_model import DbtModel
from dbt_ddc_generator.core.utils.dbt_profiles import DbtProfiles
from dbt_ddc_generator.core.utils.ddc_translator import DDCTranslator

logger = logging.getLogger(__name__)


class Generator:
    """Main class for generating DDC files."""

    def __init__(self) -> None:
        """Initialize Generator with required components."""
        try:
            # Get the project root directory (where .env is located)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            env_path = os.path.join(project_root, ".env")

            if not os.path.exists(env_path):
                raise ValueError(f".env file not found at {env_path}")

            load_dotenv(env_path)

            self.dbt_directory = os.getenv("instacart_dbt_directory")
            if not self.dbt_directory:
                raise ValueError("DBT directory not found in environment variables")

            if not os.path.exists(self.dbt_directory):
                raise ValueError(f"DBT directory does not exist: {self.dbt_directory}")

            self.translator = DDCTranslator(self.dbt_directory)
            self.profiles = DbtProfiles(self.dbt_directory)

        except Exception as e:
            logger.error(f"Failed to initialize Generator: {e}")
            raise

    def generate(self, model_name: str, env: str = "local") -> list:
        """Generate Declarative Data Checks for a specific dbt model."""
        try:
            if not self.dbt_directory:  # Add validation
                raise ValueError("DBT directory not initialized")

            model = DbtModel(self.dbt_directory, model_name)

            # Get database and schema from profile
            db_schema = self.profiles.get_database_schema(model_name, env)
            if not db_schema:
                raise ValueError(
                    f"No database/schema found for model '{model_name}' in environment '{env}'"
                )

            database, schema = db_schema

            # Common configuration
            base_config = {
                "table": model_name,
                "table_fqdn": f"{database}.{schema}.{model_name}",
            }

            return self._generate_checks(model_name, base_config, model)

        except Exception as e:
            logger.error(f"Error generating DDC: {e}")
            raise

    def _generate_checks(
        self, model_name: str, base_config: dict, model: DbtModel
    ) -> list:
        """Generate and output DDC checks."""
        try:
            generated_checks = []

            # Generate duplicates check
            logger.info(f"Generating duplicates check for {model_name}")
            duplicates_config = {
                **base_config,
                "name": f"{model_name} duplicate check",
                "description": f"Check for duplicates in {model_name}",
                "column_name": model.get_unique_key() or "id",
            }
            generated_checks.append(
                {
                    "type": "duplicates",
                    "content": self.translator.generate_duplicates_check(
                        duplicates_config
                    ),
                }
            )

            # Generate completeness check
            logger.info(f"Generating completeness check for {model_name}")
            completeness_config = {
                **base_config,
                "name": f"{model_name} completeness check",
                "description": f"Check completeness of {model_name}",
                "column_name": model.get_unique_key() or "id",
            }
            generated_checks.append(
                {
                    "type": "completeness",
                    "content": self.translator.generate_completeness_check(
                        completeness_config
                    ),
                }
            )

            # Generate freshness check
            logger.info(f"Generating freshness check for {model_name}")
            freshness_config = {
                **base_config,
                "name": f"{model_name} freshness check",
                "description": f"Check freshness of {model_name}",
                "column_name": "etl_created_date_time_utc",  # Default to etl_created_date_time_utc
                "freshness_interval": "24h",
            }
            generated_checks.append(
                {
                    "type": "freshness",
                    "content": self.translator.generate_freshness_check(
                        freshness_config
                    ),
                }
            )

            return generated_checks

        except Exception as e:
            logger.error(f"Error generating checks: {e}")
            raise
