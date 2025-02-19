import logging
import os
from typing import Optional, Tuple

from dotenv import load_dotenv

from dbt_ddc_generator.core.utils.cron_converter import CronConverter
from dbt_ddc_generator.core.utils.dbt_model import DbtModel
from dbt_ddc_generator.core.utils.dbt_profiles import DbtProfiles
from dbt_ddc_generator.core.utils.dbt_scheduling import DbtScheduling
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

            self.dbt_directory = dbt_directory  # Assign after validation
            if not os.path.exists(self.dbt_directory):
                raise ValueError(f"DBT directory does not exist: {self.dbt_directory}")

            self.scheduling = DbtScheduling(self.dbt_directory)
            self.profiles = DbtProfiles(self.dbt_directory)
            self.translator = DDCTranslator(self.dbt_directory)
            self.cron_converter = CronConverter()

            logger.info("Generator initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Generator: {e}")
            raise

    def _get_model_info(self, model_name: str) -> Optional[Tuple[DbtModel, str]]:
        """Get model information and unique key."""
        try:
            model = DbtModel(self.dbt_directory, model_name)
            unique_key = model.get_unique_key()
            if not unique_key:
                logger.error(f"No unique_key found for model: {model_name}")
                return None
            return model, unique_key
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return None

    def generate(self, model_name: str, env: str = "local") -> None:
        """
        Generate documentation and data contracts for a specific dbt model.

        Args:
            model_name: Name of the model to generate DDC for
            env: Target environment (local, dev, prod)
        """
        try:
            # Get model info
            model_info = self._get_model_info(model_name)
            if not model_info:
                return
            model, unique_key = model_info

            # Get pipeline and profile info
            pipeline_config = self.scheduling.find_pipeline_config(model_name)
            if not pipeline_config:
                logger.error(f"No pipeline configuration found for model: {model_name}")
                return

            profile_name = pipeline_config["profile"]
            if not profile_name:
                logger.error("No profile name found in pipeline configuration")
                return

            # Get database and schema
            db_schema = self.profiles.get_database_schema(profile_name, env)
            if not db_schema:
                logger.error(
                    f"No database/schema found for profile '{profile_name}' in environment '{env}'"
                )
                return

            database, schema = db_schema

            # Generate checks
            self._generate_checks(
                model_name, unique_key, database, schema, pipeline_config, model
            )

        except Exception as e:
            logger.error(f"Error generating DDC: {e}")
            raise

    def _generate_checks(
        self,
        model_name: str,
        unique_key: str,
        database: str,
        schema: str,
        pipeline_config: dict,
        model: DbtModel,
    ) -> None:
        """Generate and output DDC checks."""
        try:
            # Common configuration
            base_config = {
                "table": f"{database}.{schema}.{model_name}",
                "table_fqdn": f"{database}.{schema}.{model_name}",
                "schedule_interval": CronConverter.to_hourly(
                    pipeline_config["schedule"]
                ),
            }

            # Generate duplicates check
            duplicates_config = {
                **base_config,
                "name": f"{database.lower()}.{schema.lower()}.{model_name} duplicate check",
                "description": f"Check for duplicate {unique_key} in {model_name}",
                "column_name": unique_key,
            }

            print(self.translator.generate_duplicates_check(duplicates_config))
            print("\n---\n")

            # Generate freshness check using detected timestamp columns
            timestamp_columns = model.get_timestamp_columns()
            if timestamp_columns:
                freshness_config = {
                    **base_config,
                    "name": f"{database.lower()}.{schema.lower()}.{model_name} freshness check",
                    "description": f"Check freshness of {model_name}",
                    "column_name": timestamp_columns[
                        0
                    ],  # Use first detected timestamp column
                    "freshness_interval": "24h",
                }
                print(self.translator.generate_freshness_check(freshness_config))
            else:
                logger.warning(
                    f"No timestamp columns found for freshness check in model: {model_name}"
                )

        except Exception as e:
            logger.error(f"Error generating checks: {e}")
            raise
