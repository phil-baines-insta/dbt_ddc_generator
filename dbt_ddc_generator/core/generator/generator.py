import logging
import os
from typing import Optional
from dotenv import load_dotenv

from dbt_ddc_generator.core.utils.dbt_model import DbtModel
from dbt_ddc_generator.core.utils.ddc_translator import DDCTranslator
from dbt_ddc_generator.core.utils.git import GitOperations

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

    def write_to_carrot(self, model_name: str, generated_checks: list, branch_name: str) -> None:
        """Write generated checks to carrot repo in a new branch."""
        try:
            # Initialize git operations
            git_ops = GitOperations()

            # Create new branch from master
            git_ops.create_branch_from_master(branch_name)

            # Write files
            carrot_dir = os.getenv("carrot_directory")
            for check in generated_checks:
                check_path = os.path.join(carrot_dir, f"{model_name}_{check['type']}.yml")
                with open(check_path, 'w') as f:
                    f.write(check['content'])
                logger.info(f"Written {check['type']} check to {check_path}")

        except Exception as e:
            logger.error(f"Failed to write checks to carrot repo: {e}")
            raise

    def generate(self, model_name: str, env: str = "local") -> None:
        """Generate documentation and data contracts for a specific dbt model."""
        try:
            # Get model info
            model = DbtModel(self.dbt_directory, model_name)

            # Store generated checks
            generated_checks = []

            # Common configuration
            base_config = {
                "table": model_name,
                "table_fqdn": f"database.schema.{model_name}",
                "schedule_interval": "0 * * * *",
            }

            # Generate checks
            checks = self._generate_checks(model_name, base_config, model)

            return checks  # Return the generated checks instead of printing

        except Exception as e:
            logger.error(f"Error generating DDC: {e}")
            raise

    def _generate_checks(self, model_name: str, base_config: dict, model: DbtModel) -> list:
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
            generated_checks.append({
                'type': 'duplicates',
                'content': self.translator.generate_duplicates_check(duplicates_config)
            })

            # Generate completeness check
            logger.info(f"Generating completeness check for {model_name}")
            completeness_config = {
                **base_config,
                "name": f"{model_name} completeness check",
                "description": f"Check completeness of {model_name}",
                "column_name": model.get_unique_key() or "id",
            }
            generated_checks.append({
                'type': 'completeness',
                'content': self.translator.generate_completeness_check(completeness_config)
            })

            # Generate freshness check
            logger.info(f"Generating freshness check for {model_name}")
            freshness_config = {
                **base_config,
                "name": f"{model_name} freshness check",
                "description": f"Check freshness of {model_name}",
                "column_name": "created_at",  # Default to created_at
                "freshness_interval": "24h",
            }
            generated_checks.append({
                'type': 'freshness',
                'content': self.translator.generate_freshness_check(freshness_config)
            })

            return generated_checks

        except Exception as e:
            logger.error(f"Error generating checks: {e}")
            raise
