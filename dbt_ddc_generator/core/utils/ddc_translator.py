import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional

from jinja2 import Template

logger = logging.getLogger(__name__)


@dataclass
class CheckConfig:
    """Configuration for generating checks."""

    name: str
    description: str
    schedule_interval: str
    table: str
    column_name: str
    table_fqdn: str
    freshness_interval: Optional[str] = None


class DDCTranslator:
    """Handles translation of configurations into DDC YAML files."""

    def __init__(self, dbt_directory: str) -> None:
        """
        Initialize DDCTranslator.

        Args:
            dbt_directory: Root directory of dbt project

        Raises:
            FileNotFoundError: If template files cannot be found
        """
        self.dbt_directory = dbt_directory
        self.template_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "templates"
        )

        try:
            self.freshness_template = self._load_template("freshness.yml")
            self.duplicates_template = self._load_template("duplicates.yml")
            self.completeness_template = self._load_template("completeness.yml")
        except FileNotFoundError as e:
            logger.error(f"Failed to load templates: {e}")
            raise

    def _load_template(self, template_name: str) -> Template:
        """
        Load and return a Jinja2 template.

        Args:
            template_name: Name of the template file

        Returns:
            Loaded Jinja2 template

        Raises:
            FileNotFoundError: If template file doesn't exist
        """
        template_path = os.path.join(self.template_dir, template_name)
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template not found: {template_path}")

        try:
            with open(template_path, "r") as f:
                return Template(f.read())
        except Exception as e:
            logger.error(f"Failed to read template {template_name}: {e}")
            raise

    def _validate_config(self, config: Dict) -> None:
        """
        Validate check configuration and normalize values.

        Args:
            config: Configuration dictionary

        Raises:
            ValueError: If required fields are missing
        """
        required_fields = [
            "name",
            "description",
            "table",
            "column_name",
            "table_fqdn",
        ]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

        # Convert all string values to lowercase
        for key, value in config.items():
            if isinstance(value, str):
                config[key] = value.lower()

    def generate_duplicates_check(self, config: Dict) -> str:
        """
        Generate a duplicates check YAML configuration.

        Args:
            config: Check configuration dictionary

        Returns:
            Rendered YAML configuration

        Raises:
            ValueError: If configuration is invalid
        """
        try:
            self._validate_config(config)
            config = {
                k: v.lower() if isinstance(v, str) else v for k, v in config.items()
            }
            return self.duplicates_template.render(**config)
        except Exception as e:
            logger.error(f"Failed to generate duplicates check: {e}")
            raise

    def generate_freshness_check(self, config: Dict) -> str:
        """
        Generate a freshness check YAML configuration.

        Args:
            config: Check configuration dictionary

        Returns:
            Rendered YAML configuration

        Raises:
            ValueError: If configuration is invalid
        """
        try:
            self._validate_config(config)
            if "freshness_interval" not in config:
                raise ValueError("freshness_interval is required for freshness checks")

            config = {
                k: v.lower() if isinstance(v, str) else v for k, v in config.items()
            }
            return self.freshness_template.render(**config)
        except Exception as e:
            logger.error(f"Failed to generate freshness check: {e}")
            raise

    def generate_completeness_check(self, config: Dict) -> str:
        """Generate a completeness check YAML configuration."""
        try:
            self._validate_config(config)

            # Add target-specific fields
            config["target_table"] = config["table"]
            config["target_date_column"] = config.get("date_column", "created_at")

            return self.completeness_template.render(**config)
        except Exception as e:
            logger.error(f"Failed to generate completeness check: {e}")
            raise

    def write_check_to_file(self, yaml_content: str, output_path: str) -> None:
        """
        Write generated YAML to file.

        Args:
            yaml_content: Generated YAML content
            output_path: Path to write the file

        Raises:
            IOError: If writing to file fails
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as f:
                f.write(yaml_content)
            logger.info(f"Successfully wrote check to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write check to {output_path}: {e}")
            raise

    def create_table_fqdn(self, database: str, schema: str, table: str) -> str:
        """Create a fully qualified table name for annotations."""
        return f"{database.lower()}.{schema.lower()}.{table.lower()}"
