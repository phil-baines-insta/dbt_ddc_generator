import logging
import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration extracted from a dbt model."""

    unique_key: Optional[str] = None
    timestamp_columns: List[str] = field(default_factory=list)
    materialization: Optional[str] = None
    schema: Optional[str] = None


class DbtModel:
    """Handles parsing and extracting information from dbt model files."""

    def __init__(self, dbt_directory: str, model_name: str) -> None:
        """
        Initialize DbtModel with dbt project directory and model name.

        Args:
            dbt_directory: Root directory of dbt project
            model_name: Name of the model (e.g., 'fact_tax_hist')

        Raises:
            ValueError: If model file cannot be found
        """
        self.model_name = model_name
        self.models_dir = os.path.join(dbt_directory, "models")
        self.model_path = self._find_model_file()
        self.model_content = self._read_model_file()
        self.config = self._parse_model_config()

    def _find_model_file(self) -> str:
        """
        Find the SQL file for this model in the dbt project.

        Returns:
            str: Full path to the model file

        Raises:
            ValueError: If model file cannot be found
        """
        for root, _, files in os.walk(self.models_dir):
            if f"{self.model_name}.sql" in files:
                return os.path.join(root, f"{self.model_name}.sql")

        error_msg = f"Model '{self.model_name}' not found in {self.models_dir}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    def _read_model_file(self) -> str:
        """
        Read the contents of the model file.

        Returns:
            str: Contents of the model file

        Raises:
            IOError: If file cannot be read
        """
        try:
            with open(self.model_path, "r") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Failed to read model file {self.model_path}: {e}")
            raise

    def _parse_model_config(self) -> ModelConfig:
        """
        Parse the model file for configuration.

        Returns:
            ModelConfig: Parsed configuration from the model
        """
        config = ModelConfig()

        # Find config block
        pattern = r"{{\s*config\s*\(([^}]+)\s*\)\s*}}"
        config_matches = re.finditer(pattern, self.model_content)

        for match in config_matches:
            config_content = match.group(1)

            # Extract unique_key
            unique_key_pattern = r"unique_key\s*=\s*['\"]([^'\"]+)['\"]"
            unique_key_match = re.search(unique_key_pattern, config_content)
            if unique_key_match:
                config.unique_key = unique_key_match.group(1)

            # Extract materialization
            materialization_pattern = r"materialized\s*=\s*['\"]([^'\"]+)['\"]"
            materialization_match = re.search(materialization_pattern, config_content)
            if materialization_match:
                config.materialization = materialization_match.group(1)

        # Find timestamp columns
        config.timestamp_columns = self._find_timestamp_columns()

        return config

    def _find_timestamp_columns(self) -> List[str]:
        """
        Find potential timestamp columns in the model SQL.

        Returns:
            List[str]: List of column names that appear to be timestamps
        """
        # Common timestamp column patterns
        timestamp_patterns = [r"\b\w+_at\b", r"\b\w+_timestamp\b", r"\b\w+_date\b"]

        columns: List[str] = []
        for pattern in timestamp_patterns:
            matches = re.finditer(pattern, self.model_content, re.IGNORECASE)
            columns.extend(match.group(0) for match in matches)

        return list(set(columns))

    def get_unique_key(self) -> Optional[str]:
        """
        Get the unique_key from model config.

        Returns:
            Optional[str]: The unique_key if found, None otherwise
        """
        return self.config.unique_key

    def get_timestamp_columns(self) -> List[str]:
        """
        Get potential timestamp columns from the model.

        Returns:
            List[str]: List of column names that appear to be timestamps
        """
        return self.config.timestamp_columns

    def __str__(self) -> str:
        return f"DbtModel({self.model_name})"
