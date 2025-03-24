import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration extracted from a dbt model."""

    unique_key: Optional[str] = None


class DbtModel:
    """Handles parsing and extracting information from dbt model files."""

    def __init__(self, dbt_directory: str, model_name: str) -> None:
        """Initialize DbtModel with dbt project directory and model name."""
        try:
            self.model_name = model_name
            self.dbt_directory = dbt_directory
            logger.info(f"Initializing DbtModel for {model_name}")

            # Handle model name that might include the full path
            if "/" in model_name:
                # If model_name includes path, use it directly
                model_file = os.path.join(self.dbt_directory, "models", f"{model_name}.sql")
            else:
                # Otherwise search for it in models directory
                model_file = None
                models_dir = os.path.join(self.dbt_directory, "models")
                for root, _, files in os.walk(models_dir):
                    if f"{model_name}.sql" in files:
                        model_file = os.path.join(root, f"{model_name}.sql")
                        break

            if not model_file or not os.path.exists(model_file):
                raise ValueError(f"Model file not found for: {model_name}")

            with open(model_file, "r") as f:
                self.model_content = f.read()

            self.config = self._parse_model_config()
        except Exception as e:
            logger.error(f"Failed to initialize DbtModel: {e}")
            raise

    def _parse_model_config(self) -> ModelConfig:
        """Parse the model file for configuration."""
        try:
            logger.debug(f"Parsing config for model {self.model_name}")
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

            logger.debug(f"Parsed config: {config}")
            return config
        except Exception as e:
            logger.error(f"Failed to parse model config: {e}")
            raise

    def get_unique_key(self) -> Optional[str]:
        """Get the unique_key from model config."""
        logger.debug(f"Getting unique key for model {self.model_name}")
        return self.config.unique_key
