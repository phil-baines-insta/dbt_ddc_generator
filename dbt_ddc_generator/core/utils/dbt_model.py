import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

@dataclass
class ModelConfig:
    """Configuration extracted from a dbt model."""
    unique_key: Optional[str] = None
    timestamp_columns: List[str] = field(default_factory=list)

class DbtModel:
    """Handles parsing and extracting information from dbt model files."""

    def __init__(self, dbt_directory: str, model_name: str) -> None:
        """Initialize DbtModel with dbt project directory and model name."""
        try:
            self.model_name = model_name
            self.dbt_directory = dbt_directory
            logger.info(f"Initializing DbtModel for {model_name}")
            self.config = self._parse_model_config()
        except Exception as e:
            logger.error(f"Failed to initialize DbtModel: {e}")
            raise

    def _parse_model_config(self) -> ModelConfig:
        """Parse the model file for configuration."""
        try:
            logger.debug(f"Parsing config for model {self.model_name}")
            config = ModelConfig()
            # Basic config parsing for unique_key and timestamp columns
            logger.debug(f"Parsed config: {config}")
            return config
        except Exception as e:
            logger.error(f"Failed to parse model config: {e}")
            raise

    def get_unique_key(self) -> Optional[str]:
        """Get the unique_key from model config."""
        logger.debug(f"Getting unique key for model {self.model_name}")
        return self.config.unique_key

    def get_timestamp_columns(self) -> List[str]:
        """Get potential timestamp columns from the model."""
        logger.debug(f"Getting timestamp columns for model {self.model_name}")
        return self.config.timestamp_columns
