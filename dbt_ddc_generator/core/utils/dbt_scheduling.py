import logging
import os
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)

class DbtScheduling:
    def __init__(self, dbt_directory: str):
        self.dbt_directory = dbt_directory
        self.scheduling_dir = os.path.join(self.dbt_directory, "scheduling")
        logger.info(f"Initialized DbtScheduling with directory: {self.scheduling_dir}")

        if not os.path.exists(self.scheduling_dir):
            raise ValueError(f"Scheduling directory not found in {self.dbt_directory}")

    def find_pipeline_config(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Find and parse the pipeline.yml file that contains the specified model.

        Args:
            model_name: The name of the dbt model to find scheduling config for

        Returns:
            Optional[Dict]: The pipeline configuration for the model if found, None otherwise
        """
        logger.info(f"Searching for pipeline config for model: {model_name}")

        # Walk through all yml files in scheduling directory
        for root, _, files in os.walk(self.scheduling_dir):
            for file in files:
                if file.endswith("pipeline.yml"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r") as f:
                            pipeline_config = yaml.safe_load(f)

                            # Check if this pipeline contains our model
                            if pipeline_config and isinstance(pipeline_config, dict):
                                # Look through models list
                                for model in pipeline_config.get("models", []):
                                    if model.get("name") == model_name:
                                        profile = pipeline_config.get("profile")
                                        return {
                                            "deploy_profile": profile,
                                            "file_path": file_path,
                                            "pipeline_name": os.path.basename(root),
                                            "model_config": model,
                                        }
                    except yaml.YAMLError as e:
                        logger.error(f"Error parsing {file_path}: {e}")
                    except Exception as e:
                        logger.error(f"Error reading {file_path}: {e}")

        return None
