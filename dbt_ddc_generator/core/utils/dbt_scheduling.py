import os
from typing import Any, Dict, Optional

import yaml


class DbtScheduling:
    def __init__(self, dbt_directory: str):
        self.dbt_directory = dbt_directory
        self.scheduling_dir = os.path.join(self.dbt_directory, "scheduling")

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
                                        return {
                                            "file_path": file_path,
                                            "pipeline_name": os.path.basename(
                                                root
                                            ),  # Using directory name as pipeline name
                                            "schedule": pipeline_config.get(
                                                "schedule_interval"
                                            ),
                                            "owner": pipeline_config.get("owner"),
                                            "description": pipeline_config.get(
                                                "description"
                                            ),
                                            "model_config": model,
                                            "profile": pipeline_config.get("profile"),
                                            "notify_on_failure": pipeline_config.get(
                                                "notify_on_failure"
                                            ),
                                            "tags": pipeline_config.get("tags"),
                                            "deploy_env": pipeline_config.get(
                                                "deploy_env"
                                            ),
                                        }
                    except yaml.YAMLError as e:
                        print(f"Error parsing {file_path}: {str(e)}")
                    except Exception as e:
                        print(f"Error reading {file_path}: {str(e)}")

        return None
