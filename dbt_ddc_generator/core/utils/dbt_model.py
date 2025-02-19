import os
import re
from typing import Optional

class DbtModel:
    def __init__(self, dbt_directory: str, model_name: str):
        """
        Initialize DbtModel with dbt project directory and model name.

        Args:
            dbt_directory: Root directory of dbt project
            model_name: Name of the model (e.g., 'fact_tax_hist')
        """
        self.model_name = model_name

        # Find the model file
        models_dir = os.path.join(dbt_directory, 'models')
        self.model_path = None

        for root, _, files in os.walk(models_dir):
            if f"{model_name}.sql" in files:
                self.model_path = os.path.join(root, f"{model_name}.sql")
                break

        if not self.model_path:
            raise ValueError(f"Model '{model_name}' not found in {models_dir}")

        # Read and parse the model file
        with open(self.model_path, 'r') as f:
            self.model_content = f.read()

    def get_unique_key(self) -> Optional[str]:
        """
        Extract the unique_key from model config.
        Returns the unique_key if found, None otherwise.

        Example matches:
            {{ config(unique_key='order_id') }}
            {{config(unique_key = "user_id")}}
            {{ config(
                unique_key = 'customer_id',
                ...
            )}}
        """
        # Pattern to match config block with unique_key
        pattern = r'{{\s*config\s*\(([^}]+)\s*\)\s*}}'
        config_matches = re.finditer(pattern, self.model_content)

        for match in config_matches:
            config_content = match.group(1)
            # Look for unique_key in config parameters
            unique_key_pattern = r"unique_key\s*=\s*['\"]([^'\"]+)['\"]"
            unique_key_match = re.search(unique_key_pattern, config_content)

            if unique_key_match:
                return unique_key_match.group(1)

        return None

    def __str__(self) -> str:
        return f"DbtModel({self.model_path})"
