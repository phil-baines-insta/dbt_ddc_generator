import os
from dotenv import load_dotenv
import yaml
from typing import Optional, Dict, Tuple

class DbtProfiles:
    def __init__(self, dbt_directory: str):
        # Load environment variables from .env file
        load_dotenv()

        # Get the profiles directory from environment variables
        self.profiles_directory = os.getenv('dbt_profiles_directory')
        if not self.profiles_directory:
            raise ValueError("Profiles directory not found in environment variables. Please set 'dbt_profiles_directory' in .env file")

        self.profiles_path = os.path.join(self.profiles_directory, 'profiles.yml')

        if not os.path.exists(self.profiles_path):
            raise ValueError(f"profiles.yml not found at {self.profiles_path}")

        # Load and parse profiles.yml
        with open(self.profiles_path, 'r') as f:
            self.profiles = yaml.safe_load(f)

    def get_profile_target(self, profile_name: str, env: str = 'local') -> Optional[Dict]:
        """
        Get the target configuration for a profile in a specific environment.

        Args:
            profile_name: Name of the profile (e.g., 'de_finance')
            env: Environment to use ('local', 'dev', or 'prod')

        Returns:
            Optional[Dict]: The target configuration if found
        """
        # Construct environment-specific profile name
        env_profile_name = f"{profile_name}_{env}"

        # Look in the instacart profile's outputs
        instacart_profile = self.profiles.get('instacart', {})
        outputs = instacart_profile.get('outputs', {})

        # Try to find the environment-specific profile in outputs
        if env_profile_name in outputs:
            return outputs[env_profile_name]

        return None

    def get_database_schema(self, profile_name: str, env: str = 'local') -> Optional[Tuple[str, str]]:
        """
        Get the database and schema for a profile in a specific environment.

        Args:
            profile_name: Name of the profile (e.g., 'de_finance')
            env: Environment to use ('local', 'dev', or 'prod')

        Returns:
            Optional[Tuple[str, str]]: Tuple of (database, schema) if found
        """
        target = self.get_profile_target(profile_name, env)
        if not target:
            return None

        return (target.get('database'), target.get('schema'))
