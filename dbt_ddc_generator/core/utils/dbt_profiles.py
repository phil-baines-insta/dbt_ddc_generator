import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class ProfileTarget:
    """Configuration for a dbt profile target."""

    database: str
    schema: str
    type: str
    warehouse: str
    role: str
    threads: int
    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    query_tag: Optional[str] = None


class DbtProfiles:
    """Handles reading and parsing dbt profiles."""

    def __init__(self, dbt_directory: str) -> None:
        """
        Initialize DbtProfiles.

        Args:
            dbt_directory: Root directory of dbt project

        Raises:
            ValueError: If profiles.yml cannot be found
            yaml.YAMLError: If profiles.yml is invalid
        """
        # Load environment variables
        load_dotenv()

        self.profiles_directory = os.getenv("dbt_profiles_directory")
        if not self.profiles_directory:
            error_msg = "Profiles directory not found in environment variables. Please set 'dbt_profiles_directory' in .env file"
            logger.error(error_msg)
            raise ValueError(error_msg)

        self.profiles_path = os.path.join(self.profiles_directory, "profiles.yml")
        if not os.path.exists(self.profiles_path):
            error_msg = f"profiles.yml not found at {self.profiles_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        self.profiles = self._load_profiles()

    def _load_profiles(self) -> Dict[str, Any]:
        """
        Load and parse profiles.yml file.

        Returns:
            Dict containing profile configurations

        Raises:
            yaml.YAMLError: If profiles.yml is invalid
        """
        try:
            with open(self.profiles_path, "r") as f:
                profiles = yaml.safe_load(f)
                logger.debug(f"Successfully loaded profiles from {self.profiles_path}")
                return profiles
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse profiles.yml: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to read profiles.yml: {e}")
            raise

    def get_profile_target(
        self, profile_name: str, env: str = "local"
    ) -> Optional[Dict[str, Any]]:
        """
        Get the target configuration for a profile in a specific environment.

        Args:
            profile_name: Name of the profile (e.g., 'de_finance')
            env: Environment to use ('local', 'dev', or 'prod')

        Returns:
            Optional[Dict]: The target configuration if found

        Raises:
            ValueError: If environment is invalid
        """
        try:
            # Construct environment-specific profile name
            env_profile_name = f"{profile_name}_{env}"

            # Look in the instacart profile's outputs
            instacart_profile = self.profiles.get("instacart", {})
            outputs = instacart_profile.get("outputs", {})

            if env_profile_name in outputs:
                logger.debug(f"Found profile target: {env_profile_name}")
                return outputs[env_profile_name]

            logger.warning(f"Profile target not found: {env_profile_name}")
            return None

        except Exception as e:
            logger.error(f"Error getting profile target: {e}")
            raise

    def get_database_schema(
        self, profile_name: str, env: str = "local"
    ) -> Optional[Tuple[str, str]]:
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
            logger.warning(
                f"No target found for profile '{profile_name}' in environment '{env}'"
            )
            return None

        try:
            database = str(target.get("database", ""))
            schema = str(target.get("schema", ""))
            if not database or not schema:
                return None
            return (database, schema)
        except Exception as e:
            logger.error(f"Error extracting database/schema from target: {e}")
            return None

    def validate_profile_structure(self, profile_name: str, env: str = "local") -> bool:
        """
        Validate the structure of a profile configuration.

        Args:
            profile_name: Name of the profile to validate
            env: Environment to validate

        Returns:
            bool: True if profile structure is valid
        """
        target = self.get_profile_target(profile_name, env)
        if not target:
            return False

        required_fields = {"type", "database", "schema", "warehouse", "role", "threads"}
        return all(field in target for field in required_fields)
