import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import yaml
from dotenv import load_dotenv

from dbt_ddc_generator.core.utils.dbt_scheduling import DbtScheduling

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
        self.dbt_directory = dbt_directory
        self.scheduling = DbtScheduling(dbt_directory)

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

    def get_deploy_profile_from_schedule(self, model_name: str) -> Optional[str]:
        """Get deploy profile from model's schedule file."""
        try:
            # Look in scheduling directory for model's schedule file
            schedule_dir = os.path.join(self.dbt_directory, "scheduling")
            logger.info(f"Searching for pipeline.yml in directory: {schedule_dir}")

            # Walk through scheduling directory to find model's schedule file
            for root, _, files in os.walk(schedule_dir):
                logger.debug(f"Checking directory: {root}")
                for file in files:
                    if file.endswith(".yml"):
                        schedule_path = os.path.join(root, file)
                        logger.debug(f"Checking file: {schedule_path}")
                        try:
                            with open(schedule_path, "r") as f:
                                schedule = yaml.safe_load(f)
                                logger.debug(f"Loaded YAML content from {schedule_path}")

                                # Check if this schedule file contains our model
                                if model_name in str(schedule):
                                    logger.info(f"Found model '{model_name}' in {schedule_path}")
                                    # Extract deploy_profile
                                    deploy_profile = schedule.get("profile")
                                    if deploy_profile:
                                        logger.info(f"Found deploy_profile: {deploy_profile}")
                                        return deploy_profile
                                    else:
                                        logger.warning(f"No profile found in {schedule_path}")
                        except yaml.YAMLError as e:
                            logger.error(f"Error parsing {schedule_path}: {e}")
                        except Exception as e:
                            logger.error(f"Error reading {schedule_path}: {e}")

            logger.warning(f"No pipeline.yml found containing model '{model_name}'")
            return None

        except Exception as e:
            logger.error(f"Failed to get deploy profile from schedule: {e}")
            return None

    def get_database_schema(
        self, model_name: str, env: str
    ) -> Optional[Tuple[str, str]]:
        """Get database and schema from profile."""
        try:
            logger.info(f"Getting database/schema for model '{model_name}' in environment '{env}'")

            # Get the deploy profile from the model's schedule
            deploy_profile = self.get_deploy_profile_from_schedule(model_name)
            if not deploy_profile:
                logger.error(f"No deploy profile found for model '{model_name}'")
                return None

            logger.info(f"Found deploy profile: {deploy_profile}")

            # Get the target configuration for this profile
            target = self.get_profile_target(deploy_profile, env)
            if not target:
                logger.error(f"No target found for profile {deploy_profile} in environment {env}")
                return None

            database = target.get("database")
            schema = target.get("schema")

            if not database or not schema:
                logger.error(f"Missing database or schema in profile target for {deploy_profile}")
                return None

            logger.info(f"Found database={database}, schema={schema} for model {model_name}")
            return database, schema

        except Exception as e:
            logger.error(f"Failed to get database/schema: {e}")
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
