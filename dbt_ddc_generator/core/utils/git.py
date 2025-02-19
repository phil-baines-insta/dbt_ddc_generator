import logging
import os
import subprocess
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class GitOperations:
    """Handle Git operations for the carrot repository."""

    def __init__(self) -> None:
        """Initialize GitOperations with carrot directory."""
        try:
            load_dotenv()
            self.carrot_directory = os.getenv("carrot_directory")
            if not self.carrot_directory:
                raise ValueError("Carrot directory not found in environment variables")

            if not os.path.exists(self.carrot_directory):
                raise ValueError(f"Carrot directory does not exist: {self.carrot_directory}")

            logger.info(f"Initialized GitOperations for carrot directory: {self.carrot_directory}")
        except Exception as e:
            logger.error(f"Failed to initialize GitOperations: {e}")
            raise

    def create_branch_from_master(self, branch_name: str) -> None:
        """Switch to master, pull latest, and create new branch."""
        try:
            logger.info("Switching to master branch in carrot repo")
            os.chdir(self.carrot_directory)

            # Fetch and checkout master
            subprocess.run(["git", "fetch", "origin"], check=True)
            subprocess.run(["git", "checkout", "master"], check=True)
            subprocess.run(["git", "pull", "origin", "master"], check=True)

            # Create and checkout new branch
            logger.info(f"Creating new branch: {branch_name}")
            subprocess.run(["git", "checkout", "-b", branch_name], check=True)

            logger.info(f"Successfully created branch: {branch_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed in carrot repo: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create branch in carrot repo: {e}")
            raise
