import logging
import os
import subprocess

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class GitOperations:
    """Handle Git operations for the carrot repository."""

    def __init__(self) -> None:
        """Initialize GitOperations with carrot directory."""
        try:
            load_dotenv()
            carrot_directory = os.getenv("carrot_directory")
            github_token = os.getenv("GITHUB_TOKEN")

            if not carrot_directory:
                raise ValueError("Carrot directory not found in environment variables")
            if not github_token:
                raise ValueError("GitHub token not found in environment variables")

            if not os.path.exists(carrot_directory):
                raise ValueError(f"Carrot directory does not exist: {carrot_directory}")

            # Store as str since we validated
            self.carrot_directory: str = carrot_directory
            self.github_token: str = github_token

            logger.info(
                f"Initialized GitOperations for carrot directory: {self.carrot_directory}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize GitOperations: {e}")
            raise

    def create_branch_from_master(self, branch_name: str) -> None:
        """Switch to master, pull latest, and create new branch or use existing."""
        try:
            os.chdir(self.carrot_directory)

            # Check if branch exists
            result = subprocess.run(
                ["git", "branch", "--list", branch_name],
                check=True,
                capture_output=True,
                text=True,
            )

            if result.stdout.strip():
                # Branch exists, just check it out
                logger.info(f"Using existing branch: {branch_name}")
                subprocess.run(
                    ["git", "checkout", branch_name], check=True, capture_output=True
                )
            else:
                # Create new branch from master
                logger.info("Switching to master branch in carrot repo")
                subprocess.run(
                    ["git", "fetch", "origin"], check=True, capture_output=True
                )
                subprocess.run(
                    ["git", "checkout", "master"], check=True, capture_output=True
                )
                subprocess.run(
                    ["git", "pull", "origin", "master"], check=True, capture_output=True
                )

                logger.info(f"Creating new branch: {branch_name}")
                subprocess.run(
                    ["git", "checkout", "-b", branch_name],
                    check=True,
                    capture_output=True,
                )
                print(f"Created branch: {branch_name}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed in carrot repo: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create/use branch in carrot repo: {e}")
            raise

    def commit_and_push(self, branch_name: str) -> None:
        """Commit changes and push to remote."""
        try:
            logger.info("Committing changes")
            os.chdir(self.carrot_directory)

            # Add and commit (suppress output)
            subprocess.run(["git", "add", "."], check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", "feat: add ddc checks"],
                check=True,
                capture_output=True,
            )

            # Push to remote
            logger.info(f"Pushing branch {branch_name} to remote")
            subprocess.run(
                ["git", "push", "-u", "origin", branch_name],
                check=True,
                capture_output=True,
            )

            print(f"Changes pushed to branch: {branch_name}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed in carrot repo: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to commit and push changes: {e}")
            raise

    def create_pull_request(self, branch_name: str, title: str) -> None:
        """Create a pull request for the current branch."""
        try:
            logger.info("Creating pull request")

            # GitHub API endpoint for carrot repo
            url = "https://api.github.com/repos/instacart/carrot/pulls"

            # PR data
            data = {
                "title": title,
                "body": f"Add DDC checks for {branch_name}\n\nGenerated using dbt-ddc-generator",
                "head": branch_name,
                "base": "master",
                "draft": True,  # Create PR in draft mode
            }

            # Headers with authentication
            headers = {
                "Authorization": f"token {self.github_token}",
                "Accept": "application/vnd.github.v3+json",
            }

            # Create PR
            response = requests.post(url, json=data, headers=headers)
            response.raise_for_status()

            pr_url = response.json()["html_url"]
            logger.info(f"Successfully created PR: {pr_url}")
            print(f"\nPull Request created: {pr_url}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create PR: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create PR: {e}")
            raise

    def write_to_files(self, model_name: str, generated_checks: list, database: str, schema: str) -> bool:
        """Write check files to disk."""
        try:
            # Format database and schema names to use underscores and lowercase
            formatted_database = database.replace("-", "_").lower()
            formatted_schema = schema.replace("-", "_").lower()

            # Map check types to folder names
            check_type_to_folder = {
                "duplicates": "uniqueness",
                "completeness": "completeness",
                "freshness": "freshness"
            }

            # Check if all files exist first
            all_files_exist = True
            print(f"Checking existing files for {model_name}...")
            print(f"Checks for {model_name}:")

            # First check if all files exist
            for check in generated_checks:
                folder_name = check_type_to_folder.get(check['type'], check['type'])
                check_path = os.path.join(
                    self.carrot_directory,
                    formatted_database,
                    formatted_schema,
                    folder_name,
                    f"{formatted_database}_{formatted_schema}_{model_name}_{check['type']}.yml"
                )
                if not os.path.exists(check_path):
                    all_files_exist = False
                    break

            # List all files with their status
            for check in generated_checks:
                folder_name = check_type_to_folder.get(check['type'], check['type'])
                check_path = os.path.join(
                    self.carrot_directory,
                    formatted_database,
                    formatted_schema,
                    folder_name,
                    f"{formatted_database}_{formatted_schema}_{model_name}_{check['type']}.yml"
                )
                if os.path.exists(check_path):
                    print(f"  Skipped: {os.path.basename(check_path)} (already exists)")
                    continue

                if not all_files_exist:  # Only write if we're creating files
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(check_path), exist_ok=True)
                    with open(check_path, "w") as f:
                        f.write(check["content"])
                    print(f"  Created: {os.path.basename(check_path)}")

            return not all_files_exist

        except Exception as e:
            logger.error(f"Failed to write check files: {e}")
            raise
