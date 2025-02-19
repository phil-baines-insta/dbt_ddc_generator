import logging
import sys
from typing import Optional

import click
import pkg_resources

from dbt_ddc_generator.core.generator.generator import Generator
from dbt_ddc_generator.core.utils.git import GitOperations

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_version() -> str:
    """
    Get the current version of dbt-ddc-generator.

    Returns:
        str: Current version or version not available message
    """
    try:
        return pkg_resources.get_distribution("dbt-ddc-generator").version
    except pkg_resources.DistributionNotFound:
        return "Version information not available"


def init_generator() -> Optional[Generator]:
    """
    Initialize the Generator with error handling.

    Returns:
        Optional[Generator]: Initialized generator or None if initialization fails
    """
    try:
        return Generator()
    except Exception as e:
        logger.error(f"Failed to initialize generator: {e}")
        return None


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option(
    get_version(), "-v", "--version", message="%(prog)s version %(version)s"
)
def main() -> None:
    """
    DBT DDC Generator - A tool for generating dbt documentation and data contracts.

    This CLI tool helps automate the creation of dbt documentation and data contracts
    for your dbt projects.
    """
    pass


@main.command()
def version() -> None:
    """Display the current version of dbt-ddc-generator."""
    click.echo(f"dbt-ddc-generator version {get_version()}")


@main.command()
@click.argument("model_names", nargs=-1, required=True)  # Accept multiple model names
@click.option(
    "--env",
    type=click.Choice(["local", "dev", "prod"]),
    default="local",
    help="Environment to use for profile configuration (local, dev, or prod)",
    show_default=True,
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    help="Directory to write generated files (optional)",
)
def generate(model_names: tuple, env: str, output_dir: Optional[str] = None) -> None:
    """
    Generate DDC (Documentation and Data Contracts) for specific dbt models.

    MODEL_NAMES: The names of the dbt models to generate DDC for (e.g., 'stg_users dim_customers fact_orders')

    Examples:
        dbtddc generate stg_users dim_customers --env prod
        dbtddc generate fact_orders dim_products --env dev
    """
    try:
        # Initialize generator
        generator = init_generator()
        if not generator:
            raise click.Abort()

        all_generated_checks = []
        # Generate DDC for each model
        for model_name in model_names:
            logger.info(f"Generating DDC for model: {model_name} in environment: {env}")
            generated_checks = generator.generate(model_name, env)
            all_generated_checks.extend(
                [{"model": model_name, "checks": generated_checks}]
            )

            # Print generated checks
            print(f"\nGenerated checks for {model_name}:")
            for check in generated_checks:
                print(check["content"])
                print("\n---\n")

        # Prompt user for creating files
        if click.confirm(
            "Do you want to create these files in the carrot repo?", default=False
        ):
            # Keep prompting until valid branch name is provided
            while True:
                branch_name = click.prompt("Enter branch name", type=str)
                if branch_name:
                    break
                print("You must enter a branch name")

            git_ops = GitOperations()

            # Create or switch to branch first
            git_ops.create_branch_from_master(branch_name)
            print()  # Add blank line after branch message

            # write_to_carrot returns True if files were created, False if all skipped
            files_created = False
            for generated_check in all_generated_checks:
                model_name = generated_check["model"]
                checks = generated_check["checks"]
                if git_ops.write_to_files(
                    model_name, checks
                ):  # New method that just handles file operations
                    files_created = True

            if files_created:
                # Only show commit prompt if files were created
                if click.confirm(
                    "Do you want to commit and push these changes to remote?",
                    default=False,
                ):
                    git_ops.commit_and_push(branch_name)
                    logger.info(
                        f"Successfully pushed changes to remote branch: {branch_name}"
                    )

                    # Prompt for PR creation
                    if click.confirm(
                        "Do you want to create a pull request?", default=False
                    ):
                        # Keep prompting until valid PR title is provided
                        while True:
                            pr_title = click.prompt("Enter PR title", type=str)
                            if pr_title:
                                break
                            print("You must enter a PR title")

                        git_ops.create_pull_request(
                            branch_name, ", ".join(model_names), pr_title
                        )
                    else:
                        logger.info("Skipped creating pull request")
                else:
                    logger.info("Skipped pushing changes to remote")
        else:
            logger.info("Skipped writing to carrot repo")

    except Exception as e:
        logger.error(f"Error generating DDC: {e}")
        raise click.Abort()


def cli() -> None:
    """Entry point for the CLI."""
    try:
        main()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
