import logging
import sys
from typing import Optional

import click
import pkg_resources

from dbt_ddc_generator.core.generator.generator import Generator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
@click.argument("model_name", required=True)
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
def generate(model_name: str, env: str, output_dir: Optional[str] = None) -> None:
    """
    Generate DDC (Documentation and Data Contracts) for a specific dbt model.

    MODEL_NAME: The name of the dbt model to generate DDC for (e.g., 'stg_users' or 'dim_customers')

    Examples:
        dbtddc generate stg_users --env prod
        dbtddc generate dim_customers --env dev --output-dir ./ddc_files
    """
    try:
        # Initialize generator
        generator = init_generator()
        if not generator:
            raise click.Abort()

        # Generate DDC
        logger.info(f"Generating DDC for model: {model_name} in environment: {env}")
        generated_checks = generator.generate(model_name, env)

        # Print generated checks
        for check in generated_checks:
            print(check['content'])
            print("\n---\n")

        # Prompt user
        if click.confirm('Do you want to create these files in the carrot repo?', default=False):
            # Keep prompting until valid branch name is provided
            while True:
                branch_name = click.prompt('Enter branch name', type=str, default='')
                if branch_name:
                    break
                print("You must enter a branch name")  # Use print instead of logger for cleaner output

            generator.write_to_carrot(model_name, generated_checks, branch_name)
            logger.info(f"Successfully written checks to carrot repo in branch: {branch_name}")
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
