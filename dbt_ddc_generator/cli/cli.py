import click
import pkg_resources
from dbt_ddc_generator.core.generator.generator import Generator


def get_version():
    """Helper function to get the current version"""
    try:
        return pkg_resources.get_distribution("dbt-ddc-generator").version
    except pkg_resources.DistributionNotFound:
        return "Version information not available"


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option(get_version(), "-v", "--version", message="%(prog)s version %(version)s")
def main():
    """DBT DDC Generator - A tool for generating dbt documentation and data contracts.

    This CLI tool helps automate the creation of dbt documentation and data contracts
    for your dbt projects.
    """
    pass


@main.command()
def version():
    """Display the current version of dbt-ddc-generator."""
    click.echo(f"dbt-ddc-generator version {get_version()}")


@main.command(name="generate-ddc")
@click.argument('model_name', required=True)
@click.option('--env', type=click.Choice(['local', 'dev', 'prod']), default='local', help='Environment to use for profile configuration')
def generate_ddc(model_name, env):
    """Generate DDC (Documentation and Data Contracts) for a specific dbt model.

    MODEL_NAME: The name of the dbt model to generate DDC for (e.g., 'stg_users' or 'dim_customers')
    """
    try:
        generator = Generator()
        generator.generate(model_name, env)
        click.echo(f"Successfully generated DDC files for model: {model_name}!")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()
    except Exception as e:
        click.echo(f"An unexpected error occurred: {str(e)}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    main()
