import click
import pkg_resources

def get_version():
    """Helper function to get the current version"""
    try:
        return pkg_resources.get_distribution('dbt-ddc-generator').version
    except pkg_resources.DistributionNotFound:
        return "Version information not available"

@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(get_version(), '-v', '--version', message='%(prog)s version %(version)s')
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

if __name__ == '__main__':
    main()
