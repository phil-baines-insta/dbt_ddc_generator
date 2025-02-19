import os
from typing import Dict
from jinja2 import Template

class DDCTranslator:
    def __init__(self, dbt_directory: str):
        self.dbt_directory = dbt_directory
        self.template_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')

        # Load templates
        self.freshness_template = self._load_template('freshness.yml')
        self.duplicates_template = self._load_template('duplicates.yml')

    def _load_template(self, template_name: str) -> Template:
        """Load and return a Jinja2 template."""
        template_path = os.path.join(self.template_dir, template_name)
        with open(template_path, 'r') as f:
            return Template(f.read())

    def generate_freshness_check(self, config: Dict) -> str:
        """
        Generate a freshness check YAML configuration.

        Args:
            config: Dictionary containing:
                - name: Name of the check
                - description: Description of the check
                - schedule_interval: How often to run the check
                - table: Full table name
                - column_name: Column to check freshness on
                - freshness_interval: Time interval for freshness
                - table_fqdn: Fully qualified table name for annotations

        Returns:
            str: Rendered YAML configuration
        """
        # Ensure table and column names are lowercase
        config['table'] = config['table'].lower()
        config['column_name'] = config['column_name'].lower()
        config['table_fqdn'] = config['table_fqdn'].lower()

        return self.freshness_template.render(
            name=config['name'],
            description=config['description'],
            schedule_interval=config['schedule_interval'],
            table=config['table'],
            column_name=config['column_name'],
            freshness_interval=config['freshness_interval'],
            table_fqdn=config['table_fqdn']
        )

    def generate_duplicates_check(self, config: Dict) -> str:
        """
        Generate a duplicates check YAML configuration.

        Args:
            config: Dictionary containing:
                - name: Name of the check
                - description: Description of the check
                - schedule_interval: How often to run the check
                - table: Full table name
                - column_name: Column to check for duplicates
                - table_fqdn: Fully qualified table name for annotations

        Returns:
            str: Rendered YAML configuration
        """
        # Ensure table and column names are lowercase
        config['table'] = config['table'].lower()
        config['column_name'] = config['column_name'].lower()
        config['table_fqdn'] = config['table_fqdn'].lower()

        return self.duplicates_template.render(
            name=config['name'],
            description=config['description'],
            schedule_interval=config['schedule_interval'],
            table=config['table'],
            column_name=config['column_name'],
            table_fqdn=config['table_fqdn']
        )

    def create_table_fqdn(self, database: str, schema: str, table: str) -> str:
        """Create a fully qualified table name for annotations."""
        return f"{database.lower()}.{schema.lower()}.{table.lower()}"
