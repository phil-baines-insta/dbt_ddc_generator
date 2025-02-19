import os
from dotenv import load_dotenv
from dbt_ddc_generator.core.utils.dbt_scheduling import DbtScheduling
from dbt_ddc_generator.core.utils.dbt_profiles import DbtProfiles
from dbt_ddc_generator.core.utils.dbt_model import DbtModel
from dbt_ddc_generator.core.utils.ddc_translator import DDCTranslator
from dbt_ddc_generator.core.utils.cron_converter import CronConverter

class Generator:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Get the DBT directory from environment variables
        self.dbt_directory = os.getenv('instacart_dbt_directory')
        if not self.dbt_directory:
            raise ValueError("DBT directory not found in environment variables. Please set 'instacart_dbt_directory' in .env file")

        # Ensure the directory exists
        if not os.path.exists(self.dbt_directory):
            raise ValueError(f"DBT directory does not exist: {self.dbt_directory}")

        self.scheduling = DbtScheduling(self.dbt_directory)
        self.profiles = DbtProfiles(self.dbt_directory)
        self.translator = DDCTranslator(self.dbt_directory)
        self.cron_converter = CronConverter()

    def generate(self, model_name: str, env: str = 'local'):
        """
        Generate documentation and data contracts for a specific dbt model.
        """
        # Get model info
        model = DbtModel(self.dbt_directory, model_name)
        unique_key = model.get_unique_key()
        if not unique_key:
            return

        # Get pipeline and profile info
        pipeline_config = self.scheduling.find_pipeline_config(model_name)
        if not pipeline_config:
            return

        profile_name = pipeline_config['profile']
        if not profile_name:
            return

        # Get database and schema
        db_schema = self.profiles.get_database_schema(profile_name, env)
        if not db_schema:
            return

        database, schema = db_schema

        # Generate and print duplicates check
        duplicates_config = {
            'name': f'{database.lower()}.{schema.lower()}.{model_name} duplicate check',
            'description': f'Check for duplicate {unique_key} in {model_name}',
            'schedule_interval': CronConverter.to_hourly(pipeline_config['schedule']),
            'table': f'{database}.{schema}.{model_name}',
            'column_name': unique_key,
            'table_fqdn': f'{database}.{schema}.{model_name}'
        }

        print(self.translator.generate_duplicates_check(duplicates_config))
        print("\n---\n")  # Separator between checks

        # Generate and print freshness check
        freshness_config = {
            'name': f'{database.lower()}.{schema.lower()}.{model_name} freshness check',
            'description': f'Check freshness of {model_name}',
            'schedule_interval': CronConverter.to_hourly(pipeline_config['schedule']),
            'table': f'{database}.{schema}.{model_name}',
            'column_name': 'updated_at',  # We can make this configurable later
            'freshness_interval': '24h',
            'table_fqdn': f'{database}.{schema}.{model_name}'
        }

        print(self.translator.generate_freshness_check(freshness_config))

        # TODO: Implement the following steps:
        # 1. Parse the specific SQL file for column information
        # 2. Generate documentation for the model
        # 3. Create data contracts for the model
        # 4. Save generated files to appropriate locations
