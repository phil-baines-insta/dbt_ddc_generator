import os
from dotenv import load_dotenv
from dbt_ddc_generator.core.utils.dbt_scheduling import DbtScheduling
from dbt_ddc_generator.core.utils.dbt_profiles import DbtProfiles

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

    def generate(self, model_name: str, env: str = 'local'):
        """
        Generate documentation and data contracts for a specific dbt model.

        Args:
            model_name: The name of the dbt model to generate DDC for
            env: The environment to use for profile configuration
        """
        # Validate dbt project structure
        models_dir = os.path.join(self.dbt_directory, 'models')
        if not os.path.exists(models_dir):
            raise ValueError(f"Models directory not found in {self.dbt_directory}")

        # Find the model file
        model_file = None
        for root, _, files in os.walk(models_dir):
            for file in files:
                if file == f"{model_name}.sql":
                    model_file = os.path.join(root, file)
                    break
            if model_file:
                print(model_file)
                break

        if not model_file:
            raise ValueError(f"Model '{model_name}' not found in {models_dir}")

        # Find scheduling configuration
        pipeline_config = self.scheduling.find_pipeline_config(model_name)
        if pipeline_config:
            print(f"\nPipeline Configuration:")
            print(f"  File: {pipeline_config['file_path']}")

            # Get and display profile information
            profile_name = pipeline_config['profile']
            print(f"  Profile: {profile_name}")

            if profile_name:
                db_schema = self.profiles.get_database_schema(profile_name, env)
                if db_schema:
                    database, schema = db_schema
                    print(f"  Target Environment: {env}")
                    print(f"  Target Database: {database}")
                    print(f"  Target Schema: {schema}")
                else:
                    print(f"  Warning: No database/schema configuration found for profile '{profile_name}' in {env} environment")

            print(f"  Pipeline: {pipeline_config['pipeline_name']}")
            print(f"  Owner: {pipeline_config['owner']}")
            print(f"  Schedule: {pipeline_config['schedule']}")
            print(f"  Description: {pipeline_config['description']}")
            if pipeline_config['model_config'].get('sensors'):
                print("\n  Sensors:")
                for sensor in pipeline_config['model_config']['sensors']:
                    print(f"    - {sensor}")
            if pipeline_config['model_config'].get('create_upstream_models'):
                print("\n  Upstream Models:")
                for model in pipeline_config['model_config']['create_upstream_models']:
                    print(f"    - {model}")
        else:
            print(f"\nNo pipeline configuration found for model: {model_name}")

        # TODO: Implement the following steps:
        # 1. Parse the specific SQL file for column information
        # 2. Generate documentation for the model
        # 3. Create data contracts for the model
        # 4. Save generated files to appropriate locations
