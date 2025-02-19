# DBT DDC Generator

A tool for generating Declarative Data Checks (DDC) for dbt models.
Creates the bare minimum DDCs to get you started.

## Overview

The DBT DDC Generator automates the creation of data quality checks and documentation for your dbt models. It generates three types of checks:

- **Duplicates Check**: Verifies no duplicate records exist based on unique key
- **Completeness Check**: Ensures data completeness against source tables
- **Freshness Check**: Validates data freshness using timestamp columns

## Installation

### Prerequisites

- Python 3.8 or higher
- Poetry for dependency management

### Install from Source

Clone the repository and install dependencies:
```bash
git clone git@github.com:instacart/dbt-ddc-generator.git
cd dbt-ddc-generator
poetry install
```

### Environment Setup

Create a `.env` file in the project root:
```bash
instacart_dbt_directory=/path/to/your/dbt/project
dbt_profiles_directory=/path/to/your/dbt/profiles
carrot_directory=/path/to/carrot/repo
GITHUB_TOKEN=your_github_token
```

## Usage

### Basic Commands

Generate checks for models:
```bash
# Single model
dbtddc generate fact_orders --env prod

# Multiple models
dbtddc generate fact_orders dim_products --env prod

# Show version
dbtddc version
```

### Environment Options

- `local` (default): Local development environment
- `dev`: Development environment
- `prod`: Production environment

## Project Structure

```
dbt_ddc_generator/
├── cli/                    # Command line interface
│   └── cli.py             # CLI implementation
├── core/                   # Core functionality
│   ├── generator/         # Check generation logic
│   │   └── generator.py   # Main generator class
│   ├── templates/         # Check templates
│   │   ├── completeness.yml
│   │   ├── duplicates.yml
│   │   └── freshness.yml
│   └── utils/            # Utility functions
│       ├── dbt_model.py      # DBT model parsing
│       ├── dbt_profiles.py   # Profile management
│       ├── dbt_scheduling.py # Schedule parsing
│       ├── ddc_translator.py # Template rendering
│       └── git.py           # Git operations
└── tests/                # Test suite
```

## Development

### Setup Development Environment

Install all dependencies including dev dependencies:
```bash
poetry install
```

### Development Commands

```bash
# Format code
make format

# Run linters
make lint

# Run tests
make test

# Build package
make build
```

### Code Style

The project uses:
- **Black**: Code formatting
- **Ruff**: Linting
- **MyPy**: Type checking

## Contributing

1. Create a new branch
2. Make your changes
3. Run tests and linting
4. Submit a pull request
