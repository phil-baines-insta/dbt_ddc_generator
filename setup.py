from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

setup(
    name="dbt-ddc-generator",
    version="0.1.0",
    author="Phillip Baines",
    author_email="your.email@example.com",
    description="A tool for generating dbt documentation and data contracts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/dbt-ddc-generator",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "dbt-ddc-generator=dbt_ddc_generator.cli.cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "dbt_ddc_generator": ["core/templates/*.yml"],
    },
)
