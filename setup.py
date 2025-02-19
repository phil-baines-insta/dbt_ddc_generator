from setuptools import find_namespace_packages, setup

with open("requirements.txt") as file:
    requirements = file.read().splitlines()

with open("requirements-dev.txt") as file:
    requirements_dev = file.read().splitlines()

with open("VERSION") as f:
    version = f.read().strip()

setup(
    name="dbt-ddc-generator",
    version=version,
    description="Utility for dbt ddc generation",
    author="Data Engineering Finance",
    author_email="phillip.baines@instacart.com",
    classifiers=[
        "Programming Language :: Python :: 3.9"
    ],
    install_requires=requirements,
    keywords="instacart-dbt",
    python_requires=">=3.8, <4",
    extras_require={
        "dev": requirements_dev,
    },
    setup_requires=["setuptools_scm"],
    packages=find_namespace_packages(),
    entry_points={"console_scripts": ["dbtddc = dbt_ddc_generator.cli.cli:main"]},
)
