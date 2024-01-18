from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open(Path(__file__).parent / "dagster_dbt/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)

    return version["__version__"]


ver = get_version()
setup(
    name="paradime-dagster-dbt",
    version=ver,
    author="Dagster Labs",
    author_email="hello@dagsterlabs.com",
    license="Apache-2.0",
    description="A Dagster integration for dbt",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-dbt",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=["dagster_dbt_tests*"]),
    include_package_data=True,
    install_requires=[
        "dagster==1.5.9",  # hardcoded by paradime
        # Follow the version support constraints for dbt Core: https://docs.getdbt.com/docs/dbt-versions/core
        "dbt-core<1.8",
        "cachetools",
        "Jinja2",
        "networkx",
        "orjson",
        "requests",
        "rich",
        "typer>=0.9.0",
        "packaging",
    ],
    extras_require={
        "test": [
            "dbt-duckdb",
            "dagster-duckdb",
            "dagster-duckdb-pandas",
        ]
    },
    entry_points={
        "console_scripts": [
            "dagster-dbt-cloud = dagster_dbt.cloud.cli:app",
            "dagster-dbt = dagster_dbt.cli.app:app",
        ]
    },
    zip_safe=False,
)
