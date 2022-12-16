#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

import cloud2sql

with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("requirements-mysql.txt") as f:
    required_mysql = f.read().splitlines()

with open("requirements-postgresql.txt") as f:
    required_postgresql = f.read().splitlines()

with open("requirements-parquet.txt") as f:
    required_parquet = f.read().splitlines()

with open("requirements-snowflake.txt") as f:
    required_snowflake = f.read().splitlines()

with open("requirements-test.txt") as f:
    test_required = f.read().splitlines()

with open("README.md") as f:
    readme = f.read()

setup_requirements = [
    "pytest-runner",
]


setup(
    name=cloud2sql.__title__,
    version=cloud2sql.__version__,
    description=cloud2sql.__description__,
    python_requires=">=3.9",
    classifiers=["Programming Language :: Python :: 3"],
    entry_points={"console_scripts": ["cloud2sql=cloud2sql.__main__:main"]},
    install_requires=required,
    extras_require={
        "all": required_mysql + required_postgresql + required_snowflake + required_parquet,
        "mysql": required_mysql,
        "mariadb": required_mysql,
        "postgresql": required_postgresql,
        "snowflake": required_snowflake,
        "parquet": required_parquet,
    },
    license="Apache Software License 2.0",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    packages=find_packages(include=["cloud2sql", "cloud2sql.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_required,
    url="https://github.com/someengineering/cloud2sql",
)
