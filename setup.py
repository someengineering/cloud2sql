from setuptools import setup, find_packages
import os
import pkg_resources
import cloud2sql


def read(file_name: str) -> str:
    with open(os.path.join(os.path.dirname(__file__), file_name)) as of:
        return of.read()


def read_requirements(fname):
    return [str(requirement) for requirement in pkg_resources.parse_requirements(read(fname))]


required_mysql = read_requirements("requirements-mysql.txt")
required_postgresql = read_requirements("requirements-postgresql.txt")
required_snowflake = read_requirements("requirements-snowflake.txt")
required_parquet = read_requirements("requirements-parquet.txt")


setup(
    name=cloud2sql.__title__,
    version=cloud2sql.__version__,
    description=cloud2sql.__description__,
    python_requires=">=3.9",
    classifiers=["Programming Language :: Python :: 3"],
    entry_points={"console_scripts": ["cloud2sql=cloud2sql.__main__:main"]},
    install_requires=read_requirements("requirements.txt"),
    extras_require={
        "all": required_mysql + required_postgresql + required_snowflake + required_parquet,
        "mysql": required_mysql,
        "mariadb": required_mysql,
        "postgresql": required_postgresql,
        "snowflake": required_snowflake,
        "parquet": required_parquet,
    },
    license="Apache Software License 2.0",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    include_package_data=True,
    packages=find_packages(include=["cloud2sql", "cloud2sql.*"]),
    setup_requires=["pytest-runner"],
    test_suite="tests",
    tests_require=read_requirements("requirements-test.txt"),
    url="https://github.com/someengineering/cloud2sql",
)
