# cloud2sql
Read infrastructure data from your cloud and export it to a SQL database.


## Installation

Python 3.9 or higher is required.
It is recommended to use a separate virtual environment for this project. You can create one with the following command:

```bash
python3 -m venv venv --prompt "cloud2sql"
source venv/bin/activate
```

Once the virtual environment is activated, you can install cloud2sql:

```bash
pip install cloud2sql[all]
```

If you only require support for a specific database, instead of `cloud2sql[all]` you can choose between `cloud2sql[snowflake]`, `cloud2sql[parquet]`, `cloud2sql[postgresql]`, `cloud2sql[mysql]`.

## Usage

The sources and destinations for `cloud2sql` are configured via a configuration file. Create your own configuration by adjusting the [config template file](./config-template.yaml).
You can safely delete the sections that are not relevant to you (e.g. if you do not use AWS, you can delete the `aws` section).
All sections refer to cloud providers and are enabled if a configuration section is provided.

The following databases are currently supported:

#### SQLite

```
destinations:
    sqlite:
        database: /path/to/database.db
```

#### PostgreSQL

```
destinations:
    postgresql:
        host: 127.0.0.1
        port: 5432
        user: cloud2sql
        password: changeme
        database: cloud2sql
        args:
            key: value
```

#### MySQL

```
destinations:
    mysql:
        host: 127.0.0.1
        port: 3306
        user: cloud2sql
        password: changeme
        database: cloud2sql
        args:
            key: value
```

#### MariaDB

```
destinations:
    mariadb:
        host: 127.0.0.1
        port: 3306
        user: cloud2sql
        password: changeme
        database: cloud2sql
        args:
            key: value
```

#### Snowflake

```
destinations:
    snowflake:
        host: myorg-myaccount
        user: cloud2sql
        password: changeme
        database: cloud2sql/public
        args:
            warehouse: compute_wh
            role: accountadmin
```

#### Apache Parquet

```
destinations:
    file:
        path: /where/to/write/parquet/files/
        format: parquet
        batch_size: 100_000
```

#### CSV

```
destinations:
    file:
        path: /where/to/write/to/csv/files/
        format: csv
        batch_size: 100_000
```


#### My database is not listed here

cloud2sql uses SQLAlchemy to connect to the database. If your database is not listed here, you can check if it is supported in [SQLAlchemy Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).
Install the relevant driver and use the connection string from the documentation.

#### Example

We use a minimal configuration [example](./config-example.yaml) and export the data to a SQLite database.
The example uses our AWS default credentials and the default kubernetes config.

```bash
cloud2sql --config config-example.yaml
```

## Local Development

Create a local development environment with the following command:

```bash
make setup
source venv/bin/activate
```
