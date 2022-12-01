# cloud2sql
Read infrastructure data from your cloud and export it to an SQL database.


## Installation

Python 3.10 or higher is required.
It is recommended to use a separate virtual environment for this project. You can create one with the following command:

```bash
python3 -m venv venv --prompt "cloud2sql"
source venv/bin/activate
```

Once the virtual environment is activated, you can install cloud2sql:

```bash
pip install cloud2sql
```

## Usage

The sources for `cloud2sql` are configured via a configuration file. Create your own configuration by adjusting the [config template file](./config-template.yaml).
After the sources are configured, you need to define the url to the database.
The following databases are currently supported:

#### SQLite

```
sqlite:///path/to/resoto.db
```

#### PostgreSQL

```
postgresql://user:password@host:port/dbname[?key=value&key=value...]
```

#### MySQL

```
mysql://username:password@host/dbname[?key=value&key=value...]
```

#### MariaDB

```
mariadb://username:password@host/dbname[?key=value&key=value...]
```

#### My database is not listed here

cloud2sql uses SQLAlchemy to connect to the database. If your database is not listed here, you can check if it is supported in [SQLAlchemy Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).
Install the relevant driver and use the connection string from the documentation.

#### Example

```bash
cloud2sql --config config.yaml --db "sqlite:///resoto.db"
```

## Local Development

Create a local development environment with the following command:

```bash
make setup
source venv/bin/activate
```

