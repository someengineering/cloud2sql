# Cloud2SQL 🤩
Read infrastructure data from your cloud ☁️ and export it to a SQL database 📋.

![Cloud2SQL](misc/cloud2sql.gif)

## Installation

### Install via homebrew

This is the easiest way to install Cloud2SQL. 
Please note, that the installation process will take a couple of minutes.

```bash
brew install someengineering/tap/cloud2sql
```

### Install via Python pip

Alternatively you can install Cloud2SQL as Python package, where Python 3.9 or higher is required.

If you only need support for a specific database, instead of `cloud2sql[all]` you can choose between `cloud2sql[snowflake]`, `cloud2sql[parquet]`, `cloud2sql[postgresql]`, `cloud2sql[mysql]`.

```bash
pip3 install --user "cloud2sql[all]"
```

This will install the executable to the user install directory of your platform. Please make sure this installation directory is listed in `PATH`.


## Usage

The sources and destinations for `cloud2sql` are configured via a configuration file. Create your own configuration by adjusting the [config template file](./config-template.yaml).

You can safely delete the sections that are not relevant to you (e.g. if you do not use AWS, you can delete the `aws` section).
All sections refer to cloud providers and are enabled if a configuration section is provided.

In the next section you will create a YAML configuration file. Once you have created your configuration file, you can run `cloud2sql` with the following command:

```bash
cloud2sql --config myconfig.yaml
```

## Configuration

Cloud2SQL uses a YAML configuration file to define the `sources` and `destinations`.

### Sources

#### AWS

```yaml
sources:
  aws:
    # AWS Access Key ID (null to load from env - recommended)
    access_key_id: null
    # AWS Secret Access Key (null to load from env - recommended)
    secret_access_key: null
    # IAM role name to assume
    role: null
    # List of AWS profiles to collect
    profiles: null
    # List of AWS Regions to collect (null for all)
    region: null
    # Scrape the entire AWS organization
    scrape_org: false
    # Assume given role in current account
    assume_current: false
    # Do not scrape current account
    do_not_scrape_current: false
```

#### Google Cloud
    
```yaml
sources:
  gcp:
    # GCP service account file(s)
    service_account: []
    # GCP project(s)
    project: []
```

#### Kubernetes

```yaml
sources:
  k8s:
    # Configure access via kubeconfig files.
    # Structure:
    #   - path: "/path/to/kubeconfig"
    #     all_contexts: false
    #     contexts: ["context1", "context2"]
    config_files: []
    # Alternative: configure access to k8s clusters directly in the config.
    # Structure:
    #   - name: 'k8s-cluster-name'
    #     certificate_authority_data: 'CERT'
    #     server: 'https://k8s-cluster-server.example.com'
    #     token: 'TOKEN'
    configs: []
```

#### DigitalOcean

```yaml
sources:
digitalocean:
  # DigitalOcean API tokens for the teams to be collected
  api_tokens: []
  # DigitalOcean Spaces access keys for the teams to be collected, separated by colons
  spaces_access_keys: []
```

### Destinations

#### SQLite

```yaml
destinations:
  sqlite:
    database: /path/to/database.db
```

#### PostgreSQL

```yaml
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

```yaml
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

```yaml
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

```yaml
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

```yaml
destinations:
  file:
    path: /where/to/write/parquet/files/
    format: parquet
    batch_size: 100_000
```

#### CSV

```yaml
destinations:
  file:
    path: /where/to/write/to/csv/files/
    format: csv
    batch_size: 100_000
```

#### Upload to S3

```yaml
destinations:
  s3:
    uri: s3://bucket_name/
    region: eu-central-1
    format: csv
    batch_size: 100_000
```

### Upload to Google Cloud Storage

```yaml
destinations:
  gcs:
    uri: gs://bucket_name/
    format: parquet
    batch_size: 100_000
```

#### My database is not listed here

Cloud2SQL uses SQLAlchemy to connect to the database. If your database is not listed here, you can check if it is supported in [SQLAlchemy Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).
Install the relevant driver and use the connection string from the documentation.

#### Example

We use a minimal configuration [example](./config-example.yaml) and export the data to a SQLite database.
The example uses our AWS default credentials and the default kubernetes config.

```bash
cloud2sql --config config-example.yaml
```

For a more in-depth example, check out our [blog post](https://resoto.com/blog/2022/12/21/installing-cloud2sql).

## Local Development

Create a local development environment with the following command:

```bash
make setup
source venv/bin/activate
```
