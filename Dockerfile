FROM python:3.11

# Leaving out snowflake, it requires an arrow installation.
RUN pip3 install cloud2sql[mysql,mariadb,postgresql,parquet]==0.7.2

CMD ["cloud2sql"]

