FROM python:3.9

RUN pip3 install cloud2sql[all]==0.7.2

ENTRYPOINT ["cloud2sql"]

