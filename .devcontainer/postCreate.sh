python3 -m pip install --upgrade pip
pip install -r requirements-test.txt
pip install -r requirements.txt
pip install -r requirements-parquet.txt
pip install -e .
mypy --install-types --non-interactive cloud2sql tests

# building
pip install tox wheel flake8 build