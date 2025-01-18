# build python egg and deps
FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
# install dependencies
RUN pip install -r requirements.txt

# copy to python slim site packages
COPY cmd ./cmd
COPY sqlflow ./sqlflow
COPY setup.py .
RUN python setup.py install

# Add /app/plugins to the Python path
ENV PYTHONPATH="/app/plugins:${PYTHONPATH}"

ENTRYPOINT [ "python", "cmd/sql-flow.py" ]