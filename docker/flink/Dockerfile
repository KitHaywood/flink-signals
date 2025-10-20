FROM apache/flink:1.18.1-scala_2.12-java11

USER root

# Install Python dependencies required by PyFlink jobs and utilities for downloading JDBC drivers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip python3-venv python3-setuptools wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Ensure `python` resolves to Python 3 for PyFlink tooling.
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install PostgreSQL JDBC driver for Flink JDBC connector compatibility.
RUN wget -q -O /opt/flink/lib/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Copy requirement lockfile for job dependencies and increase pip network timeout
# to avoid transient read timeouts when fetching large Apache Beam wheels.
COPY requirements.txt /opt/flink/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=600
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Pre-create directory for user code that will be mounted at runtime.
RUN mkdir -p /opt/flink/usrlib

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3 \
    FLINK_PYTHON=/usr/bin/python3

USER flink
