FROM apache/flink:1.18.1-scala_2.12-java11

USER root

# Install Python dependencies required by PyFlink jobs.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip python3-venv python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

# Copy requirement lockfile for job dependencies.
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Pre-create directory for user code that will be mounted at runtime.
RUN mkdir -p /opt/flink/usrlib

USER flink
