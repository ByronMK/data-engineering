FROM apache/airflow:2.6.0-python3.9

USER root
# Install build dependencies for pyarrow
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install Python dependencies
RUN pip install --user \
    kafka-python==2.0.2 \
    requests==2.29.0 \
    psycopg2-binary==2.9.6 \
    scikit-learn==1.6.1 \
    pandas==2.2.2 \
    numpy==1.24.3 \
    pyarrow==9.0.0 \
    cryptography==40.0.2