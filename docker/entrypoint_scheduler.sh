#!/bin/bash
set -e

echo "Starting entrypoint script at $(date)"

# Wait for PostgreSQL to be ready
until pg_isready -h postgres -p 5432 -U airflow; do
  echo "Waiting for PostgreSQL at $(date)..."
  sleep 2
done
echo "PostgreSQL is ready at $(date)"

# Initialize the db
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Initializing Airflow database at $(date)..."
  airflow db init || { echo "airflow db init failed at $(date)"; exit 1; }
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin \
    || { echo "airflow users create failed at $(date)"; exit 1; }
  echo "Database initialized and admin user created at $(date)"
fi

# Upgrade the db
echo "Upgrading Airflow database at $(date)..."
airflow db upgrade || { echo "airflow db upgrade failed at $(date)"; exit 1; }
echo "Database upgraded successfully at $(date)"

# Start the scheduler 
echo "Starting Airflow scheduler at $(date)..."
exec airflow scheduler || { echo "airflow scheduler failed at $(date)"; exit 1; }
