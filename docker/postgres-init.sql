cat << 'EOF' > docker/postgres-init.sql
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow') THEN
      CREATE DATABASE airflow;
   END IF;
END
$$;

\c airflow

CREATE TABLE IF NOT EXISTS users_created (
    email VARCHAR(255) PRIMARY KEY,
    dob VARCHAR(50),
    location VARCHAR(255),
    cluster_id INT,
    engagement_score FLOAT
);
EOF