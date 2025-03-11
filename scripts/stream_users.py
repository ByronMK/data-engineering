import logging
import time
import requests
import json
from kafka import KafkaProducer
import psycopg2
from psycopg2 import OperationalError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    max_retries = 5
    retry_delay = 5  # seconds
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="airflow",
                user="airflow",
                password="airflow",
                port="5432"
            )
            logger.info("Connected to PostgreSQL")
            return conn
        except OperationalError as e:
            logger.error(f"Postgres connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise Exception("Failed to connect to PostgreSQL after retries")

def stream_users():
    producer = None
    conn = None
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers='broker:29092',
            retries=5,
            retry_backoff_ms=1000
        )
        logger.info("Connected to Kafka at broker:29092")

        # Initialize PostgreSQL connection
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_created (
                email VARCHAR(255) PRIMARY KEY,
                dob VARCHAR(50),
                location VARCHAR(255),
                cluster_id INT,
                engagement_score FLOAT
            )
        """)
        conn.commit()
        logger.info("Ensured users_created table exists")

        user_count = 0
        while True:
            try:
                response = requests.get('https://randomuser.me/api/?results=1', timeout=5)
                response.raise_for_status()
                user = response.json()['results'][0]
                user_count += 1

                email = user['email']
                dob = user['dob']['date']
                location = f"{user['location']['city']}, {user['location']['country']}"

                # Send to Kafka
                message = json.dumps({'email': email, 'dob': dob, 'location': location}).encode('utf-8')
                producer.send('users_created', message)
                logger.info(f"[{user_count}] Sent to Kafka: {email}")

                # Insert into PostgreSQL
                cur.execute(
                    "INSERT INTO users_created (email, dob, location) VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING",
                    (email, dob, location)
                )
                conn.commit()
                logger.info(f"[{user_count}] Inserted into PostgreSQL: {email}")

                time.sleep(0.5)

            except requests.RequestException as e:
                logger.error(f"API request failed: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Streaming error: {e}")
                time.sleep(5)

    except Exception as e:
        logger.error(f"Fatal error in stream_users: {e}")
        raise
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed")
        if conn and not conn.closed:
            cur.close()
            conn.close()
            logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    logger.info("Starting real-time user streaming...")
    stream_users()