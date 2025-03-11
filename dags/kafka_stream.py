import logging
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from sklearn.cluster import KMeans  # Corrected back to sklearn
from sklearn.linear_model import LogisticRegression  # Corrected back to sklearn
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

def cluster_users():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT email, dob, location FROM users_created WHERE cluster_id IS NULL LIMIT 100")
        data = cur.fetchall()

        if not data:
            logger.info("No new users to cluster")
            return

        logger.info(f"Clustering {len(data)} users")
        df = pd.DataFrame(data, columns=['email', 'dob', 'location'])

        df['age'] = df['dob'].apply(lambda x: (datetime.now() - pd.Timestamp(x)).days // 365)
        df['location_code'] = df['location'].astype('category').cat.codes

        X = df[['age', 'location_code']].fillna(0)
        kmeans = KMeans(n_clusters=3, random_state=42)
        df['cluster_id'] = kmeans.fit_predict(X)

        for _, row in df.iterrows():
            cur.execute("UPDATE users_created SET cluster_id = %s WHERE email = %s",
                        (int(row['cluster_id']), row['email']))

        conn.commit()
        logger.info(f"Successfully clustered {len(df)} users")

    except Exception as e:
        logger.error(f"Error in cluster_users: {e}")
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            cur.close()
            conn.close()
            logger.info("PostgreSQL connection closed")

def predict_engagement():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT email, dob, location, cluster_id FROM users_created WHERE engagement_score IS NULL LIMIT 100")
        data = cur.fetchall()

        if not data:
            logger.info("No new users to predict engagement for")
            return

        logger.info(f"Predicting engagement for {len(data)} users")
        df = pd.DataFrame(data, columns=['email', 'dob', 'location', 'cluster_id'])

        df['age'] = df['dob'].apply(lambda x: (datetime.now() - pd.Timestamp(x)).days // 365)
        df['location_code'] = df['location'].astype('category').cat.codes
        df['active'] = df['cluster_id'].apply(lambda x: 1 if x in [1, 2] else 0)

        X = df[['age', 'location_code', 'cluster_id']].fillna(0)
        y = df['active']
        model = LogisticRegression(random_state=42)
        model.fit(X, y)
        scores = model.predict_proba(X)[:, 1]
        df['engagement_score'] = scores

        for _, row in df.iterrows():
            cur.execute("UPDATE users_created SET engagement_score = %s WHERE email = %s",
                        (float(row['engagement_score']), row['email']))

        conn.commit()
        logger.info(f"Successfully predicted engagement for {len(df)} users")

    except Exception as e:
        logger.error(f"Error in predict_engagement: {e}")
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            cur.close()
            conn.close()
            logger.info("PostgreSQL connection closed")

# DAG definition
default_args = {
    'owner': 'byron',
    'start_date': datetime(2023, 9, 13, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=2),
}

dag = DAG(
    'my_first_dag',
    default_args=default_args,
    description='Process real-time streamed user data',
    schedule='*/5 * * * *',
    catchup=False
)

cluster_task = PythonOperator(
    task_id='cluster_task',
    python_callable=cluster_users,
    dag=dag,
)

predict_task = PythonOperator(
    task_id='predict_task',
    python_callable=predict_engagement,
    dag=dag,
)

cluster_task >> predict_taskpython