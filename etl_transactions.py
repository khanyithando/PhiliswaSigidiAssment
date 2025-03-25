from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from dateutil.parser import parse

logger = logging.getLogger(__name__)


default_args = {
    'owner': 'candidate',  
    'depends_on_past': False,
    'email_on_failure': False,  
    'email_on_retry': False,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etl_transactions',
    default_args=default_args,
    description='ETL pipeline for financial transactions',
    schedule_interval='0 0 * * *', 
    start_date=datetime(2023, 1, 1),  
    catchup=False,
)

# Extract function
def extract():
    logger.info("Extracting data from CSV")
    try:
        df = pd.read_csv('/opt/airflow/data/financial_transactions.csv')
        logger.info(f"Successfully read {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV: {str(e)}")
        raise

# Transform function
def transform(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract')
    
    logger.info("Transforming data")
    
    # Clean amounts
    df['amount'] = df['amount'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df.dropna(subset=['amount'])
    logger.info(f"After amount cleaning: {len(df)} records")
    
    # Parse dates
    df['transaction_date'] = df['transaction_date'].apply(
        lambda x: parse(x).date() if pd.notna(x) else None
    )
    df = df.dropna(subset=['transaction_date'])
    logger.info(f"After date parsing: {len(df)} records")
    
    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates(subset=['transaction_id'])
    logger.info(f"Removed {initial_count - len(df)} duplicates")
    
    return df

# Load function
def load(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform')
    
    logger.info("Loading data to PostgreSQL")
    conn = None
    cursor = None
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                user_id INT NOT NULL,
                amount FLOAT NOT NULL,
                transaction_date DATE NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id);
        """)
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            tuples = [tuple(x) for x in batch[
                ['transaction_id', 'user_id', 'amount', 'transaction_date']
            ].to_numpy()]
            
            cursor.executemany("""
                INSERT INTO transactions 
                (transaction_id, user_id, amount, transaction_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, tuples)
            conn.commit()
            logger.info(f"Inserted batch {i//batch_size + 1}")
        
        logger.info("Data load completed successfully")
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)


extract_task >> transform_task >> load_task