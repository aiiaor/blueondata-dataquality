"""
Test PostgreSQL Connection DAG

This DAG tests the connection to the PostgreSQL database used in the data cleaning pipeline.
It attempts to connect to the database and execute a simple query to verify connectivity.
The DAG is scheduled to run daily but can also be triggered manually for on-demand testing.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import psycopg2
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='A DAG to test PostgreSQL database connectivity',
    schedule_interval='@daily',
    catchup=False
)

def test_postgres_connection():
    """
    Test connection to PostgreSQL database and execute a simple query.
    
    Returns:
        bool: True if connection and query successful, False otherwise.
    """
    logger.info("Testing PostgreSQL connection")
    
    try:
        # Get connection parameters from environment variables
        host = os.getenv('PG_HOST')
        port = os.getenv('PG_PORT')
        database = os.getenv('PG_DB')
        user = os.getenv('PG_USER')
        password = os.getenv('PG_PASSWORD')
        
        # Establish connection
        logger.info(f"Connecting to PostgreSQL at {host}:{port}/{database}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode='disable'
        )
        
        logger.info("Connection established successfully")
        
        # Execute a simple query
        query = "SELECT order_id, customer, order_date FROM ecommerce_orders LIMIT 5;"
        df = pd.read_sql(query, conn)
        
        # Display results
        logger.info(f"Query executed successfully. Retrieved {len(df)} rows.")
        print("\nSample data from ecommerce_orders:")
        print(df)
        
        # Close connection
        conn.close()
        logger.info("Connection closed")
        
        return True
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False

# Define the PythonOperator task
test_connection_task = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag
)

# Task dependencies (single task in this case)
test_connection_task
