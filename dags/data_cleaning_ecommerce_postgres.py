"""
E-commerce Data Cleaning DAG for PostgreSQL.

This DAG extracts data from a PostgreSQL database containing e-commerce orders,
performs various data cleaning operations, validates the data using Great Expectations,
and loads the cleaned data back to the database.

The cleaning operations include:
- Handling missing values
- Removing duplicate records
- Standardizing data formats
- Adding computed columns
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import psycopg2
import logging
from typing import Dict, Any


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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_cleaning_ecommerce_postgres',
    default_args=default_args,
    description='A DAG for cleaning an e-commerce dataset from PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)


def get_database_connection():
    """
    Create and return a database connection to PostgreSQL.

    Returns:
        psycopg2.connection: A connection object to the PostgreSQL database.
    """
    logger.info("Establishing database connection")
    try:
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
            password=password
        )
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise


def extract_data(conn):
    """
    Extract data from PostgreSQL database.

    Args:
        conn (psycopg2.connection): Database connection object.

    Returns:
        pandas.DataFrame: The extracted data.
    """
    logger.info("Extracting data from ecommerce_orders table")
    query = "SELECT order_id, customer, order_date, product, quantity, price FROM ecommerce_orders;"
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df)} records")
        logger.debug("Original DataFrame:")
        logger.debug(df.head())
        print("Original DataFrame:")
        print(df.head())
        return df
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise


def handle_missing_values(df):
    """
    Fill missing values in the dataframe.

    Args:
        df (pandas.DataFrame): Input dataframe with missing values.

    Returns:
        pandas.DataFrame: Dataframe with missing values filled.
    """
    logger.info("Handling missing values")
    missing_quantity = df['quantity'].isna().sum()
    missing_price = df['price'].isna().sum()
    logger.info(f"Found {missing_quantity} missing quantity values and {missing_price} missing price values")
    
    df['quantity'] = df['quantity'].fillna(df['quantity'].median())
    df['price'] = df['price'].fillna(df['price'].mean())
    
    logger.info("Missing values handled successfully")
    return df


def remove_duplicates(df):
    """
    Remove duplicate order records.

    Args:
        df (pandas.DataFrame): Input dataframe potentially containing duplicates.

    Returns:
        pandas.DataFrame: Dataframe with duplicates removed.
    """
    logger.info("Removing duplicate records")
    initial_count = len(df)
    df_deduped = df.drop_duplicates(subset='order_id', keep='first')
    removed_count = initial_count - len(df_deduped)
    logger.info(f"Removed {removed_count} duplicate records")
    return df_deduped


def standardize_formats(df):
    """
    Standardize data formats for consistency.

    Args:
        df (pandas.DataFrame): Input dataframe with inconsistent formats.

    Returns:
        pandas.DataFrame: Dataframe with standardized formats.
    """
    logger.info("Standardizing data formats")
    
    # Count invalid dates before conversion
    invalid_dates_before = df['order_date'].isna().sum()
    
    # Handle multiple date formats
    # This will handle formats like:
    # - 2021/01/15
    # - 15-01-2021
    # - January 15, 2021
    # - 15/01/2021
    # - 2021.01.16
    df['order_date'] = pd.to_datetime(df['order_date'], 
                                     format='mixed',  # Use mixed format detection
                                     dayfirst=True)  # Interpret as day first for ambiguous dates
    
    print("Standardized date formats")
    print(df['order_date'].head())
    # Count invalid dates after conversion
    invalid_dates_after = df['order_date'].isna().sum()
    logger.info(f"Date standardization: {invalid_dates_after - invalid_dates_before} new invalid dates detected")
    
    df['customer'] = df['customer'].str.strip().str.title()
    df['product'] = df['product'].str.strip().str.lower().str.title()
    
    logger.info("Data formats standardized successfully")
    return df


def add_computed_columns(df):
    """
    Add computed columns to the dataframe.

    Args:
        df (pandas.DataFrame): Input dataframe.

    Returns:
        pandas.DataFrame: Dataframe with additional computed columns.
    """
    logger.info("Adding computed columns")
    df['total_cost'] = df['quantity'] * df['price']
    logger.info("Added 'total_cost' column")
    return df


def run_data_quality_checks(df):
    """
    Run data quality checks using pandas validation.

    Args:
        df (pandas.DataFrame): Dataframe to validate.

    Returns:
        bool: True if all quality checks pass, False otherwise.
    """
    logger.info("Running data quality checks")
    
    # Check for null values in quantity
    quantity_null_check = df['quantity'].notnull().all()
    logger.info(f"Quality check for 'quantity' not null: {'PASSED' if quantity_null_check else 'FAILED'}")
    
    # Check for null values in order_date
    date_null_check = df['order_date'].notnull().all()
    logger.info(f"Quality check for 'order_date' not null: {'PASSED' if date_null_check else 'FAILED'}")
    
    # Additional data quality checks
    date_type_check = pd.api.types.is_datetime64_dtype(df['order_date'])
    logger.info(f"Quality check for 'order_date' type: {'PASSED' if date_type_check else 'FAILED'}")
    
    quantity_range_check = ((df['quantity'] >= 1) & (df['quantity'] <= 1000)).all()
    logger.info(f"Quality check for 'quantity' range: {'PASSED' if quantity_range_check else 'FAILED'}")
    
    price_range_check = ((df['price'] >= 0) & (df['price'] <= 10000)).all()
    logger.info(f"Quality check for 'price' range: {'PASSED' if price_range_check else 'FAILED'}")
    
    total_cost_check = df['total_cost'].notnull().all()
    logger.info(f"Quality check for 'total_cost' not null: {'PASSED' if total_cost_check else 'FAILED'}")
    
    # Print detailed results
    print("\nData Quality Check Results:")
    print(f"Quantity not null: {quantity_null_check}")
    print(f"Order date not null: {date_null_check}")
    print(f"Order date is datetime type: {date_type_check}")
    print(f"Quantity between 1 and 1000: {quantity_range_check}")
    print(f"Price between 0 and 10000: {price_range_check}")
    print(f"Total cost not null: {total_cost_check}")
    
    # Return overall result
    return all([quantity_null_check, date_null_check, date_type_check, 
                quantity_range_check, price_range_check, total_cost_check])


def save_cleaned_data(df, conn):
    """
    Save cleaned data back to PostgreSQL.

    Args:
        df (pandas.DataFrame): Cleaned dataframe to save.
        conn (psycopg2.connection): Database connection object.
    """
    cleaned_table = "ecommerce_orders_cleaned"
    logger.info(f"Saving cleaned data to {cleaned_table} table")
    try:
        # Create cursor
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {cleaned_table}")
        
        # Create table
        create_table_query = f"""
        CREATE TABLE {cleaned_table} (
            order_id VARCHAR(255),
            customer VARCHAR(255),
            order_date TIMESTAMP,
            product VARCHAR(255),
            quantity NUMERIC,
            price NUMERIC,
            total_cost NUMERIC
        )
        """
        cursor.execute(create_table_query)
        
        # Insert data
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {cleaned_table} (order_id, customer, order_date, product, quantity, price, total_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['order_id'],
                row['customer'],
                row['order_date'],
                row['product'],
                row['quantity'],
                row['price'],
                row['total_cost']
            ))
        
        # Commit changes
        conn.commit()
        
        # Close cursor
        cursor.close()
        
        logger.info(f"Successfully saved {len(df)} records to {cleaned_table}")
        print(f"Cleaned data written to PostgreSQL table: {cleaned_table}")
    except Exception as e:
        logger.error(f"Failed to save cleaned data: {str(e)}")
        raise


def extract_raw_data(**context):
    """Task to extract raw data from PostgreSQL."""
    conn = get_database_connection()
    df = extract_data(conn)
    conn.close()
    
    df['order_date'] = df['order_date'].astype(str)
    # Push the DataFrame to XCom
    context['task_instance'].xcom_push(key='raw_data', value=df.to_dict())
    return "Data extraction completed"


def clean_and_transform_data(**context):
    """Task to clean and transform the extracted data."""
    # Get DataFrame from XCom
    df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids='extract_raw_data', key='raw_data'))
    
    print("Debug: DataFrame after extraction")
    print(df.head())
    
    # Apply transformations
    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = standardize_formats(df)
    df = add_computed_columns(df)
    
    # Convert to string for XCom
    df['order_date'] = df['order_date'].astype(str)
    
    # Push cleaned data to XCom
    context['task_instance'].xcom_push(key='cleaned_data', value=df.to_dict())
    return "Data cleaning completed"


def validate_data_quality(**context):
    """Task to validate data quality."""
    df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids='clean_and_transform_data', key='cleaned_data'))
    
    # Convert order_date to datetime safely
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    quality_check_passed = run_data_quality_checks(df)
    
    if not quality_check_passed:
        raise ValueError("Data quality checks failed")
    
    # Convert to string for XCom
    df['order_date'] = df['order_date'].astype(str)
    
    context['task_instance'].xcom_push(key='validated_data', value=df.to_dict())
    return "Data validation completed"


def load_final_data(**context):
    """Task to load the cleaned and validated data back to PostgreSQL."""
    df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids='validate_data_quality', key='validated_data'))
    
    # Convert order_date to datetime safely
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    conn = get_database_connection()
    save_cleaned_data(df, conn)
    conn.close()
    return "Data loading completed"


# Define tasks
extract_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data,
    provide_context=True,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_and_transform_data',
    python_callable=clean_and_transform_data,
    provide_context=True,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_final_data',
    python_callable=load_final_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_task >> clean_task >> validate_task >> load_task
