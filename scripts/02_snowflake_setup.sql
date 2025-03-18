-- Snowflake SQL Setup

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS FRED_INDEX_DATA;
CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.RAW_DOW30;
CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.HARMONIZED_DOW30;
CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.ANALYTICS_DOW30;

-- Create file format for CSV
CREATE OR REPLACE FILE FORMAT FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('');

-- Create external stage with placeholders for credentials
CREATE OR REPLACE STAGE FRED_INDEX_DATA.RAW_DOW30.FRED_DATA_STAGE
    URL = '${S3_PATH}'
    CREDENTIALS = (AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}')
    FILE_FORMAT = FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT;

-- Create raw staging table
CREATE OR REPLACE TABLE FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT
);

-- Create stream for tracking changes
CREATE OR REPLACE STREAM FRED_INDEX_DATA.RAW_DOW30.DOW30_STREAM ON TABLE FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING;

-- Create harmonized table
CREATE OR REPLACE TABLE FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT
);

-- Create analytics tables
CREATE OR REPLACE TABLE FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_INDEX_METRICS (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT
);

CREATE OR REPLACE TABLE FRED_INDEX_DATA.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS (
    month DATE,
    index_name STRING,
    monthly_return FLOAT
);


CREATE OR REPLACE TABLE FRED_INDEX_DATA.ANALYTICS_DOW30.WEEKLY_INDEX_METRICS (
week DATE,
index_name STRING,
avg_index_value FLOAT,
avg_daily_return FLOAT
);

-- Create SQL UDF for data normalization
CREATE OR REPLACE FUNCTION FRED_INDEX_DATA.HARMONIZED_DOW30.NORMALIZE_INDEX_VALUE(value FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
CASE
    WHEN value IS NULL THEN 0
    WHEN value < 0 THEN 0
    ELSE value
END
$$;

-- Create Python UDF for volatility calculation
CREATE OR REPLACE FUNCTION FRED_INDEX_DATA.HARMONIZED_DOW30.CALCULATE_VOLATILITY(input_values ARRAY)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('numpy')
HANDLER = 'calculate_volatility'
AS
$$
import numpy as np

def calculate_volatility(input_values):
    if not input_values or len(input_values) < 2:
        return 0.0
    
    # Convert to numpy array and calculate returns
    values_array = np.array([float(v) for v in input_values if v is not None])
    returns = np.diff(values_array) / values_array[:-1]
    
    # Calculate standard deviation (volatility)
    return float(np.std(returns) * np.sqrt(252)) # Annualized
$$;

-- Create stored procedure for incremental updates using stream
CREATE OR REPLACE PROCEDURE FRED_INDEX_DATA.HARMONIZED_DOW30.UPDATE_DAILY_INDEX_METRICS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_data'
AS
$$
def process_data(session):
    # Ingest data from stream (only new/changed records)
    stream_df = session.table("FRED_INDEX_DATA.RAW_DOW30.DOW30_STREAM")
    
    if stream_df.count() == 0:
        return "No new data to process"
    
    # Transform data
    from snowflake.snowpark.functions import col, lag, partition, orderBy
    
    # Process stream data and calculate daily returns
    transformed_df = stream_df.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return"),
        col("monthly_return")
    )
    
    # Get existing data to calculate returns properly
    existing_df = session.table("FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED")
    
    # Combine existing and new data for proper lag calculation
    combined_df = existing_df.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return"),
        col("monthly_return")
    ).union(transformed_df)
    
    # Filter to only include new records from the stream
    new_records_df = result_df = transformed_df
    
    # Insert into harmonized table
    new_records_df.write.mode("append").save_as_table("FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED")
    
    # Insert into daily analytics table
    daily_df = new_records_df.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return")
    )
    daily_df.write.mode("append").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_INDEX_METRICS")
    
    # Calculate and update monthly metrics
    monthly_df = session.sql("""
        SELECT
            DATE_TRUNC('MONTH', date) as month,
            index_name,
            MAX(monthly_return) as monthly_return
        FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
        WHERE date >= DATE_TRUNC('MONTH', CURRENT_DATE())
        GROUP BY month, index_name
    """)
    
    # Merge into monthly analytics table (upsert)
    session.sql("""
        MERGE INTO FRED_INDEX_DATA.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS target
        USING (
            SELECT
                DATE_TRUNC('MONTH', date) as month,
                index_name,
                MAX(monthly_return) as monthly_return
            FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
            WHERE date >= DATE_TRUNC('MONTH', CURRENT_DATE())
            GROUP BY month, index_name
        ) source
        ON target.month = source.month AND target.index_name = source.index_name
        WHEN MATCHED THEN
            UPDATE SET target.monthly_return = source.monthly_return
        WHEN NOT MATCHED THEN
            INSERT (month, index_name, monthly_return)
            VALUES (source.month, source.index_name, source.monthly_return)
    """).collect()
    
    return "Data processed successfully"
$$;

-- Create tasks for automation
CREATE OR REPLACE TASK FRED_INDEX_DATA.RAW_DOW30.LOAD_DOW30_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 0 * * * America/New_York'
AS
    CALL FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA();

CREATE OR REPLACE TASK FRED_INDEX_DATA.RAW_DOW30.UPDATE_DOW30_METRICS_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER FRED_INDEX_DATA.RAW_DOW30.LOAD_DOW30_TASK
AS
    CALL FRED_INDEX_DATA.HARMONIZED_DOW30.UPDATE_DAILY_INDEX_METRICS();

-- Activate tasks
ALTER TASK FRED_INDEX_DATA.RAW_DOW30.UPDATE_DOW30_METRICS_TASK RESUME;
ALTER TASK FRED_INDEX_DATA.RAW_DOW30.LOAD_DOW30_TASK RESUME;
