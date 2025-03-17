import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, date_trunc
import os
from datetime import datetime
import pandas as pd
from fredapi import Fred
 
def create_session():
    """Create Snowflake session using environment variables"""
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "FRED_INDEX_DATA"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DOW30")
    }
    return snowpark.Session.builder.configs(connection_parameters).create()
 
def fetch_fred_data():
    """Fetch data from FRED API"""
    fred = Fred(api_key=os.getenv('FRED_API_KEY'))
    series_ids = {
        "NASDAQ": "NASDAQCOM",
        "S&P500": "SP500",
        "DOW": "DJIA"
    }
    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    all_data = []
    for index, series_id in series_ids.items():
        data = fred.get_series(series_id, start_date, end_date)
        df = pd.DataFrame(data, columns=['value'])
        df['date'] = df.index
        df['index_name'] = index
        df['daily_return'] = df['value'].pct_change()
        df['monthly_return'] = df['value'].pct_change(periods=30)
        all_data.append(df.reset_index(drop=True))
    return pd.concat(all_data, ignore_index=True)
 
def load_data(session, df):
    """Load data into Snowflake"""
    try:
        # Convert pandas DataFrame to Snowpark DataFrame
        snowpark_df = session.create_dataframe(df)
        # Write to raw staging table
        snowpark_df.write.mode("append").save_as_table("RAW_DOW30_STAGING")
        print("Data loaded successfully to RAW_DOW30_STAGING")
        # Execute the stored procedures
        session.sql("CALL FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA()").collect()
        print("Data processing procedures executed successfully")
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise
 
def deploy_udfs(session):
    """Deploy all UDFs"""
    try:
        # Create deployment stage if it doesn't exist
        session.sql("""
        CREATE OR REPLACE STAGE FRED_INDEX_DATA.HARMONIZED_DOW30.DEPLOYMENT
        """).collect()
        print("✓ Created deployment stage")
 
        # Deploy normalize_exchange_rate UDF directly as a Python UDF
        def normalize_exchange_rate(value):
            """Normalizes exchange rate values to a standard scale"""
            if value is None or value == 0:
                return 0.0
            return float(value / abs(value))
       
        session.udf.register(
            func=normalize_exchange_rate,
            name="NORMALIZE_EXCHANGE_RATE",
            stage_location="@FRED_INDEX_DATA.HARMONIZED_DOW30.DEPLOYMENT",
            is_permanent=True,
            replace=True,
            return_type=snowpark.types.FloatType(),
            input_types=[snowpark.types.FloatType()],
            runtime_version='3.10'
        )
        print("✓ Deployed normalize_exchange_rate UDF")
 
        # Deploy calculate_stock_volatility UDF
        def calculate_volatility(prices):
            import numpy as np
            if not prices or len(prices) < 2:
                return 0.0
            # Convert to numpy array and calculate returns
            prices_array = np.array([float(p) for p in prices if p is not None])
            returns = np.log(prices_array[1:] / prices_array[:-1])
            # Calculate standard deviation (volatility)
            volatility = np.std(returns) * np.sqrt(252) # Annualized
            return float(round(volatility, 4))
 
        session.udf.register(
            func=calculate_volatility,
            name="CALCULATE_VOLATILITY",
            stage_location="@FRED_INDEX_DATA.HARMONIZED_DOW30.DEPLOYMENT",
            is_permanent=True,
            packages=['numpy', 'cloudpickle'], # Use latest compatible versions
            replace=True,
            return_type=snowpark.types.FloatType(),
            input_types=[snowpark.types.ArrayType(snowpark.types.FloatType())],
            runtime_version='3.10' # Specify Python runtime version
        )
        print("✓ Deployed calculate_stock_volatility UDF")
    except Exception as e:
        print(f"Error deploying UDFs: {str(e)}")
        raise
 
def deploy_procedures(session):
    """Deploy stored procedures"""
    try:
        # Deploy update metrics procedure
        with open('./scripts/03_update_metrics_procedure.sql', 'r') as f:
            update_metrics_code = f.read()
        # Execute the entire procedure creation as one statement
        session.sql(update_metrics_code).collect()
        print("✅ Successfully deployed update metrics procedure")
 
        # Deploy load raw data procedure
        with open('./scripts/03_load_raw_data_procedure.sql', 'r') as f:
            load_raw_data_code = f.read()
        # Execute the entire procedure creation as one statement
        session.sql(load_raw_data_code).collect()
        print("✅ Successfully deployed load raw data procedure")
    except Exception as e:
        print(f"❌ Error deploying procedures: {str(e)}")
        raise
 
def deploy_and_execute_transformations(session):
    """Deploy and execute data transformation logic"""
    try:
        # Create a temporary stage for the transformation code
        session.sql("""
        CREATE OR REPLACE STAGE FRED_INDEX_DATA.HARMONIZED_DOW30.TRANSFORM_STAGE
        """).collect()
 
        # Read the transformation code
        with open('./scripts/04_harmozization_data_transformation.py', 'r') as f:
            transform_code = f.read()
 
        # Create a temporary file with the transformation code in the current directory
        temp_file = 'harmonization_transform.py'
        with open(temp_file, 'w') as f:
            f.write(transform_code)
 
        # Get the absolute path and normalize it for Windows
        file_path = os.path.abspath(temp_file).replace('\\', '/')
 
        # Upload the file to stage using SQL
        session.sql(f"""
        PUT 'file://{file_path}' @FRED_INDEX_DATA.HARMONIZED_DOW30.TRANSFORM_STAGE
        OVERWRITE = TRUE
        AUTO_COMPRESS = FALSE
        """).collect()
        print("✓ Deployed data transformation logic")
 
        # Create a stored procedure to execute the transformation
        # Create the stored procedure with correct indentation
        session.sql("""
    CREATE OR REPLACE PROCEDURE FRED_INDEX_DATA.HARMONIZED_DOW30.RUN_HARMONIZATION()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'transform_dow30_data'
    AS
    $$
from snowflake.snowpark.functions import col
import snowflake.snowpark.window as snowpark_window
 
def transform_dow30_data(session):
    try:
        # Read raw data
        raw_data = session.table("RAW_DOW30.RAW_DOW30_STAGING")
 
        # Transform data
        transformed_data = raw_data.select(
            col("date").cast("timestamp").alias("timestamp"),
            col("index_name"),
            col("index_value").cast("float").alias("value"),
            col("daily_return").cast("float").alias("daily_return"),
            col("monthly_return").cast("float").alias("monthly_return")
        )
 
        # Remove duplicates
        transformed_data = transformed_data.dropDuplicates(["timestamp", "index_name"])
 
        # Save the harmonized data
        transformed_data.write.mode("overwrite").save_as_table("DOW30_HARMONIZED")
        print("DOW30_HARMONIZED table created.")
 
        # Apply UDFs
        normalized_data = transformed_data.withColumn(
            "normalized_value",
            session.udf.FRED_INDEX_DATA.HARMONIZED_DOW30.NORMALIZE_EXCHANGE_RATE(col("value"))
        )
 
        # Calculate volatility (assuming we want to calculate it over a 30-day window)
        window_spec = snowpark_window.Window.partitionBy("index_name").orderBy("timestamp").rowsBetween(-29, 0)
        volatility_data = normalized_data.withColumn(
            "volatility",
            session.udf.calculate_volatility(col("normalized_value").over(window_spec))
        )
 
        # Save the final result
        volatility_data.write.mode("overwrite").save_as_table("DOW30_HARMONIZED_WITH_VOLATILITY")
        print("DOW30_HARMONIZED_WITH_VOLATILITY table created.")
 
        return "Data transformation completed successfully"
   
    except Exception as e:
        return f"Data transformation failed: {str(e)}"
    $$
    """).collect()
 
        # Execute the transformation
        print("Executing data harmonization...")
        session.sql("CALL FRED_INDEX_DATA.HARMONIZED_DOW30.RUN_HARMONIZATION()").collect()
        print("✓ Data harmonization executed successfully")
    except Exception as e:
        print(f"Error in transformation deployment or execution: {str(e)}")
        raise
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file):
            os.remove(temp_file)
 
def create_harmonization_task(session):
    """Create a task to run the harmonization procedure"""
    try:
        session.sql(f"""
        CREATE OR REPLACE TASK FRED_INDEX_DATA.HARMONIZED_DOW30.HARMONIZE_DATA_TASK
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = 'USING CRON 15 0 * * * America/New_York'
        AS
        CALL FRED_INDEX_DATA.HARMONIZED_DOW30.RUN_HARMONIZATION();
        """).collect()
        # Activate the task
        session.sql("ALTER TASK FRED_INDEX_DATA.HARMONIZED_DOW30.HARMONIZE_DATA_TASK RESUME;").collect()
        print("✓ Harmonization task created and activated")
    except Exception as e:
        print(f"Error creating harmonization task: {str(e)}")
        raise
 
def deploy_analytics_transformation(session):
    """Deploy and execute analytics data transformation"""
    try:
        # Create a temporary stage for the analytics transformation code
        session.sql("""
        CREATE OR REPLACE STAGE FRED_INDEX_DATA.ANALYTICS_DOW30.ANALYTICS_STAGE
        """).collect()
       
        # Read the analytics transformation code
        with open('./scripts/05_analytics_table.py', 'r') as f:
            analytics_code = f.read()
           
        # Create a temporary file with the transformation code
        temp_file = 'analytics_transform.py'
        with open(temp_file, 'w') as f:
            f.write(analytics_code)
           
        # Get the absolute path and normalize it for Windows
        file_path = os.path.abspath(temp_file).replace('\\', '/')
       
        # Upload the file to stage using SQL
        session.sql(f"""
        PUT 'file://{file_path}' @FRED_INDEX_DATA.ANALYTICS_DOW30.ANALYTICS_STAGE
        OVERWRITE = TRUE
        AUTO_COMPRESS = FALSE
        """).collect()
       
        print("✓ Deployed analytics transformation logic")
       
        # Create a stored procedure to execute the transformation
        session.sql("""
    CREATE OR REPLACE PROCEDURE FRED_INDEX_DATA.ANALYTICS_DOW30.RUN_ANALYTICS()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'load_analytics_data'
    AS
    $$
from snowflake.snowpark.functions import col

def load_analytics_data(session):
        # Read harmonized data
    harmonized_data = session.table("FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED")

    # Transform data for daily analytics
    daily_data = harmonized_data.select(
        col("timestamp").cast("date").alias("date"),
        col("index_name"),
        col("value").alias("index_value"),
        col("daily_return")
    ).dropDuplicates(["date", "index_name"])

    # Save to daily analytics table
    daily_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_INDEX_METRICS")
    print("DAILY_INDEX_METRICS table updated.")

    # Transform data for monthly analytics using SQL for date aggregation
    monthly_data = session.sql(\"\"\"
        SELECT 
            DATE_TRUNC('MONTH', timestamp)::DATE AS month,
            index_name,
            MAX(monthly_return) AS monthly_return
        FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
        GROUP BY month, index_name
    \"\"\")

    # Save to monthly analytics table
    monthly_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS")
    print("MONTHLY_INDEX_METRICS table updated.")

    # Transform data for weekly analytics
    weekly_data = session.sql(\"\"\"
        SELECT 
            DATE_TRUNC('WEEK', timestamp)::DATE AS week,
            index_name,
            AVG(value) AS avg_index_value,
            AVG(daily_return) AS avg_daily_return
        FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
        GROUP BY week, index_name
    \"\"\")

    # Save to weekly analytics table
    weekly_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.WEEKLY_INDEX_METRICS")
    print("WEEKLY_INDEX_METRICS table updated.")

    return "Analytics tables successfully updated"
    $$
""").collect()

       
        # Execute the analytics transformation
        print("Executing analytics data transformation...")
        session.sql("CALL FRED_INDEX_DATA.ANALYTICS_DOW30.RUN_ANALYTICS()").collect()
        print("✓ Analytics data transformation executed successfully")
       
    except Exception as e:
        print(f"Error in analytics transformation deployment or execution: {str(e)}")
        raise
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file):
            os.remove(temp_file)
 
 
def main():
    """Main execution function"""
    try:
        print("Starting Snowpark deployments...")
        # Create Snowflake session
        session = create_session()
        print("✓ Connected to Snowflake")
 
        # Add this section to fetch and load FRED data
        print("Fetching data from FRED API...")
        data_df = fetch_fred_data()
        print(f"✓ Retrieved {len(data_df)} rows of data")
        print("Loading data to Snowflake...")
        load_data(session, data_df)
        print("✓ Data loaded successfully")
 
        # Deploy components
        deploy_udfs(session)
        deploy_procedures(session)
        deploy_and_execute_transformations(session) # Updated to deploy AND execute
        create_harmonization_task(session) # Create a task for scheduled harmonization
        deploy_analytics_transformation(session)
        # create_analytics_task(session)
 
        # Close session
        session.close()
        print("✓ Deployments completed successfully")
    except Exception as e:
        print(f"❌ Error in deployment: {str(e)}")
        raise
    finally:
        if 'session' in locals():
            session.close()
 
if __name__ == "__main__":
    main()