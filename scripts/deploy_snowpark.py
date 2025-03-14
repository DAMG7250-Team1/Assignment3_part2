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
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "HARMONIZED_DOW30")
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
        
        # Rename columns to match table structure
        snowpark_df = snowpark_df.select(
            col("date").cast("DATE"),
            col("index_name").alias("index_name"),
            col("value").alias("index_value"),
            col("daily_return"),
            col("monthly_return")
        )
        
        # Write to raw staging table
        snowpark_df.write.mode("append").save_as_table("RAW_DOW30_STAGING")
        print("Data loaded successfully to RAW_DOW30_STAGING")
        
        # Execute the stored procedures
        session.sql("CALL FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA()").collect()
        session.sql("CALL FRED_INDEX_DATA.HARMONIZED_DOW30.UPDATE_DAILY_INDEX_METRICS()").collect()
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

        # Deploy normalize_exchange_rate UDF
        with open('scripts/normalize_exchange_rate_udf/normalize_exchange_rate_udf/function.sql', 'r') as f:
            normalize_udf_sql = f.read()
        session.sql(normalize_udf_sql).collect()
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
            volatility = np.std(returns) * np.sqrt(252)  # Annualized
            
            return float(round(volatility, 4))

        session.udf.register(
            func=calculate_volatility,
            name="CALCULATE_VOLATILITY",
            stage_location="@FRED_INDEX_DATA.HARMONIZED_DOW30.DEPLOYMENT",
            is_permanent=True,
            packages=['numpy', 'cloudpickle'],  # Use latest compatible versions
            replace=True,
            return_type=snowpark.types.FloatType(),
            input_types=[snowpark.types.ArrayType(snowpark.types.FloatType())],
            runtime_version='3.10'  # Specify Python runtime version
        )
        print("✓ Deployed calculate_stock_volatility UDF")

    except Exception as e:
        print(f"Error deploying UDFs: {str(e)}")
        raise

def deploy_procedures(session):
    """Deploy stored procedures"""
    try:
        # Deploy update metrics procedure
        with open('scripts/03_update_metrics_procedure.sql', 'r') as f:
            update_metrics_code = f.read()
        # Execute the entire procedure creation as one statement
        session.sql(update_metrics_code).collect()
        print("✅ Successfully deployed update metrics procedure")
        
        # Deploy load raw data procedure
        with open('scripts/03_load_raw_data_procedure.sql', 'r') as f:
            load_raw_data_code = f.read()
        # Execute the entire procedure creation as one statement
        session.sql(load_raw_data_code).collect()
        print("✅ Successfully deployed load raw data procedure")
        
    except Exception as e:
        print(f"❌ Error deploying procedures: {str(e)}")
        raise

def deploy_transformations(session):
    """Deploy data transformation logic"""
    try:
        # Create a temporary stage for the transformation code
        session.sql("""
        CREATE OR REPLACE STAGE FRED_INDEX_DATA.HARMONIZED_DOW30.TRANSFORM_STAGE
        """).collect()
        
        # Read the transformation code
        with open('scripts/04_harmozization_data_transformation.py', 'r') as f:
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

    except Exception as e:
        print(f"Error deploying transformations: {str(e)}")
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
        
        # Deploy components
        deploy_udfs(session)
        deploy_procedures(session)
        deploy_transformations(session)
        
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