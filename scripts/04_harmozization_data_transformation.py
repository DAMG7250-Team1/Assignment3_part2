import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, date_trunc
import os
from datetime import datetime
import pandas as pd




def transform_dow30_data(session):
    # Read raw data
    raw_data = session.table("RAW_DOW30.RAW_DOW30_STAGING")

    # Transform data
    transformed_data = raw_data.select(
        col("date").cast("timestamp").alias("timestamp"),
        col("index_name"),
        col("index_value").cast("float").alias("value"),
        col("daily_return").cast("float"),
        col("monthly_return").cast("float")
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
    window_spec = snowpark.Window.partitionBy("index_name").orderBy("timestamp").rowsBetween(-29, 0)
    volatility_data = normalized_data.withColumn(
        "volatility",
        session.udf.calculate_volatility(col("normalized_value").over(window_spec))
    )

    # Save the final result
    volatility_data.write.mode("overwrite").save_as_table("DOW30_HARMONIZED_WITH_VOLATILITY")
    print("DOW30_HARMONIZED_WITH_VOLATILITY table created.")


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

session=create_session()
transform_dow30_data(session)