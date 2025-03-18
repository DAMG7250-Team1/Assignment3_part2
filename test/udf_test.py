from snowflake.snowpark import Session
import os

# Connection parameters
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

# Ensure no None values before connecting
missing_keys = [key for key, value in connection_parameters.items() if value is None]
if missing_keys:
    raise ValueError(f"Missing environment variables: {', '.join(missing_keys)}")

# Create a Snowflake Snowpark session
session = Session.builder.configs(connection_parameters).create()

# Test SQL UDF
def test_normalize_exchange_rate():
    result = session.sql("SELECT FRED_INDEX_DATA.HARMONIZED_DOW30.NORMALIZE_EXCHANGE_RATE(1.5)").collect()
    print("Normalize Exchange Rate Result:", result[0][0])

# Test Python UDF
def test_calculate_stock_volatility():
    result = session.sql("SELECT FRED_INDEX_DATA.HARMONIZED_DOW30.CALCULATE_STOCK_VOLATILITY_UDF(ARRAY_CONSTRUCT(100, 102, 98, 103, 101))").collect()
    print("Calculate Stock Volatility Result:", result[0][0])

# Run tests
if __name__ == "__main__":
    test_normalize_exchange_rate()
    test_calculate_stock_volatility()
    
    # Close the session
    session.close()
