import pytest
import os

def test_environment_variables():
    """Test if all required environment variables are set"""
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'S3_PATH',
        'FRED_API_KEY'
    ]
    
    for var in required_vars:
        assert os.getenv(var) is not None, f"Environment variable {var} is not set" 