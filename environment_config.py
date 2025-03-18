# config/environment_config.py
import jinja2
import os

# Template for environment configuration
template_str = """
-- Configuration for {{ env }} environment
USE ROLE {{ role }};
USE WAREHOUSE {{ warehouse }};
USE DATABASE {{ database }};

-- Schema setup
CREATE SCHEMA IF NOT EXISTS {{ database }}.RAW_DOW30;
CREATE SCHEMA IF NOT EXISTS {{ database }}.HARMONIZED_DOW30;
CREATE SCHEMA IF NOT EXISTS {{ database }}.ANALYTICS_DOW30;

-- Warehouse configuration
ALTER WAREHOUSE {{ warehouse }} SET
WAREHOUSE_SIZE = '{{ warehouse_size }}'
AUTO_SUSPEND = {{ auto_suspend }}
AUTO_RESUME = TRUE;

-- File format for FRED Index Data
CREATE OR REPLACE FILE FORMAT {{ database }}.RAW_DOW30.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('');

-- Stage setup for data loading
CREATE OR REPLACE STAGE {{ database }}.RAW_DOW30.FRED_DATA_STAGE
URL = 'S3_path'
CREDENTIALS = (AWS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY'))
FILE_FORMAT = {{ database }}.RAW_DOW30.CSV_FORMAT;

-- Raw table structure
CREATE TABLE IF NOT EXISTS {{ database }}.RAW_DOW30.RAW_DOW30_STAGING (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT,
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Stream for change data capture
CREATE STREAM IF NOT EXISTS {{ database }}.RAW_DOW30.DOW30_STREAM
ON TABLE {{ database }}.RAW_DOW30.RAW_DOW30_STAGING;

-- Harmonized table structure
CREATE TABLE IF NOT EXISTS {{ database }}.HARMONIZED_DOW30.DOW30_HARMONIZED (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT,
    PRIMARY KEY (date, index_name)
);

-- Analytics tables
CREATE TABLE IF NOT EXISTS {{ database }}.ANALYTICS_DOW30.DAILY_PERFORMANCE (
    DATE DATE,
    INDEX_NAME STRING,
    VALUE FLOAT,
    DAILY_RETURN FLOAT,
    VOLATILITY FLOAT,
    PRIMARY KEY (DATE, INDEX_NAME)
);

CREATE TABLE IF NOT EXISTS {{ database }}.ANALYTICS_DOW30.WEEKLY_PERFORMANCE (
    WEEK_START DATE,
    INDEX_NAME STRING,
    AVG_VALUE FLOAT,
    WEEKLY_RETURN FLOAT,
    VOLATILITY FLOAT,
    PRIMARY KEY (WEEK_START, INDEX_NAME)
);

CREATE TABLE IF NOT EXISTS {{ database }}.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS (
    month DATE,
    index_name STRING,
    monthly_return FLOAT,
    PRIMARY KEY (month, index_name)
);
"""

# Environment configurations
environments = {
    'DEV': {
        'role': 'DEVELOPER_ROLE',
        'warehouse': 'DEV_WH',
        'database': 'FRED_INDEX_DATA_DEV',
        'warehouse_size': 'XSMALL',
        'auto_suspend': 60
    },
    'PROD': {
        'role': 'PROD_ROLE',
        'warehouse': 'PROD_WH',
        'database': 'FRED_INDEX_DATA',
        'warehouse_size': 'SMALL',
        'auto_suspend': 300
    }
}

# Generate configuration files
def generate_configs():
    template = jinja2.Template(template_str)
    # Create output directory if it doesn't exist
    os.makedirs('config/generated', exist_ok=True)
    
    # Generate config for each environment
    for env, config in environments.items():
        config_sql = template.render(env=env, **config)
        # Write configuration to file
        with open(f'config/generated/{env.lower()}_config.sql', 'w') as f:
            f.write(config_sql)
        print(f"Generated configuration for {env} environment")

if __name__ == "__main__":
    generate_configs()
