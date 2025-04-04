CREATE DATABASE IF NOT EXISTS FRED_INDEX_DATA;

CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.RAW_DOW30;
CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.HARMONIZED_DOW30;
CREATE SCHEMA IF NOT EXISTS FRED_INDEX_DATA.ANALYTICS_DOW30;

CREATE OR REPLACE FILE FORMAT FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('', 'NULL');

CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::872515278213:role/s3-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://damg7250-assignment3-part2/');

CREATE OR REPLACE STAGE FRED_INDEX_DATA.RAW_DOW30.FRED_DATA_STAGE
URL = 's3://damg7250-assignment3-part2/'
STORAGE_INTEGRATION = S3_INTEGRATION
FILE_FORMAT = FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT;

DESC INTEGRATION

CREATE OR REPLACE TABLE FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING (
    index_value FLOAT,
    date DATE,
    index_name VARCHAR,
    daily_return FLOAT,
    monthly_return FLOAT
);

CREATE OR REPLACE STREAM FRED_INDEX_DATA.RAW_DOW30.DOW30_STREAM ON TABLE FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING APPEND_ONLY = TRUE;

CREATE OR REPLACE TABLE FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED (
    date DATE,
    index_name STRING,
    index_value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT
);

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


CREATE OR REPLACE PROCEDURE FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Copy data from the stage into the raw staging table
    COPY INTO FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING 
    FROM @FRED_INDEX_DATA.RAW_DOW30.FRED_DATA_STAGE 
    FILE_FORMAT = (FORMAT_NAME = 'FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT') 
    ON_ERROR = 'CONTINUE';

    -- Return success message
    RETURN 'Data loaded successfully from stage';
END;
$$;

CALL FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA();


COPY INTO FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING 
    FROM @FRED_INDEX_DATA.RAW_DOW30.FRED_DATA_STAGE