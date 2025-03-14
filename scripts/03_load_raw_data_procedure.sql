-- Create stored procedure for loading data from stage to raw table
CREATE OR REPLACE PROCEDURE FRED_INDEX_DATA.RAW_DOW30.LOAD_RAW_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Copy data from stage to raw table
    COPY INTO FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING
    FROM (
        SELECT
            TO_DATE($2) as date,
            $3::STRING as index_name,
            $1::FLOAT as index_value,
            $4::FLOAT as daily_return,
            $5::FLOAT as monthly_return
        FROM @FRED_INDEX_DATA.RAW_DOW30.FRED_DATA_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = 'FRED_INDEX_DATA.RAW_DOW30.CSV_FORMAT')
    PATTERN = '.*fred_daily_index_data.*[.]csv'
    ON_ERROR = 'CONTINUE';
    
    RETURN 'Data loaded successfully';
END;
$$;

