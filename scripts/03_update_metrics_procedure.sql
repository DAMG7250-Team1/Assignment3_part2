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
    
    # Process stream data with daily returns already calculated in CSV
    transformed_df = stream_df.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return")
    )
    
    # Apply normalization UDF to index values
    transformed_df = transformed_df.with_column(
        "index_value", 
        session.call_udf("FRED_INDEX_DATA.HARMONIZED_DOW30.NORMALIZE_INDEX_VALUE", col("index_value"))
    )
    
    # Insert into harmonized table
    transformed_df.write.mode("append").save_as_table("FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED")
    
    # Insert into daily analytics table
    daily_df = transformed_df.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return")
    )
    daily_df.write.mode("append").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_INDEX_METRICS")
    
    # Calculate and update monthly metrics
    session.sql("""
        MERGE INTO FRED_INDEX_DATA.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS target
        USING (
            SELECT
                DATE_TRUNC('MONTH', date) as month,
                index_name,
                MAX(monthly_return) as monthly_return
            FROM FRED_INDEX_DATA.RAW_DOW30.RAW_DOW30_STAGING
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
    
    # Calculate volatility and update analytics tables
    session.sql("""
        MERGE INTO FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_PERFORMANCE T
        USING (
            SELECT
                DATE,
                INDEX_NAME,
                INDEX_VALUE as VALUE,
                DAILY_RETURN,
                FRED_INDEX_DATA.HARMONIZED_DOW30.CALCULATE_VOLATILITY(
                    ARRAY_AGG(INDEX_VALUE) OVER (PARTITION BY INDEX_NAME ORDER BY DATE
                    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
                ) AS VOLATILITY
            FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
            WHERE DATE >= DATEADD(DAY, -30, CURRENT_DATE())
        ) S
        ON T.DATE = S.DATE AND T.INDEX_NAME = S.INDEX_NAME
        WHEN MATCHED THEN UPDATE SET
            T.VALUE = S.VALUE,
            T.DAILY_RETURN = S.DAILY_RETURN,
            T.VOLATILITY = S.VOLATILITY
        WHEN NOT MATCHED THEN INSERT
            (DATE, INDEX_NAME, VALUE, DAILY_RETURN, VOLATILITY)
        VALUES
            (S.DATE, S.INDEX_NAME, S.VALUE, S.DAILY_RETURN, S.VOLATILITY)
    """).collect()
    
    # Weekly aggregation
    session.sql("""
        MERGE INTO FRED_INDEX_DATA.ANALYTICS_DOW30.WEEKLY_PERFORMANCE T
        USING (
            SELECT
                DATE_TRUNC('WEEK', DATE) AS WEEK_START,
                INDEX_NAME,
                AVG(INDEX_VALUE) AS AVG_VALUE,
                (MAX(INDEX_VALUE) - MIN(INDEX_VALUE))/MIN(INDEX_VALUE) AS WEEKLY_RETURN,
                FRED_INDEX_DATA.HARMONIZED_DOW30.CALCULATE_VOLATILITY(ARRAY_AGG(INDEX_VALUE)) AS VOLATILITY
            FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
            WHERE DATE >= DATEADD(WEEK, -4, CURRENT_DATE())
            GROUP BY WEEK_START, INDEX_NAME
        ) S
        ON T.WEEK_START = S.WEEK_START AND T.INDEX_NAME = S.INDEX_NAME
        WHEN MATCHED THEN UPDATE SET
            T.AVG_VALUE = S.AVG_VALUE,
            T.WEEKLY_RETURN = S.WEEKLY_RETURN,
            T.VOLATILITY = S.VOLATILITY
        WHEN NOT MATCHED THEN INSERT
            (WEEK_START, INDEX_NAME, AVG_VALUE, WEEKLY_RETURN, VOLATILITY)
        VALUES
            (S.WEEK_START, S.INDEX_NAME, S.AVG_VALUE, S.WEEKLY_RETURN, S.VOLATILITY)
    """).collect()
    
    return "Data processed successfully"
$$;