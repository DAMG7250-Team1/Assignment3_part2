def load_analytics_data(session):
    # Read harmonized data
    harmonized_data = session.table("FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED")
    
    # Transform data for daily analytics
    daily_data = harmonized_data.select(
        col("date"),
        col("index_name"),
        col("index_value"),
        col("daily_return")
    )
    
    # Remove duplicates for daily data
    daily_data = daily_data.dropDuplicates(["date", "index_name"])
    
    # Save to daily analytics table
    daily_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.DAILY_INDEX_METRICS")
    print("DAILY_INDEX_METRICS table updated.")
    
    # Transform data for monthly analytics using SQL for date aggregation
    monthly_data = session.sql("""
        SELECT 
            DATE_TRUNC('MONTH', date) as month,
            index_name,
            MAX(monthly_return) as monthly_return
        FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
        GROUP BY month, index_name
    """)
    
    # Save to monthly analytics table
    monthly_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.MONTHLY_INDEX_METRICS")
    print("MONTHLY_INDEX_METRICS table updated.")
    
    # Transform data for weekly analytics
    weekly_data = session.sql("""
        SELECT 
            DATE_TRUNC('WEEK', date) as week,
            index_name,
            AVG(index_value) as avg_index_value,
            AVG(daily_return) as avg_daily_return
        FROM FRED_INDEX_DATA.HARMONIZED_DOW30.DOW30_HARMONIZED
        GROUP BY week, index_name
    """)
    
    # Save to weekly analytics table
    weekly_data.write.mode("overwrite").save_as_table("FRED_INDEX_DATA.ANALYTICS_DOW30.WEEKLY_INDEX_METRICS")
    print("WEEKLY_INDEX_METRICS table updated.")
    
    # Calculate additional metrics if needed
    # For example, you could calculate volatility over different time periods
    
    # Return success message
    return "Analytics tables successfully updated"
