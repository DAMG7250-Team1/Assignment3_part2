import pandas as pd
import boto3
from fredapi import Fred
from datetime import datetime
import json
import os
# FRED API setup
fred = Fred(api_key= os.getenv('FRED_API_KEY'))

# AWS S3 setup
s3_client = boto3.client('s3',
    aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')
)

# FRED series IDs for NASDAQ, S&P 500, and Dow Jones
series_ids = {
    "NASDAQ": "NASDAQCOM",
    "S&P500": "SP500",
    "DOW": "DJIA"
}

start_date = "2020-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

# Fetch and process data
def get_index_data():
    all_data = []
    for index, series_id in series_ids.items():
        data = fred.get_series(series_id, start_date, end_date)
        df = pd.DataFrame(data, columns=['value'])
        df['date'] = df.index
        df['index'] = index
        df['daily_return'] = df['value'].pct_change()
        df['monthly_return'] = df['value'].pct_change(periods=30)
        all_data.append(df.reset_index(drop=True))
    return pd.concat(all_data, ignore_index=True)

# Convert DataFrame to CSV
def df_to_csv(df):
    return df.to_csv(index=False)

# Upload data to S3
def upload_to_s3(data, bucket_name, file_name):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=data
    )

# Main execution
if __name__ == "__main__":
    # Fetch and process data
    index_data = get_index_data()
    
    # Convert DataFrame to CSV
    csv_data = df_to_csv(index_data)
    
    # Upload to S3
    bucket_name = 'damg7250-assignment3'
    file_name = f'fred_daily_index_data_{datetime.now().strftime("%Y%m%d")}.csv'
    upload_to_s3(csv_data, bucket_name, file_name)
    print(f"Data uploaded to S3: {bucket_name}/{file_name}")
 
# Snowflake setup (run these SQL commands in Snowflake)
"""
-- Create file format
CREATE OR REPLACE FILE FORMAT my_json_format
    TYPE = 'JSON';
 
-- Create external stage
CREATE OR REPLACE STAGE fred_data_stage
    URL = 's3://damg7250-assignment3/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY' AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY')
    FILE_FORMAT = my_json_format;
 
-- Create external table
CREATE OR REPLACE EXTERNAL TABLE fred_daily_index_data (
    date DATE,
    index STRING,
    value FLOAT,
    daily_return FLOAT,
    monthly_return FLOAT
)
LOCATION = @fred_data_stage
FILE_FORMAT = my_json_format
PATTERN = '.*fred_daily_index_data.*[.]json';
 
-- Query the external table
SELECT * FROM fred_daily_index_data LIMIT 10;
"""