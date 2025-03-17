import pandas as pd
import boto3
from fredapi import Fred
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

fred = Fred(api_key=os.getenv('FRED_API_KEY'))

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

series_ids = {
    "NASDAQ": "NASDAQCOM",
    "S&P500": "SP500",
    "DOW": "DJIA"
}

start_date = "2020-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

def verify_s3_upload(bucket_name, file_name):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        logger.info(f"Successfully verified S3 upload: {bucket_name}/{file_name}")
        return True
    except s3_client.exceptions.ClientError:
        logger.error(f"Failed to verify S3 upload: {bucket_name}/{file_name}")
        return False

def get_index_data():
    try:
        all_data = []
        for index, series_id in series_ids.items():
            logger.info(f"Fetching data for {index} ({series_id})")
            data = fred.get_series(series_id, start_date, end_date)
            df = pd.DataFrame(data, columns=['value'])
            df['date'] = df.index.strftime('%Y-%m-%d')
            df['index'] = index
            df['daily_return'] = df['value'].pct_change()
            df['monthly_return'] = df['value'].pct_change(periods=30)
            all_data.append(df.reset_index(drop=True))
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Successfully fetched {len(combined_df)} rows of data")
        return combined_df
    except Exception as e:
        logger.error(f"Error fetching FRED data: {str(e)}")
        raise

def df_to_csv(df):
    try:
        df = df[['value', 'date', 'index', 'daily_return', 'monthly_return']]
        csv_data = df.to_csv(index=False)
        logger.info("Successfully converted DataFrame to CSV")
        return csv_data
    except Exception as e:
        logger.error(f"Error converting DataFrame to CSV: {str(e)}")
        raise

def upload_to_s3(data, bucket_name, file_name):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data
        )
        if verify_s3_upload(bucket_name, file_name):
            logger.info(f"Successfully uploaded data to S3: {bucket_name}/{file_name}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

def main():
    try:
        logger.info("Starting FRED data fetch process")
        index_data = get_index_data()
        csv_data = df_to_csv(index_data)
        bucket_name = 'damg7250-assignment3-part2'
        file_name = f'fred_daily_index_data_{datetime.now().strftime("%Y%m%d")}.csv'
        if upload_to_s3(csv_data, bucket_name, file_name):
            logger.info("Data pipeline completed successfully")
        else:
            logger.error("Data pipeline failed during S3 upload")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

