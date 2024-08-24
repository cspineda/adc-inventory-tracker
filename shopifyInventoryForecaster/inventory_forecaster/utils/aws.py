import pandas as pd
from io import StringIO
from utils.logger import get_logger


logger = get_logger()


def read_csv_from_s3(boto3, bucket, key, **kwargs):
    obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    data = StringIO(obj['Body'].read().decode('utf-8'))
    df = pd.read_csv(data, **kwargs)
    logger.info(f'Loaded file from {bucket}{key}')
    return df


def save_df_to_s3(df, bucket, key, s3, **kwargs):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, **kwargs)
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())
    logger.info(f'Saved to {bucket}/{key}')