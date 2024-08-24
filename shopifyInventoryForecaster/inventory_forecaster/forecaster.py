import os
import boto3
import json
import shopify
import pandas as pd
from dotenv import load_dotenv
import numpy as np
np.float_ = np.float64
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime, timedelta
from prophet import Prophet

from .utils.queries import inventory_query, orders_query
from .utils.data import extract_inventory_total, generate_orders_table
from .utils.namespace import sku_mapper
from .utils.aws import save_df_to_s3, read_csv_from_s3
from .utils.logger import get_logger


logger = get_logger()

load_dotenv()

# Load environment variables
token = os.environ['TOKEN']
shop_url = os.environ['MERCHANT']
api_key = os.environ['API_KEY']
api_secret = os.environ['API_SECRET']
bucket = os.environ['BUCKET']

s3 = boto3.resource('s3')

shopify.Session.setup(api_key=api_key, secret=api_secret)

api_version = '2024-07'
session = shopify.Session(shop_url, api_version, token)
shopify.ShopifyResource.activate_session(session)

shop = shopify.Shop.current() # Get the current shop

LEAD_TIME = 21
BUFFER = 7
DATE_RANGE = 180
FORECAST_DAYS = 90
BUFFER_DAYS = LEAD_TIME + BUFFER

# Date variables and strings to be used for filtering
todays_date = datetime.now().date()
today_dt = datetime.today()

start_date_dt = (today_dt - timedelta(days=DATE_RANGE))
start_date = (start_date_dt.date())
start_date_filter = (today_dt - timedelta(days=1)).date().strftime('%Y-%m-%d')

end_date_filter = todays_date.strftime('%Y-%m-%d')

# Get orders data
logger.info(f"Querying orders from {start_date_filter} to {end_date_filter}")
orders_query = orders_query.replace('STARTDATE', start_date_filter).replace('ENDDATE', end_date_filter)
orders_query_results = shopify.GraphQL().execute(orders_query)
orders = json.loads(orders_query_results)
orders = generate_orders_table(orders)

# Save daily data to
year, month, day = start_date_filter.split('-')
output_path = f'data/YEAR={year}/MONTH={month}/DAY={day}/daily_orders.csv'
save_df_to_s3(orders, bucket, output_path, s3, index=None)

# import rest of orders
orders_full = read_csv_from_s3(boto3, bucket='agua-de-cielo-shopify-orders', key='model/data/input/daily_orders.csv')

orders = pd.concat([orders_full, orders])
orders['order_date'] = pd.to_datetime(orders['order_date'])

# Save full dataframe to S3
save_df_to_s3(df=orders, bucket=bucket, key='model/data/input/daily_orders.csv', s3=s3, index=None)

# Get inventory data
inventory_query_results = shopify.GraphQL().execute(inventory_query)
inventory = json.loads(inventory_query_results)
inventory = extract_inventory_total(inventory)

# Clear session
shopify.ShopifyResource.clear_session()

skus = [sku for sku in orders.sku.unique() if sku in sku_mapper]

orders_timeseries = (
    orders.loc[(orders.sku.isin(skus)) & (orders.order_date >= start_date_dt)].groupby(['order_date', 'sku'])
    .agg({'quantity': "sum"})
    .reset_index()
    .rename(columns={'quantity': 'total'})
)


def forecaster(orders_timeseries, inventory, skus, start_date, end_date):

    dates = pd.DataFrame(pd.date_range(start_date, end_date-timedelta(days=1), freq='d'), columns=['ds'])
    dates['ds'] = pd.to_datetime(dates['ds'])#.dt.date

    # full forecast data
    forecast_full = pd.DataFrame(columns=['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'product_name', 'sku'])

    # Only the forecasted results
    forecast_reduced = pd.DataFrame(columns=['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'product_name', 'sku'])

    for sku in skus:
        logger.info(f'Running forecast on {sku_mapper.get(sku)}')
        orders_timeseries_sku = (
            orders_timeseries.loc[orders_timeseries['sku']==sku, ['order_date', 'total']]
            .rename(columns={'order_date': 'ds', 'total': 'y'})
        ).copy()

        try:
            # Add missing dates and fill total with 0
            orders_timeseries_sku = dates.merge(orders_timeseries_sku, how='left', on=['ds'])
            orders_timeseries_sku.y.fillna(0, inplace=True)

            # Convert y to log(y) to avoid negative values
            orders_timeseries_sku['y'] = orders_timeseries_sku.y.apply(lambda x: np.log(x+1))

            # Fit model
            m = Prophet()
            m.fit(orders_timeseries_sku)

            # Make predictions
            future = m.make_future_dataframe(periods=FORECAST_DAYS)
            forecast = m.predict(future)

            # Convert log(y) back to y
            forecast['yhat'] = forecast.yhat.apply(lambda x: np.exp(x)-1)

            forecast = forecast.loc[:, ['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            forecast['product_name'] = sku_mapper.get(sku)
            forecast['sku'] = sku

            # append full forecast data
            forecast_full = pd.concat([forecast_full, forecast])

            # append only the forecasted rows
            forecast_reduced = pd.concat([forecast_reduced, forecast.iloc[-FORECAST_DAYS:]])

            # Plot forecast
            # fig = m.plot(forecast, xlabel='Date', ylabel='Total Products Sold')
            # ax = fig.gca()
            # ax.set_title(sku, size=24)
        except Exception as e:
            logger.info(f'ERROR with {sku}:', e)
        logger.info('\n')

    forecast_reduced['yhat_cumsum'] = forecast_reduced.groupby(['product_name', 'sku'])['yhat'].cumsum()

    forecast_reduced = forecast_reduced.merge(inventory, how='left', on='sku').rename(columns={'inventory': 'current_inventory'})

    stockage = (
        forecast_reduced.loc[forecast_reduced.yhat_cumsum < forecast_reduced.current_inventory]
        .groupby('product_name')
        .size()
        .reset_index(name='days')
        .sort_values('days')
    )

    stockage_dict = dict(zip(stockage.product_name, stockage.days))
    stockage_text = "\n"

    for i, row in stockage.iterrows():
        if row.days < BUFFER_DAYS:
            stockage_text += 'WARNING!\n'
            stockage_text +=  f"{row.product_name:<30s}- Expected to run out in {row.days} days, which is less than the buffer of {BUFFER_DAYS}. Order now if you haven't already\n"
        elif row.days >= FORECAST_DAYS:
            stockage_text += f"{row.product_name:<30s}- Not forecasted to run out until {row.days}+ days, no action needed\n"
        else:
            stockage_text += f"{row.product_name:<30s}- Product forecasted to run out in {row.days} days, you should order in {row.days-BUFFER_DAYS} days.\n"

    return stockage_text


def main():
    results = forecaster(orders_timeseries, inventory, skus, start_date, today_dt)
    return results


if __name__ == "__main__":
    logger.info(main())