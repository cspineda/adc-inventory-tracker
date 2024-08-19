import os
import re
import glob
import json
import uuid
import binascii
import urllib.request
import shopify
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import numpy as np
np.float_ = np.float64
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime, timedelta
from prophet import Prophet

load_dotenv()

token = os.getenv('TOKEN')
shop_url = os.getenv('MERCHANT')
api_key = os.getenv('API_KEY')
api_secret = os.getenv('API_SECRET')

shopify.Session.setup(api_key=api_key, secret=api_secret)

api_version = '2024-07'
session = shopify.Session(shop_url, api_version, token)
shopify.ShopifyResource.activate_session(session)

shop = shopify.Shop.current() # Get the current shop

LEAD_TIME = 21
BUFFER = 7
DATE_RANGE = 180
FORECAST_DAYS = 90

todays_date = datetime.now().date()
start_date = (today_dt - timedelta(days=DATE_RANGE)).date()

sku_mapper = {
    'AMP-01-03': 'Hair Ampoules Set',
    'AMP-01-04': 'Hair Ampoules Set',
    'CHA-01-16': 'Recover Me Shampoo',
    'MAS-02-16': 'Hydra Glow Mask',
    'LEA-01-08': 'Hair Hydrate',
    'GOT-01-08': 'Hair Energizer',
    'MAS-01-16': 'Nourishing Mask',
    'SER-01-04': 'Everything Serum',
    'CDG-01-08': 'Team Rizos Curl Defining Gel',
    'LAC-01-08': 'Team Lizos Straightener',
    'MAS-03-16': 'Protein Therapy Mask',
    'MAS-04-16': 'Anti Breakage Liquid Mask',
    'PRE-01-08': 'Honey Repair Pre-Wash'
}

inventory_results = execute_query(current_inventory_query)
inventory = extract_inventory_total(inventory_results)

order_results = execute_query(orders_query)
orders = extract_orders(order_results)


# load data
csv_files = glob.glob('data/*.csv')
orders = pd.DataFrame()

for csv_file in csv_files:
    orders_temp = pd.read_csv(csv_file)
    orders = pd.concat([orders, orders_temp])


# normalize columns
orders.columns = [c.lower().replace(' ', '_') for c in orders.columns]

orders['order_date'] = pd.to_datetime(orders['order_date']).dt.date
orders['sku'] = orders['sku'].apply(lambda x: x.rstrip('O') if isinstance(x, str) else x)

# orders.dropna(subset=['SKU'], inplace=True)  # remove references created by TikTok Shop
orders.sort_values(['order_date', 'order_timestamp'], inplace=True)

orders = remove_tiktok_shop_dups(orders)

skus = [sku for sku in orders.sku.unique() if sku in sku_mapper]

orders_timeseries = (
    orders.loc[orders['sku'].isin(skus)].groupby(['order_date', 'sku'])
    .agg({'quantity': "sum"})
    .reset_index()
    .rename(columns={'quantity': 'total'})
)

min_date, max_date = orders_timeseries.order_date.min(), orders_timeseries.order_date.max()

dates = pd.DataFrame(pd.date_range(min_date, max_date-timedelta(days=1), freq='d'), columns=['ds'])
dates['ds'] = pd.to_datetime(dates['ds']).dt.date


# full forecast data
forecast_full = pd.DataFrame(columns=['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'product_name', 'sku'])

# Only the forecasted results
forecast_reduced = pd.DataFrame(columns=['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'product_name', 'sku'])
                              

for sku in skus:
    print(f'Running forecast on {sku_mapper.get(sku)}')
    orders_timeseries_sku = (
        orders_timeseries.loc[(orders_timeseries['sku']==sku) & (orders_timeseries.order_date >= start_date), ['order_date', 'total']]
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
        fig = m.plot(forecast, xlabel='Date', ylabel='Total Products Sold')
        ax = fig.gca()
        ax.set_title(sku, size=24)
    except Exception as e:
        print(f'ERROR with {sku}:', e)
    print('\n')


forecast_reduced['yhat_cumsum'] = forecast_reduced.groupby(['product_name', 'sku'])['yhat'].cumsum()

forecast_reduced = forecast_reduced.merge(inventory, how='left', on='sku').rename(columns={'inventory': 'current_inventory'})

stockage = (
    forecast_reduced.loc[forecast_reduced.yhat_cumsum < forecast_reduced.current_inventory]
    .groupby('product_name')
    .size()
    .reset_index(name='days')
    .sort_values('days')
)

buffer_days = LEAD_TIME + BUFFER
for i, row in stockage.iterrows():
    if row.days < buffer_days:
        print('WARNING!')
        print(f"{row.product_name}: Expected to run out in {row.days} days, which is less than the buffer of {buffer_days}. Order now if you haven't already")
    elif row.days >= FORECAST_DAYS:
        print(f"{row.product_name}: Not forecasted to run out until {row.days}+ days, no action needed")
    else:
        print(f"{row.product_name}: Product forecasted to run out in {row.days} days, you should order in {row.days-buffer_days} days.")
    print('')

# Clear session
shopify.ShopifyResource.clear_session()