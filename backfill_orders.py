# import os
# import json
# import shopify
# from dotenv import load_dotenv
# import numpy as np
# np.float_ = np.float64
# import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)
#
# from utils.data import generate_orders_table
#
#
# load_dotenv()
#
# token = os.getenv('TOKEN')
# shop_url = os.getenv('MERCHANT')
# api_key = os.getenv('API_KEY')
# api_secret = os.getenv('API_SECRET')
#
# shopify.Session.setup(api_key=api_key, secret=api_secret)
#
# api_version = '2024-07'
# session = shopify.Session(shop_url, api_version, token)
# shopify.ShopifyResource.activate_session(session)
#
#
# orders_query = """
# query {
#   orders(first: 250, query:"created_at:>='2024-01-01T00:00:00Z' AND created_at:<'2024-04-01T00:00:00Z'", reverse:false, sortKey: CREATED_AT) {
#     edges {
#       node {
#         id
#         name
#         createdAt
#         lineItems(first: 100) {
#           edges{
#             node {
#               name
#               sku
#               quantity
#             }
#           }
#         }
#       }
#     }
#     pageInfo {
#       hasNextPage
#     }
#   }
# }
# """
# orders_query_results = shopify.GraphQL().execute(orders_query)
# orders = json.loads(orders_query_results)
# orders = generate_orders_table(orders)
#
# filename = 'orders_q1.xlsx'
# orders.to_csv(filename, index=None)


# # create an S3 resource
# s3 = boto3.resource('s3')
# # Upload a new file
# data = json.dumps('hi')
# s3.Bucket('agua-de-cielo-shopify-orders').put_object(Key='data/daily_orders.csv', Body=data)


from io import StringIO # python3; python2: BytesIO
import boto3
import pandas as pd


df1 = pd.read_csv('/Users/cspineda/Documents/vp/apps/inventory-forecaster/adc-inventory-tracker/orders_q1.xlsx')
df2 = pd.read_csv('/Users/cspineda/Documents/vp/apps/inventory-forecaster/adc-inventory-tracker/orders_q2.xlsx')
df3 = pd.read_csv('/Users/cspineda/Documents/vp/apps/inventory-forecaster/adc-inventory-tracker/orders_q3.xlsx')

df = pd.concat([df1, df2])
df = pd.concat([df, df3])

bucket = 'agua-de-cielo-shopify-orders'
csv_buffer = StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'data/daily_orders.csv').put(Body=csv_buffer.getvalue())