import re
import uuid
import pandas as pd
from datetime import datetime


def extract_inventory_total(query_results):
    products = query_results['data']['productVariants']['edges']
    return pd.DataFrame({
        product['node']['sku']: product['node']['inventoryQuantity'] 
        for product in products
    }.items(), columns=['sku', 'inventory'])


def remove_tiktok_shop_dups(df):
    dup_orders_full = [order for order in df['order_number'].unique() if re.search('[a-zA-Z]', str(order)) ]
    dup_order_num = [order[:-1] for order in dup_orders_full]

    dup_df = df.copy()
    dup_df.loc[(dup_df['order_number'].isin(dup_orders_full+dup_order_num)) & (dup_df['sku'].str.contains('KIT')), 'quantity'] = 0
    dup_df.loc[dup_df['sku'].isna(), 'quantity'] = 0
    
    return dup_df


def create_partition():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')


def extract_orders(query_results):
    orders = query_results['data']['orders']['edges']

    cols = ['Row ID', 'Order ID', 'Order Number', 'Order Date', 'Order Timestamp', 'Product', 'SKU', 'Quantity']
    df = pd.DataFrame(columns=cols)

    for order_dict in orders:
        order = order_dict['node']
        order_id = order['id'].split('/')[-1]
        order_number = order['name'].replace('#', '')
        order_timestamp = order['createdAt']
        order_date = order['createdAt'].split('T')[0]
        line_items = order['lineItems']['edges']

        order_info = []
        for line_item in line_items:
            product_name = line_item['node']['name']
            sku = line_item['node']['sku']
            quantity = line_item['node']['quantity']
            row_id = str(uuid.uuid4())
            
            # Create data frame
            df = pd.concat(
                [df, 
                 pd.DataFrame([[
                     row_id, order_id, order_number, order_date, order_timestamp, product_name, sku, quantity]], 
                              columns=cols)], 
                ignore_index=True
            )

    df['Partition Date'] = create_partition()
    return df


