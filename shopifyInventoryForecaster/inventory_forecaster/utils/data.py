import re
import uuid
import pandas as pd
from datetime import datetime
from .logger import get_logger


logger = get_logger()


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
    logger.info('Extracting orders')
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
    logger.info(f'Successfully extracted orders into a df.')
    return df


def generate_orders_table(orders):
    orders = extract_orders(orders)
    orders.columns = [c.lower().replace(' ', '_') for c in orders.columns]
    orders['order_date'] = pd.to_datetime(orders['order_date']).dt.date
    orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'])
    orders['sku'] = orders['sku'].apply(lambda x: x.rstrip('O') if isinstance(x, str) else x)
    orders = remove_tiktok_shop_dups(orders)
    orders.sort_values(['order_date', 'order_timestamp'], inplace=True)
    return orders


