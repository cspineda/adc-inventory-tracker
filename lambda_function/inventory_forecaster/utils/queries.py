import json
import shopify


inventory_query = """
query {
    productVariants(first: 100){
        edges {
            node {
                displayName
                barcode
                sku
                inventoryQuantity
            }
        }
   }
}
"""


# orders
orders_query = """
query {
  orders(first: 250, query:"created_at:>='{start_date}T00:00:00Z' AND created_at:<'{end_date}T00:00:00Z'", reverse:false, sortKey: CREATED_AT) {
    edges {
      node {
        id
        name
        createdAt
        lineItems(first: 100) {
          edges{
            node {
              name
              sku
              quantity
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"""


# execute a graphQL call
def execute_query(query):
    query_results = shopify.GraphQL().execute(query)
    return json.loads(query_results)