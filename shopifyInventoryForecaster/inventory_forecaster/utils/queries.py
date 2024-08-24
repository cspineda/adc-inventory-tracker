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
  orders(first: 250, query:"created_at:>='STARTDATET00:00:00Z' AND created_at:<'ENDDATET00:00:00Z'", reverse:false, sortKey: CREATED_AT) {
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