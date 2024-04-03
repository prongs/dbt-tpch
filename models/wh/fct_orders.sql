{{
  config({    
    "materialized": "table"
  })
}}

WITH orders_items AS (

  SELECT * 
  
  FROM {{ ref('orders_items')}}

),

order_item_summary AS (

  SELECT 
    o.order_key,
    sum(o.gross_item_sales_amount) AS gross_item_sales_amount,
    sum(o.item_discount_amount) AS item_discount_amount,
    sum(o.item_tax_amount) AS item_tax_amount,
    sum(o.net_item_sales_amount) AS net_item_sales_amount
  
  FROM orders_items AS o
  
  GROUP BY 1

),

orders AS (

  SELECT * 
  
  FROM {{ ref('orders')}}

),

final AS (

  SELECT 
    o.order_key,
    o.order_date,
    o.customer_key,
    o.order_status_code,
    o.order_priority_code,
    o.order_clerk_name,
    o.shipping_priority,
    1 AS order_count,
    s.gross_item_sales_amount,
    s.item_discount_amount,
    s.item_tax_amount,
    s.net_item_sales_amount
  
  FROM orders AS o
  JOIN order_item_summary AS s
     ON o.order_key = s.order_key

)

SELECT 
  f.*,
  {{ dbt_housekeeping() }}

FROM final AS f

ORDER BY f.order_date
