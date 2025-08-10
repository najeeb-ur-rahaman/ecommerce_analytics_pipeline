select 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp as order_purchase_ts,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    cast(order_estimated_delivery_date as date) as order_estimated_delivery_date
from {{ source('brazilian_ecommerce', 'orders') }}